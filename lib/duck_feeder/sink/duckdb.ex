defmodule DuckFeeder.Sink.DuckDB do
  @moduledoc """
  DuckDB-backed sink for applying batches into real tables.

  This sink moves DuckFeeder toward the target architecture where DuckDB owns
  table writes and DuckFeeder owns replication correctness:

  - append batches are inserted directly into target tables
  - CDC batches are applied as table operations (`MERGE`, `DELETE`, `TRUNCATE`)
  - checkpoints are persisted through `DuckFeeder.Meta`

  Optional DuckDB config lives in `context[:duckdb]`:

      %{
        conn: pid(),
        catalog: "lake",
        setup_sql: ["INSTALL ducklake", "LOAD ducklake"],
        setup_fun: &my_setup/1
      }

  If `:conn` is omitted, `DuckFeeder.DuckDB.Connection.get_conn/1` is used.
  """

  @behaviour DuckFeeder.Sink

  alias DuckFeeder.DesignatedTable
  alias DuckFeeder.Meta
  alias DuckFeeder.DuckDB.Connection, as: DuckDBConnection

  @setup_registry __MODULE__.SetupRegistry

  @impl true
  def process_batch(context, table, batch)
      when is_map(context) and is_tuple(table) and is_map(batch) do
    meta_module = Map.get(context, :meta_module, Meta)

    with {:ok, conn} <- duckdb_conn(context),
         :ok <- ensure_setup(conn, context),
         {:ok, designated_table} <- designated_table(context, table),
         {:ok, checkpoint_key} <- checkpoint_key(context, table, designated_table),
         {:ok, result} <- apply_batch(conn, context, designated_table, table, batch),
         {:ok, checkpoint_lsn} <-
           meta_module.upsert_checkpoint(
             Map.fetch!(context, :meta_conn),
             checkpoint_key,
             batch_lsn_end(batch)
           ) do
      {:ok,
       result
       |> Map.put(:status, :committed)
       |> Map.put(:checkpoint_key, checkpoint_key)
       |> Map.put(:checkpoint_lsn, checkpoint_lsn)}
    end
  end

  defp apply_batch(conn, context, designated_table, table, batch) do
    rows = Map.get(batch, :rows, [])
    catalog = context_catalog(context)

    with_transaction(conn, fn ->
      cond do
        rows == [] ->
          {:ok, %{row_count: 0, table: table}}

        cdc_batch?(rows) ->
          apply_cdc_batch(conn, designated_table, table, batch, rows, catalog)

        true ->
          apply_append_batch(conn, table, batch, rows, catalog)
      end
    end)
  end

  defp apply_append_batch(conn, table, batch, rows, catalog) do
    normalized_rows = Enum.map(rows, &normalize_row_map/1)

    with {:ok, source} <- rows_source(normalized_rows),
         :ok <- ensure_target_schema(conn, table, catalog),
         :ok <- ensure_table_from_source(conn, table, source, catalog),
         :ok <- ensure_additive_columns(conn, table, source.columns, catalog),
         :ok <-
           execute(
             conn,
             "INSERT INTO #{qualified_relation(table, catalog)} #{source.sql}"
           ) do
      {:ok,
       %{
         row_count: length(normalized_rows),
         lsn_start: Map.get(batch, :lsn_start),
         lsn_end: batch_lsn_end(batch),
         table: table
       }}
    end
  end

  defp apply_cdc_batch(conn, designated_table, table, batch, rows, catalog) do
    primary_keys = normalize_primary_keys(Map.get(designated_table, :primary_keys, []))
    truncate? = Enum.any?(rows, &(op_code(&1) == "T"))
    {delete_rows, upsert_rows} = cdc_stage_rows(rows, primary_keys)

    if requires_primary_keys?(rows) and primary_keys == [] do
      {:error, {:missing_primary_keys, table}}
    else
      with {:ok, delete_source} <- maybe_rows_source(delete_rows),
           {:ok, upsert_source} <- maybe_rows_source(upsert_rows),
           :ok <- ensure_target_schema(conn, table, catalog),
           :ok <- maybe_prepare_target_from_source(conn, table, upsert_source, catalog),
           :ok <- maybe_truncate_target(conn, table, truncate?, catalog),
           :ok <- maybe_delete_rows(conn, table, delete_source, primary_keys, catalog),
           :ok <- maybe_merge_rows(conn, table, upsert_source, primary_keys, catalog) do
        {:ok,
         %{
           row_count: length(rows),
           lsn_start: Map.get(batch, :lsn_start),
           lsn_end: batch_lsn_end(batch),
           table: table,
           operation_counts: %{
             truncate: if(truncate?, do: 1, else: 0),
             deletes: length(delete_rows),
             upserts: length(upsert_rows)
           }
         }}
      end
    end
  end

  defp maybe_prepare_target_from_source(_conn, _table, nil, _catalog), do: :ok

  defp maybe_prepare_target_from_source(conn, table, source, catalog) do
    with :ok <- ensure_table_from_source(conn, table, source, catalog),
         :ok <- ensure_additive_columns(conn, table, source.columns, catalog) do
      :ok
    end
  end

  defp maybe_truncate_target(_conn, _table, false, _catalog), do: :ok

  defp maybe_truncate_target(conn, table, true, catalog) do
    if relation_exists?(conn, table, catalog) do
      execute(conn, "DELETE FROM #{qualified_relation(table, catalog)}")
    else
      :ok
    end
  end

  defp maybe_delete_rows(_conn, _table, nil, _primary_keys, _catalog), do: :ok

  defp maybe_delete_rows(_conn, table, _source, _primary_keys, _catalog)
       when not is_tuple(table),
       do: {:error, {:invalid_table, table}}

  defp maybe_delete_rows(conn, table, source, primary_keys, catalog) do
    if relation_exists?(conn, table, catalog) do
      execute(
        conn,
        "DELETE FROM #{qualified_relation(table, catalog)} AS target " <>
          "USING (#{source.sql}) AS source " <>
          "WHERE #{join_predicate(primary_keys)}"
      )
    else
      :ok
    end
  end

  defp maybe_merge_rows(_conn, _table, nil, _primary_keys, _catalog), do: :ok

  defp maybe_merge_rows(conn, table, source, [], catalog) do
    execute(
      conn,
      "INSERT INTO #{qualified_relation(table, catalog)} #{source.sql}"
    )
  end

  defp maybe_merge_rows(conn, table, source, primary_keys, catalog) do
    column_names = Enum.map(source.columns, & &1.name)

    assignments =
      Enum.map_join(column_names, ", ", fn column ->
        "#{qi(column)} = source.#{qi(column)}"
      end)

    insert_columns = Enum.map_join(column_names, ", ", &qi/1)

    insert_values =
      Enum.map_join(column_names, ", ", fn column ->
        "source.#{qi(column)}"
      end)

    execute(
      conn,
      "MERGE INTO #{qualified_relation(table, catalog)} AS target " <>
        "USING (#{source.sql}) AS source " <>
        "ON #{join_predicate(primary_keys)} " <>
        "WHEN MATCHED THEN UPDATE SET #{assignments} " <>
        "WHEN NOT MATCHED THEN INSERT (#{insert_columns}) VALUES (#{insert_values})"
    )
  end

  defp ensure_target_schema(conn, {schema, _table}, catalog) do
    execute(conn, "CREATE SCHEMA IF NOT EXISTS #{qualified_schema(schema, catalog)}")
  end

  defp ensure_table_from_source(conn, table, source, catalog) do
    if relation_exists?(conn, table, catalog) do
      :ok
    else
      execute(
        conn,
        "CREATE TABLE #{qualified_relation(table, catalog)} AS #{source.sql} LIMIT 0"
      )
    end
  end

  defp ensure_additive_columns(conn, table, source_columns, catalog) do
    target_columns =
      if relation_exists?(conn, table, catalog) do
        describe_columns(conn, qualified_relation(table, catalog))
      else
        []
      end

    target_by_name = Map.new(target_columns, &{&1.name, &1.type})

    Enum.reduce_while(source_columns, :ok, fn %{name: name, type: type}, :ok ->
      case Map.get(target_by_name, name) do
        nil ->
          case execute(
                 conn,
                 "ALTER TABLE #{qualified_relation(table, catalog)} ADD COLUMN #{qi(name)} #{type}"
               ) do
            :ok -> {:cont, :ok}
            {:error, reason} -> {:halt, {:error, reason}}
          end

        existing_type ->
          if compatible_type?(existing_type, type) do
            {:cont, :ok}
          else
            {:halt,
             {:error,
              {:incompatible_column_type, table, name, normalize_type(existing_type),
               normalize_type(type)}}}
          end
      end
    end)
  end

  defp maybe_rows_source([]), do: {:ok, nil}
  defp maybe_rows_source(rows), do: rows_source(rows)

  defp rows_source(rows) when is_list(rows) and rows != [] do
    columns = infer_columns(rows)

    values_sql =
      Enum.map_join(rows, ", ", fn row ->
        "(" <>
          Enum.map_join(columns, ", ", fn column ->
            sql_literal(fetch_row_value(row, column.name), column.type)
          end) <> ")"
      end)

    column_sql = Enum.map_join(columns, ", ", &qi(&1.name))

    {:ok,
     %{
       columns: columns,
       sql: "SELECT * FROM (VALUES #{values_sql}) AS __duck_feeder_source (#{column_sql})"
     }}
  rescue
    exception ->
      {:error, {:duckdb_source_build_failed, exception}}
  end

  defp infer_columns(rows) do
    rows
    |> Enum.flat_map(&Map.keys/1)
    |> Enum.map(&to_string/1)
    |> Enum.uniq()
    |> Enum.sort()
    |> Enum.map(fn name ->
      kind =
        rows
        |> Enum.map(&fetch_row_value(&1, name))
        |> Enum.reduce(nil, &merge_kind(&2, value_kind(&1)))

      %{name: name, type: kind_to_sql_type(kind || :string)}
    end)
  end

  defp cdc_stage_rows(rows, primary_keys) do
    Enum.reduce(rows, {[], []}, fn row, {delete_acc, upsert_acc} ->
      case op_code(row) do
        op when op in ["I", "R"] ->
          {delete_acc, [normalize_row_map(fetch_map_value(row, :_record, %{})) | upsert_acc]}

        "U" ->
          record = normalize_row_map(fetch_map_value(row, :_record, %{}))
          old_record = normalize_row_map(fetch_map_value(row, :_old_record, %{}))

          delete_acc =
            if primary_key_changed?(record, old_record, primary_keys) do
              [slice_keys(old_record, primary_keys) | delete_acc]
            else
              delete_acc
            end

          {delete_acc, [record | upsert_acc]}

        "D" ->
          old_record = normalize_row_map(fetch_map_value(row, :_old_record, %{}))
          {[slice_keys(old_record, primary_keys) | delete_acc], upsert_acc}

        "T" ->
          {delete_acc, upsert_acc}

        _other ->
          {delete_acc, upsert_acc}
      end
    end)
    |> then(fn {delete_rows, upsert_rows} ->
      {Enum.reverse(delete_rows), Enum.reverse(upsert_rows)}
    end)
  end

  defp requires_primary_keys?(rows) do
    Enum.any?(rows, fn row -> op_code(row) in ["U", "D"] end)
  end

  defp relation_exists?(conn, {schema, table}, catalog) do
    catalog_sql =
      case catalog do
        value when is_binary(value) and value != "" ->
          " AND table_catalog = '#{escape_sql_string(value)}'"

        _ ->
          ""
      end

    sql =
      "SELECT count(*) AS n FROM information_schema.tables " <>
        "WHERE table_schema = '#{escape_sql_string(schema)}' " <>
        "AND table_name = '#{escape_sql_string(table)}'" <> catalog_sql

    case query_map(conn, sql) do
      {:ok, %{"n" => [count]}} -> count > 0
      _ -> false
    end
  end

  defp describe_columns(conn, relation_sql) do
    case query_map(conn, "DESCRIBE #{relation_sql}") do
      {:ok, map} ->
        Enum.zip(map["column_name"] || [], map["column_type"] || [])
        |> Enum.map(fn {name, type} -> %{name: to_string(name), type: to_string(type)} end)

      {:error, _reason} ->
        []
    end
  end

  defp compatible_type?(existing_type, incoming_type) do
    existing = normalize_type(existing_type)
    incoming = normalize_type(incoming_type)

    existing == incoming or existing == "VARCHAR" or existing == "JSON" or
      (existing == "DOUBLE" and incoming in ["BIGINT", "INTEGER", "DOUBLE"])
  end

  defp normalize_type(type) when is_binary(type) do
    type
    |> String.trim()
    |> String.upcase()
  end

  defp normalize_type(type), do: type |> to_string() |> normalize_type()

  defp value_kind(nil), do: nil
  defp value_kind(%DateTime{}), do: :timestamptz
  defp value_kind(%NaiveDateTime{}), do: :timestamp
  defp value_kind(%Date{}), do: :date
  defp value_kind(%Time{}), do: :time
  defp value_kind(%Decimal{}), do: :double
  defp value_kind(value) when is_integer(value), do: :integer
  defp value_kind(value) when is_float(value), do: :double
  defp value_kind(value) when is_boolean(value), do: :boolean
  defp value_kind(value) when is_binary(value), do: :string
  defp value_kind(value) when is_map(value) or is_list(value), do: :json
  defp value_kind(_value), do: :string

  defp merge_kind(nil, kind), do: kind
  defp merge_kind(kind, nil), do: kind
  defp merge_kind(kind, kind), do: kind
  defp merge_kind(:integer, :double), do: :double
  defp merge_kind(:double, :integer), do: :double
  defp merge_kind(:json, _other), do: :json
  defp merge_kind(_other, :json), do: :json
  defp merge_kind(:string, _other), do: :string
  defp merge_kind(_other, :string), do: :string
  defp merge_kind(_left, _right), do: :string

  defp kind_to_sql_type(:integer), do: "BIGINT"
  defp kind_to_sql_type(:double), do: "DOUBLE"
  defp kind_to_sql_type(:boolean), do: "BOOLEAN"
  defp kind_to_sql_type(:timestamptz), do: "TIMESTAMPTZ"
  defp kind_to_sql_type(:timestamp), do: "TIMESTAMP"
  defp kind_to_sql_type(:date), do: "DATE"
  defp kind_to_sql_type(:time), do: "TIME"
  defp kind_to_sql_type(:json), do: "JSON"
  defp kind_to_sql_type(_kind), do: "VARCHAR"

  defp sql_literal(nil, type), do: "CAST(NULL AS #{type})"
  defp sql_literal(value, type), do: "CAST(#{base_sql_literal(value)} AS #{type})"

  defp base_sql_literal(%DateTime{} = value), do: quote_string(DateTime.to_iso8601(value))

  defp base_sql_literal(%NaiveDateTime{} = value),
    do: quote_string(NaiveDateTime.to_iso8601(value))

  defp base_sql_literal(%Date{} = value), do: quote_string(Date.to_iso8601(value))
  defp base_sql_literal(%Time{} = value), do: quote_string(Time.to_iso8601(value))
  defp base_sql_literal(%Decimal{} = value), do: quote_string(Decimal.to_string(value, :normal))
  defp base_sql_literal(value) when is_integer(value), do: Integer.to_string(value)

  defp base_sql_literal(value) when is_float(value),
    do: :erlang.float_to_binary(value, [:compact])

  defp base_sql_literal(true), do: "TRUE"
  defp base_sql_literal(false), do: "FALSE"
  defp base_sql_literal(value) when is_binary(value), do: quote_string(value)

  defp base_sql_literal(value) when is_map(value) or is_list(value),
    do: value |> JSON.encode!() |> quote_string()

  defp base_sql_literal(value), do: value |> inspect() |> quote_string()

  defp with_transaction(conn, fun) when is_function(fun, 0) do
    with :ok <- execute(conn, "BEGIN") do
      case fun.() do
        {:ok, _result} = ok ->
          case execute(conn, "COMMIT") do
            :ok -> ok
            {:error, reason} -> {:error, reason}
          end

        {:error, reason} ->
          _ = execute(conn, "ROLLBACK")
          {:error, reason}
      end
    end
  end

  defp duckdb_conn(context) do
    duckdb = Map.get(context, :duckdb, %{}) |> Map.new()

    case Map.get(duckdb, :conn) do
      conn when is_pid(conn) ->
        {:ok, conn}

      nil ->
        try do
          {:ok, DuckDBConnection.get_conn()}
        catch
          :exit, reason -> {:error, {:duckdb_connection_unavailable, reason}}
        end

      other ->
        {:error, {:invalid_duckdb_conn, other}}
    end
  end

  defp ensure_setup(conn, context) do
    duckdb = Map.get(context, :duckdb, %{}) |> Map.new()
    key = setup_key(conn, duckdb)

    if setup_complete?(key) do
      :ok
    else
      with :ok <- execute_setup_sql(conn, Map.get(duckdb, :setup_sql, [])),
           :ok <- execute_setup_fun(conn, Map.get(duckdb, :setup_fun)) do
        remember_setup(key)
      end
    end
  end

  defp execute_setup_sql(_conn, []), do: :ok

  defp execute_setup_sql(conn, statements) when is_list(statements) do
    Enum.reduce_while(statements, :ok, fn statement, :ok ->
      case execute(conn, statement) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp execute_setup_sql(_conn, other), do: {:error, {:invalid_duckdb_setup_sql, other}}

  defp execute_setup_fun(_conn, nil), do: :ok

  defp execute_setup_fun(conn, fun) when is_function(fun, 1) do
    case fun.(conn) do
      :ok -> :ok
      {:error, _reason} = error -> error
      other -> {:error, {:invalid_duckdb_setup_fun_result, other}}
    end
  end

  defp execute_setup_fun(_conn, other), do: {:error, {:invalid_duckdb_setup_fun, other}}

  defp setup_key(conn, duckdb) when is_pid(conn) and is_map(duckdb) do
    {conn, Map.get(duckdb, :setup_sql, []), Map.get(duckdb, :setup_fun)}
  end

  defp setup_complete?(key) do
    registry = ensure_setup_registry()
    match?([{^key, true}], :ets.lookup(registry, key))
  end

  defp remember_setup(key) do
    registry = ensure_setup_registry()
    true = :ets.insert(registry, {key, true})
    :ok
  end

  defp ensure_setup_registry do
    case :ets.whereis(@setup_registry) do
      :undefined ->
        try do
          :ets.new(@setup_registry, [:named_table, :public, :set, read_concurrency: true])
        catch
          :error, :badarg -> @setup_registry
        end

      registry ->
        registry
    end
  end

  defp designated_table(context, table) do
    case Map.get(context, :designated_table_config_by_target, %{}) |> Map.get(table) do
      nil -> {:ok, %{}}
      designated_table when is_map(designated_table) -> {:ok, designated_table}
      other -> {:error, {:invalid_designated_table_config, table, other}}
    end
  end

  defp checkpoint_key(context, table, designated_table) do
    checkpoint_key =
      case DesignatedTable.checkpoint_key(designated_table) do
        value when is_binary(value) and value != "" -> value
        _ -> Map.get(context, :designated_table_by_target, %{}) |> Map.get(table)
      end

    case checkpoint_key do
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, {:unknown_target_table, table}}
    end
  rescue
    ArgumentError ->
      case Map.get(context, :designated_table_by_target, %{}) |> Map.get(table) do
        value when is_binary(value) and value != "" -> {:ok, value}
        _ -> {:error, {:unknown_target_table, table}}
      end
  end

  defp context_catalog(context) do
    context
    |> Map.get(:duckdb, %{})
    |> Map.new()
    |> Map.get(:catalog)
  end

  defp execute(conn, sql) when is_binary(sql) do
    case Adbc.Connection.query(conn, sql) do
      {:ok, _result} -> :ok
      {:error, reason} -> {:error, {:duckdb_query_failed, sql, reason}}
    end
  rescue
    exception -> {:error, {:duckdb_query_exception, sql, exception}}
  end

  defp query_map(conn, sql) when is_binary(sql) do
    case Adbc.Connection.query(conn, sql) do
      {:ok, result} -> {:ok, Adbc.Result.to_map(result)}
      {:error, reason} -> {:error, {:duckdb_query_failed, sql, reason}}
    end
  rescue
    exception -> {:error, {:duckdb_query_exception, sql, exception}}
  end

  defp cdc_batch?(rows) do
    Enum.all?(rows, fn row ->
      is_map(row) and (Map.has_key?(row, :_op) or Map.has_key?(row, "_op"))
    end)
  end

  defp normalize_primary_keys(primary_keys) do
    primary_keys
    |> List.wrap()
    |> Enum.map(&to_string/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.uniq()
  end

  defp primary_key_changed?(_record, _old_record, []), do: false

  defp primary_key_changed?(record, old_record, primary_keys) do
    Enum.any?(primary_keys, fn key -> Map.get(record, key) != Map.get(old_record, key) end)
  end

  defp join_predicate(primary_keys) do
    Enum.map_join(primary_keys, " AND ", fn key ->
      "target.#{qi(key)} = source.#{qi(key)}"
    end)
  end

  defp slice_keys(map, keys) do
    keys
    |> Enum.reduce(%{}, fn key, acc ->
      Map.put(acc, key, Map.get(map, key))
    end)
  end

  defp normalize_row_map(map) when is_map(map) do
    map
    |> Enum.map(fn {key, value} -> {to_string(key), value} end)
    |> Map.new()
  end

  defp normalize_row_map(_other), do: %{}

  defp fetch_row_value(map, key) when is_map(map) and is_binary(key) do
    cond do
      Map.has_key?(map, key) ->
        Map.get(map, key)

      true ->
        Enum.find_value(map, fn {map_key, value} -> if to_string(map_key) == key, do: value end)
    end
  end

  defp fetch_map_value(map, key, default) when is_map(map) and is_atom(key) do
    Map.get(map, key, Map.get(map, Atom.to_string(key), default))
  end

  defp op_code(row) do
    row
    |> fetch_map_value(:_op, "")
    |> to_string()
  end

  defp batch_lsn_end(batch) do
    Map.get(batch, :lsn_end) || Map.get(batch, "lsn_end") || "0/0"
  end

  defp qualified_relation({schema, table}, nil), do: Enum.map_join([schema, table], ".", &qi/1)

  defp qualified_relation({schema, table}, catalog),
    do: Enum.map_join([catalog, schema, table], ".", &qi/1)

  defp qualified_schema(schema, nil), do: qi(schema)
  defp qualified_schema(schema, catalog), do: Enum.map_join([catalog, schema], ".", &qi/1)

  defp qi(name) when is_atom(name), do: qi(to_string(name))

  defp qi(name) when is_binary(name) do
    escaped = String.replace(name, ~s("), ~s(""))
    ~s("#{escaped}")
  end

  defp quote_string(value) when is_binary(value), do: "'#{escape_sql_string(value)}'"

  defp escape_sql_string(value) when is_binary(value), do: String.replace(value, "'", "''")
end
