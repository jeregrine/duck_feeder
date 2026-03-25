defmodule DuckFeeder.Sink.DuckDB do
  @moduledoc """
  DuckDB-backed sink for applying batches into real tables.

  DuckDB owns table writes and DuckFeeder owns replication correctness:

  - append batches are inserted directly into target tables
  - CDC batches are applied as table operations (`MERGE`, `DELETE`, `TRUNCATE`)
  - checkpoints are persisted through `DuckFeeder.Meta`
  - batches are tracked inside DuckDB (`duck_feeder_internal.applied_batches`)
    so that a retry after a failed checkpoint write is deduped instead of duplicated

  DuckDB config lives in `context[:duckdb]`:

      %{
        conn: pid(),            # required — the Dux connection pid
        catalog: "lake",        # optional — DuckLake catalog prefix
        setup_sql: ["INSTALL ducklake", "LOAD ducklake"],  # optional — run once per conn
        setup_fun: &my_setup/1  # optional — run once per conn
      }

  Setup hooks are memoized per connection and automatically re-run if the
  connection process restarts.
  """

  alias DuckFeeder.{DesignatedTable, Meta}
  alias DuckFeeder.CDC.Lsn
  alias DuckFeeder.DuckDB.Client, as: DuckDBClient

  @rows_source_chunk_size 500
  @applied_batch_schema "duck_feeder_internal"
  @applied_batch_table "applied_batches"

  def process_batch(context, table, batch)
      when is_map(context) and is_tuple(table) and is_map(batch) do
    meta_module = Map.get(context, :meta_module, Meta)
    catalog = context_catalog(context)

    with {:ok, conn} <- duckdb_conn(context),
         {:ok, designated_table} <- designated_table(context, table),
         {:ok, batch_lsn} <- batch_lsn_end_int(batch),
         {:ok, result} <-
           apply_batch(
             conn,
             designated_table,
             table,
             batch,
             DesignatedTable.checkpoint_key(designated_table),
             batch_lsn,
             catalog
           ),
         {:ok, checkpoint_lsn} <-
           meta_module.upsert_checkpoint(
             Map.fetch!(context, :meta_conn),
             DesignatedTable.checkpoint_key(designated_table),
             batch_lsn_end(batch)
           ) do
      {:ok,
       result
       |> Map.put(:status, :committed)
       |> Map.put(:checkpoint_key, DesignatedTable.checkpoint_key(designated_table))
       |> Map.put(:checkpoint_lsn, checkpoint_lsn)}
    end
  end

  defp apply_batch(conn, designated_table, table, batch, checkpoint_key, batch_lsn, catalog) do
    rows = Map.get(batch, :rows, [])

    with_transaction(conn, fn ->
      with {:ok, already_applied?} <-
             batch_already_applied?(conn, checkpoint_key, batch_lsn, catalog) do
        if already_applied? do
          {:ok,
           %{
             deduped?: true,
             row_count: length(rows),
             lsn_start: Map.get(batch, :lsn_start),
             lsn_end: batch_lsn_end(batch),
             table: table
           }}
        else
          with {:ok, result} <-
                 apply_batch_rows(conn, designated_table, table, batch, rows, catalog),
               :ok <-
                 record_applied_batch(
                   conn,
                   checkpoint_key,
                   batch_lsn,
                   batch_lsn_end(batch),
                   catalog
                 ) do
            {:ok, Map.put(result, :deduped?, false)}
          end
        end
      end
    end)
  end

  defp apply_batch_rows(conn, designated_table, table, batch, rows, catalog) do
    cond do
      rows == [] ->
        {:ok, %{row_count: 0, table: table}}

      cdc_batch?(rows) ->
        apply_cdc_batch(conn, designated_table, table, batch, rows, catalog)

      true ->
        apply_append_batch(conn, table, batch, rows, catalog)
    end
  end

  defp apply_append_batch(conn, table, batch, rows, catalog) do
    normalized_rows = Enum.map(rows, &normalize_row_map/1)

    with :ok <- ensure_target_schema(conn, table, catalog),
         {:ok, target_columns} <- fetch_target_columns(conn, table, catalog),
         {:ok, sources} <-
           rows_sources(normalized_rows, target_column_type_overrides(target_columns)),
         {:ok, source_columns} <- source_columns(sources),
         {:ok, target_columns} <-
           ensure_table_from_source(conn, table, hd(sources), target_columns, catalog),
         :ok <- ensure_additive_columns(conn, table, source_columns, target_columns, catalog),
         :ok <- append_sources(conn, table, sources, catalog) do
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
      with {:ok, target_columns} <- fetch_target_columns(conn, table, catalog),
           {:ok, delete_sources} <-
             maybe_rows_sources(delete_rows, target_column_type_overrides(target_columns)),
           {:ok, upsert_sources} <-
             maybe_rows_sources(upsert_rows, target_column_type_overrides(target_columns)),
           :ok <- ensure_target_schema(conn, table, catalog),
           {:ok, target_columns} <-
             maybe_prepare_target_from_sources(
               conn,
               table,
               upsert_sources,
               target_columns,
               catalog
             ),
           :ok <- maybe_truncate_target(conn, table, truncate?, target_columns, catalog),
           :ok <-
             maybe_delete_rows(conn, table, delete_sources, primary_keys, target_columns, catalog),
           :ok <- maybe_merge_rows(conn, table, upsert_sources, primary_keys, catalog) do
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

  defp maybe_prepare_target_from_sources(_conn, _table, [], target_columns, _catalog),
    do: {:ok, target_columns}

  defp maybe_prepare_target_from_sources(conn, table, [source | _], target_columns, catalog) do
    with {:ok, target_columns} <-
           ensure_table_from_source(conn, table, source, target_columns, catalog),
         :ok <- ensure_additive_columns(conn, table, source.columns, target_columns, catalog) do
      {:ok, merge_target_columns(target_columns, source.columns)}
    end
  end

  defp maybe_truncate_target(_conn, _table, false, _target_columns, _catalog), do: :ok

  defp maybe_truncate_target(_conn, _table, true, nil, _catalog), do: :ok

  defp maybe_truncate_target(conn, table, true, _target_columns, catalog) do
    execute(conn, "DELETE FROM #{qualified_relation(table, catalog)}")
  end

  defp maybe_delete_rows(_conn, _table, [], _primary_keys, _target_columns, _catalog), do: :ok

  defp maybe_delete_rows(_conn, table, _sources, _primary_keys, _target_columns, _catalog)
       when not is_tuple(table),
       do: {:error, {:invalid_table, table}}

  defp maybe_delete_rows(_conn, _table, _sources, _primary_keys, nil, _catalog), do: :ok

  defp maybe_delete_rows(conn, table, sources, primary_keys, _target_columns, catalog) do
    Enum.reduce_while(sources, :ok, fn source, :ok ->
      case execute(
             conn,
             "DELETE FROM #{qualified_relation(table, catalog)} AS target " <>
               "USING (#{source.sql}) AS source " <>
               "WHERE #{join_predicate(primary_keys)}"
           ) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp maybe_merge_rows(_conn, _table, [], _primary_keys, _catalog), do: :ok

  defp maybe_merge_rows(conn, table, sources, [], catalog) do
    append_sources(conn, table, sources, catalog)
  end

  defp maybe_merge_rows(conn, table, sources, primary_keys, catalog) do
    Enum.reduce_while(sources, :ok, fn source, :ok ->
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

      case execute(
             conn,
             "MERGE INTO #{qualified_relation(table, catalog)} AS target " <>
               "USING (#{source.sql}) AS source " <>
               "ON #{join_predicate(primary_keys)} " <>
               "WHEN MATCHED THEN UPDATE SET #{assignments} " <>
               "WHEN NOT MATCHED THEN INSERT (#{insert_columns}) VALUES (#{insert_values})"
           ) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp ensure_target_schema(conn, {schema, _table}, catalog) do
    execute(conn, "CREATE SCHEMA IF NOT EXISTS #{qualified_schema(schema, catalog)}")
  end

  defp ensure_table_from_source(conn, table, source, nil, catalog) do
    with :ok <-
           execute(
             conn,
             "CREATE TABLE #{qualified_relation(table, catalog)} AS #{source.sql} LIMIT 0"
           ) do
      {:ok, source.columns}
    end
  end

  defp ensure_table_from_source(_conn, _table, _source, target_columns, _catalog),
    do: {:ok, target_columns}

  defp ensure_additive_columns(_conn, _table, _source_columns, nil, _catalog), do: :ok

  defp ensure_additive_columns(conn, table, source_columns, target_columns, catalog) do
    target_by_name = Map.new(target_columns, &{&1.name, &1.type})

    Enum.reduce_while(source_columns, :ok, fn %{name: name, type: type}, :ok ->
      with {:ok, validated_type} <- validate_sql_type(type) do
        case Map.get(target_by_name, name) do
          nil ->
            case execute(
                   conn,
                   "ALTER TABLE #{qualified_relation(table, catalog)} ADD COLUMN #{qi(name)} #{validated_type}"
                 ) do
              :ok -> {:cont, :ok}
              {:error, reason} -> {:halt, {:error, reason}}
            end

          existing_type ->
            if compatible_type?(existing_type, validated_type) do
              {:cont, :ok}
            else
              {:halt,
               {:error,
                {:incompatible_column_type, table, name, normalize_type(existing_type),
                 validated_type}}}
            end
        end
      else
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp append_sources(conn, table, sources, catalog) do
    Enum.reduce_while(sources, :ok, fn source, :ok ->
      case execute(conn, "INSERT INTO #{qualified_relation(table, catalog)} #{source.sql}") do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp source_columns([%{columns: columns} | _sources]), do: {:ok, columns}
  defp source_columns([]), do: {:error, :missing_source_columns}

  defp maybe_rows_sources([], _type_overrides), do: {:ok, []}
  defp maybe_rows_sources(rows, type_overrides), do: rows_sources(rows, type_overrides)

  defp rows_sources(rows, type_overrides) when is_list(rows) and rows != [] do
    columns = infer_columns(rows, type_overrides)

    rows
    |> Enum.chunk_every(rows_source_chunk_size())
    |> Enum.reduce_while({:ok, []}, fn chunk, {:ok, acc} ->
      case rows_source(chunk, columns) do
        {:ok, source} -> {:cont, {:ok, [source | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, sources} -> {:ok, Enum.reverse(sources)}
      {:error, _reason} = error -> error
    end
  end

  defp rows_source(rows, columns) when is_list(rows) and rows != [] do
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
    exception in [ArgumentError] ->
      {:error, {:duckdb_source_build_failed, exception}}
  end

  defp infer_columns(rows, type_overrides) do
    rows
    |> Enum.reduce(%{}, fn row, acc ->
      Enum.reduce(row, acc, fn {key, value}, row_acc ->
        name = to_string(key)

        case Map.get(type_overrides, name) do
          nil ->
            Map.update(
              row_acc,
              name,
              %{name: name, kind: value_kind(value)},
              fn %{kind: kind} = column ->
                %{column | kind: merge_kind(kind, value_kind(value))}
              end
            )

          existing_type ->
            Map.put(row_acc, name, %{name: name, type: normalize_type(existing_type)})
        end
      end)
    end)
    |> Map.values()
    |> Enum.map(fn %{name: name} = column ->
      %{
        name: name,
        type:
          normalize_type(
            Map.get(column, :type, kind_to_sql_type(Map.get(column, :kind) || :string))
          )
      }
    end)
    |> Enum.sort_by(& &1.name)
  end

  defp cdc_stage_rows(rows, primary_keys) do
    Enum.reduce(rows, {[], []}, fn row, {delete_acc, upsert_acc} ->
      case op_code(row) do
        op when op in ["I", "R"] ->
          {delete_acc, [normalize_row_map(Map.get(row, :_record, %{})) | upsert_acc]}

        "U" ->
          record = normalize_row_map(Map.get(row, :_record, %{}))
          old_record = normalize_row_map(Map.get(row, :_old_record, %{}))

          delete_acc =
            if primary_key_changed?(record, old_record, primary_keys) do
              [slice_keys(old_record, primary_keys) | delete_acc]
            else
              delete_acc
            end

          {delete_acc, [record | upsert_acc]}

        "D" ->
          old_record = normalize_row_map(Map.get(row, :_old_record, %{}))
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

  defp fetch_target_columns(conn, {schema, table}, catalog) do
    catalog_sql =
      case catalog do
        value when is_binary(value) and value != "" ->
          " AND table_catalog = #{quote_string(value)}"

        _ ->
          ""
      end

    sql =
      "SELECT column_name, data_type FROM information_schema.columns " <>
        "WHERE table_schema = #{quote_string(schema)} " <>
        "AND table_name = #{quote_string(table)}" <>
        catalog_sql <>
        " ORDER BY ordinal_position"

    with {:ok, map} <- query_map(conn, sql) do
      column_names = Map.get(map, "column_name", [])
      column_types = Map.get(map, "data_type", [])

      columns =
        Enum.zip(column_names, column_types)
        |> Enum.map(fn {name, type} -> %{name: to_string(name), type: normalize_type(type)} end)

      {:ok, if(columns == [], do: nil, else: columns)}
    end
  end

  defp merge_target_columns(nil, source_columns), do: source_columns

  defp merge_target_columns(target_columns, source_columns) do
    (target_columns ++ source_columns)
    |> Enum.reduce(%{}, fn %{name: name} = column, acc -> Map.put(acc, name, column) end)
    |> Map.values()
    |> Enum.sort_by(& &1.name)
  end

  defp target_column_type_overrides(nil), do: %{}

  defp target_column_type_overrides(target_columns),
    do: Map.new(target_columns, &{&1.name, &1.type})

  defp rows_source_chunk_size, do: @rows_source_chunk_size

  @sql_type_pattern ~r/\A[A-Z][A-Z0-9_ ]*(\([0-9 ,]+\))?\z/

  defp compatible_type?(existing_type, incoming_type) do
    existing = normalize_type(existing_type)
    incoming = normalize_type(incoming_type)

    existing == incoming or existing in ["VARCHAR", "JSON"] or
      (existing in ["DOUBLE", "REAL", "FLOAT"] and
         incoming in ["TINYINT", "SMALLINT", "INTEGER", "BIGINT", "DOUBLE", "REAL", "FLOAT"]) or
      (existing == "BIGINT" and incoming in ["TINYINT", "SMALLINT", "INTEGER", "BIGINT"]) or
      (existing == "INTEGER" and incoming in ["TINYINT", "SMALLINT", "INTEGER"]) or
      (existing == "SMALLINT" and incoming in ["TINYINT", "SMALLINT"])
  end

  defp validate_sql_type(type) do
    normalized = normalize_type(type)

    if Regex.match?(@sql_type_pattern, normalized) do
      {:ok, normalized}
    else
      {:error, {:invalid_duckdb_type, type}}
    end
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

  defp sql_literal(nil, type) do
    validated_type = validate_sql_type!(type)
    "CAST(NULL AS #{validated_type})"
  end

  defp sql_literal(value, type) do
    validated_type = validate_sql_type!(type)
    "CAST(#{base_sql_literal(value)} AS #{validated_type})"
  end

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
    do: value |> encode_json!() |> quote_string()

  defp base_sql_literal(value), do: value |> inspect() |> quote_string()

  defp with_transaction(conn, fun) when is_function(fun, 0) do
    with :ok <- execute(conn, "BEGIN") do
      try do
        case fun.() do
          {:ok, _result} = ok ->
            case execute(conn, "COMMIT") do
              :ok ->
                ok

              {:error, reason} ->
                _ = rollback(conn)
                {:error, reason}
            end

          {:error, reason} ->
            _ = rollback(conn)
            {:error, reason}

          other ->
            _ = rollback(conn)
            {:error, {:invalid_transaction_result, other}}
        end
      rescue
        exception ->
          _ = rollback(conn)
          reraise exception, __STACKTRACE__
      catch
        kind, reason ->
          _ = rollback(conn)
          :erlang.raise(kind, reason, __STACKTRACE__)
      end
    end
  end

  defp duckdb_conn(context) do
    duckdb = Map.get(context, :duckdb, %{}) |> Map.new()

    case Map.get(duckdb, :conn) do
      conn when is_pid(conn) ->
        {:ok, conn}

      nil ->
        {:error, :missing_duckdb_conn}

      other ->
        {:error, {:invalid_duckdb_conn, other}}
    end
  end

  defp batch_already_applied?(conn, checkpoint_key, batch_lsn, catalog)
       when is_binary(checkpoint_key) and is_integer(batch_lsn) do
    sql =
      "SELECT max(last_applied_lsn) AS last_applied_lsn FROM #{applied_batch_relation(catalog)} " <>
        "WHERE checkpoint_key = #{quote_string(checkpoint_key)}"

    with {:ok, result} <- query_map(conn, sql) do
      case Map.get(result, "last_applied_lsn", []) do
        [last_applied_lsn | _] when is_integer(last_applied_lsn) ->
          {:ok, last_applied_lsn >= batch_lsn}

        [_other | _] ->
          {:ok, false}

        [] ->
          {:ok, false}
      end
    end
  end

  defp record_applied_batch(conn, checkpoint_key, batch_lsn, batch_lsn_text, catalog)
       when is_binary(checkpoint_key) and is_integer(batch_lsn) and is_binary(batch_lsn_text) do
    execute(
      conn,
      "MERGE INTO #{applied_batch_relation(catalog)} AS target USING (" <>
        "SELECT " <>
        "#{quote_string(checkpoint_key)} AS checkpoint_key, " <>
        "#{batch_lsn}::HUGEINT AS last_applied_lsn, " <>
        "#{quote_string(batch_lsn_text)} AS last_applied_lsn_text" <>
        ") AS source ON target.checkpoint_key = source.checkpoint_key " <>
        "WHEN MATCHED THEN UPDATE SET " <>
        "last_applied_lsn = source.last_applied_lsn, " <>
        "last_applied_lsn_text = source.last_applied_lsn_text " <>
        "WHEN NOT MATCHED THEN INSERT (checkpoint_key, last_applied_lsn, last_applied_lsn_text) VALUES (" <>
        "source.checkpoint_key, source.last_applied_lsn, source.last_applied_lsn_text)"
    )
  end

  defp applied_batch_relation(catalog),
    do: qualified_relation({@applied_batch_schema, @applied_batch_table}, catalog)

  defp designated_table(context, table) do
    case Map.get(context, :designated_tables_by_target, %{}) |> Map.get(table) do
      nil -> {:error, {:unknown_target_table, table}}
      designated_table when is_map(designated_table) -> {:ok, designated_table}
      other -> {:error, {:invalid_designated_table, table, other}}
    end
  end

  defp context_catalog(context) do
    context
    |> Map.get(:duckdb, %{})
    |> Map.new()
    |> Map.get(:catalog)
  end

  defp execute(conn, sql) when is_binary(sql) do
    case DuckDBClient.execute(conn, sql) do
      :ok -> :ok
      {:error, _reason} = error -> error
      _other -> :ok
    end
  end

  defp query_map(conn, sql) when is_binary(sql) do
    DuckDBClient.query_map(conn, sql)
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

  defp fetch_row_value(map, key) when is_map(map) and is_binary(key), do: Map.get(map, key)

  defp op_code(row) do
    row
    |> Map.get(:_op, "")
    |> to_string()
  end

  defp batch_lsn_end(batch) do
    Map.get(batch, :lsn_end) || Map.get(batch, "lsn_end") || "0/0"
  end

  defp batch_lsn_end_int(batch) do
    batch
    |> batch_lsn_end()
    |> Lsn.parse()
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

  defp escape_sql_string(value) when is_binary(value) do
    if String.contains?(value, <<0>>) do
      raise ArgumentError, "SQL strings may not contain null bytes"
    end

    value
    |> String.replace("\\", "\\\\")
    |> String.replace("'", "''")
  end

  defp validate_sql_type!(type) do
    case validate_sql_type(type) do
      {:ok, validated_type} ->
        validated_type

      {:error, reason} ->
        raise ArgumentError, "invalid DuckDB type #{inspect(type)}: #{inspect(reason)}"
    end
  end

  defp rollback(conn), do: execute(conn, "ROLLBACK")

  defp encode_json!(value), do: JSON.encode!(value)
end
