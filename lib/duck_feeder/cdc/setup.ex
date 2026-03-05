defmodule DuckFeeder.CDC.Setup do
  @moduledoc """
  SQL helpers for replication setup (publication + logical replication slot).

  These helpers run against a normal Postgres connection.
  """

  @type query_fun :: (pid(), String.t(), list() -> {:ok, Postgrex.Result.t()} | {:error, term()})

  @replica_identity_sql """
  SELECT c.relreplident
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname = $1
    AND c.relname = $2
  """

  @spec publication_exists?(pid(), String.t(), keyword()) :: {:ok, boolean()} | {:error, term()}
  def publication_exists?(conn, publication_name, opts \\ []) when is_binary(publication_name) do
    query_fun = Keyword.get(opts, :query_fun, &Postgrex.query/3)

    with {:ok, result} <-
           query_fun.(conn, "SELECT 1 FROM pg_publication WHERE pubname = $1", [publication_name]) do
      {:ok, result.num_rows > 0}
    end
  end

  @spec ensure_publication(pid(), String.t(), [map()], keyword()) ::
          {:ok, :exists | :created} | {:error, term()}
  def ensure_publication(conn, publication_name, designated_tables, opts \\ [])
      when is_binary(publication_name) and is_list(designated_tables) do
    query_fun = Keyword.get(opts, :query_fun, &Postgrex.query/3)

    with {:ok, exists?} <- publication_exists?(conn, publication_name, query_fun: query_fun) do
      if exists? do
        {:ok, :exists}
      else
        create_publication(conn, publication_name, designated_tables, query_fun: query_fun)
      end
    end
  end

  @spec create_publication(pid(), String.t(), [map()], keyword()) ::
          {:ok, :created} | {:error, term()}
  def create_publication(conn, publication_name, designated_tables, opts \\ [])
      when is_binary(publication_name) and is_list(designated_tables) do
    query_fun = Keyword.get(opts, :query_fun, &Postgrex.query/3)

    case publication_tables_sql(designated_tables) do
      {:ok, table_sql} ->
        sql = "CREATE PUBLICATION #{quote_ident(publication_name)} FOR TABLE #{table_sql}"

        case query_fun.(conn, sql, []) do
          {:ok, _result} ->
            {:ok, :created}

          {:error, reason} ->
            if duplicate_object_error?(reason), do: {:ok, :exists}, else: {:error, reason}
        end

      {:error, _reason} = error ->
        error
    end
  end

  @spec ensure_replica_identity_full(pid(), [map()], keyword()) :: :ok | {:error, term()}
  def ensure_replica_identity_full(conn, designated_tables, opts \\ [])
      when is_list(designated_tables) do
    query_fun = Keyword.get(opts, :query_fun, &Postgrex.query/3)

    designated_tables
    |> Enum.reduce_while(:ok, fn designated_table, :ok ->
      with {:ok, schema} <- fetch_required(designated_table, :source_schema),
           {:ok, source_table} <- fetch_required(designated_table, :source_table),
           {:ok, result} <- query_fun.(conn, @replica_identity_sql, [schema, source_table]),
           {:ok, relreplident} <- extract_replica_identity(result.rows, schema, source_table),
           :ok <- validate_replica_identity(relreplident, schema, source_table) do
        {:cont, :ok}
      else
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  @spec slot_exists?(pid(), String.t(), keyword()) :: {:ok, boolean()} | {:error, term()}
  def slot_exists?(conn, slot_name, opts \\ []) when is_binary(slot_name) do
    with {:ok, info} <- slot_info(conn, slot_name, opts) do
      {:ok, not is_nil(info)}
    end
  end

  @spec slot_info(pid(), String.t(), keyword()) :: {:ok, map() | nil} | {:error, term()}
  def slot_info(conn, slot_name, opts \\ []) when is_binary(slot_name) do
    query_fun = Keyword.get(opts, :query_fun, &Postgrex.query/3)

    with {:ok, result} <-
           query_fun.(
             conn,
             "SELECT slot_name, slot_type, plugin FROM pg_replication_slots WHERE slot_name = $1",
             [slot_name]
           ) do
      case result.rows do
        [[found_slot_name, slot_type, plugin]] ->
          {:ok, %{slot_name: found_slot_name, slot_type: slot_type, plugin: plugin}}

        [] ->
          {:ok, nil}

        rows ->
          {:error, {:unexpected_slot_info_rows, rows}}
      end
    end
  end

  @spec ensure_slot(pid(), String.t(), String.t(), keyword()) ::
          {:ok, :exists | {:created, %{slot_name: String.t(), lsn: String.t()}}}
          | {:error, term()}
  def ensure_slot(conn, slot_name, plugin \\ "pgoutput", opts \\ [])
      when is_binary(slot_name) and is_binary(plugin) do
    query_fun = Keyword.get(opts, :query_fun, &Postgrex.query/3)

    with {:ok, slot_info} <- slot_info(conn, slot_name, query_fun: query_fun) do
      case slot_info do
        nil ->
          create_slot(conn, slot_name, plugin, query_fun: query_fun)

        %{slot_type: "logical", plugin: existing_plugin} when existing_plugin == plugin ->
          {:ok, :exists}

        %{slot_type: "logical", plugin: existing_plugin} ->
          {:error, {:slot_plugin_mismatch, plugin, existing_plugin}}

        %{slot_type: slot_type} ->
          {:error, {:slot_type_mismatch, slot_type}}
      end
    end
  end

  @spec create_slot(pid(), String.t(), String.t(), keyword()) ::
          {:ok, :exists | {:created, %{slot_name: String.t(), lsn: String.t()}}}
          | {:error, term()}
  def create_slot(conn, slot_name, plugin \\ "pgoutput", opts \\ [])
      when is_binary(slot_name) and is_binary(plugin) do
    query_fun = Keyword.get(opts, :query_fun, &Postgrex.query/3)

    case query_fun.(
           conn,
           "SELECT slot_name, lsn::text FROM pg_create_logical_replication_slot($1, $2)",
           [slot_name, plugin]
         ) do
      {:ok, %Postgrex.Result{rows: [[created_slot_name, lsn]]}} ->
        {:ok, {:created, %{slot_name: created_slot_name, lsn: lsn}}}

      {:ok, %Postgrex.Result{rows: rows}} ->
        {:error, {:unexpected_slot_create_rows, rows}}

      {:error, reason} ->
        if duplicate_object_error?(reason), do: {:ok, :exists}, else: {:error, reason}
    end
  end

  @spec drop_slot(pid(), String.t(), keyword()) :: :ok | {:error, term()}
  def drop_slot(conn, slot_name, opts \\ []) when is_binary(slot_name) do
    query_fun = Keyword.get(opts, :query_fun, &Postgrex.query/3)

    with {:ok, exists?} <- slot_exists?(conn, slot_name, query_fun: query_fun) do
      if exists? do
        case query_fun.(conn, "SELECT pg_drop_replication_slot($1)", [slot_name]) do
          {:ok, _result} -> :ok
          {:error, reason} -> {:error, reason}
        end
      else
        :ok
      end
    end
  end

  @spec publication_tables_sql([map()]) :: {:ok, String.t()} | {:error, term()}
  def publication_tables_sql(designated_tables) when is_list(designated_tables) do
    designated_tables
    |> Enum.reduce_while({:ok, []}, fn table, {:ok, acc} ->
      with {:ok, schema} <- fetch_required(table, :source_schema),
           {:ok, source_table} <- fetch_required(table, :source_table) do
        {:cont, {:ok, ["#{quote_ident(schema)}.#{quote_ident(source_table)}" | acc]}}
      else
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, []} -> {:error, :no_designated_tables}
      {:ok, names} -> {:ok, names |> Enum.reverse() |> Enum.join(", ")}
      {:error, _reason} = error -> error
    end
  end

  defp extract_replica_identity([[relreplident]], _schema, _source_table)
       when is_binary(relreplident),
       do: {:ok, relreplident}

  defp extract_replica_identity([], schema, source_table),
    do: {:error, {:source_table_not_found, {schema, source_table}}}

  defp extract_replica_identity(rows, schema, source_table),
    do: {:error, {:unexpected_replica_identity_rows, {schema, source_table}, rows}}

  defp validate_replica_identity("f", _schema, _source_table), do: :ok

  defp validate_replica_identity(relreplident, schema, source_table) do
    {:error,
     {:replica_identity_not_full, {schema, source_table}, decode_replica_identity(relreplident)}}
  end

  defp decode_replica_identity("d"), do: :default
  defp decode_replica_identity("n"), do: :nothing
  defp decode_replica_identity("f"), do: :all_columns
  defp decode_replica_identity("i"), do: :index
  defp decode_replica_identity(other), do: {:unknown, other}

  defp fetch_required(map, key) do
    case Map.get(map, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, {:missing_required, key}}
    end
  end

  defp duplicate_object_error?(%Postgrex.Error{postgres: %{code: code}})
       when code in [:duplicate_object, "42710"],
       do: true

  defp duplicate_object_error?(_reason), do: false

  defp quote_ident(identifier) do
    escaped = String.replace(identifier, "\"", "\"\"")
    "\"#{escaped}\""
  end
end
