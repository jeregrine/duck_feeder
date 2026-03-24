defmodule DuckFeeder.TestSupport.IntegrationHelpers do
  @moduledoc false

  alias DuckFeeder.CDC.ConnectionOptions
  alias DuckFeeder.DuckDB.Client, as: DuckDBClient
  alias DuckFeeder.DuckDB.Connection, as: DuckDBConnection
  alias DuckFeeder.Runtime

  def integration_config do
    Application.get_env(:duck_feeder, :integration, [])
  end

  def source_database_url! do
    fetch_database_url!(:source)
  end

  def meta_database_url! do
    fetch_database_url!(:meta)
  end

  def start_postgres_conn!(postgres_url) when is_binary(postgres_url) do
    {:ok, conn_opts} = ConnectionOptions.parse_url(postgres_url)
    {:ok, conn} = Postgrex.start_link(conn_opts ++ [types: DuckFeeder.Postgrex.Types])
    conn
  end

  def temp_dir!(prefix) when is_binary(prefix) do
    path = Path.join(System.tmp_dir!(), unique_name(prefix))
    File.mkdir_p!(path)
    path
  end

  def unique_name(prefix) when is_binary(prefix) do
    suffix =
      "#{System.system_time(:microsecond)}_#{System.unique_integer([:positive, :monotonic])}"

    "#{prefix}_#{suffix}"
  end

  def ducklake_duckdb_config(root, opts \\ []) when is_binary(root) and is_list(opts) do
    catalog = Keyword.get(opts, :catalog, "lake")
    path = Keyword.get(opts, :path, Path.join(root, "session.duckdb"))
    metadata_path = Path.join(root, "metadata.ducklake")
    data_path = Path.join(root, "data")

    File.mkdir_p!(data_path)

    %{
      path: path,
      catalog: catalog,
      setup_sql: ["INSTALL ducklake", "LOAD ducklake"],
      setup_fun: fn conn ->
        ensure_attached(conn, catalog, attach_ducklake_sql(metadata_path, catalog, data_path))
      end,
      ducklake_metadata_path: metadata_path,
      ducklake_data_path: data_path
    }
  end

  def ducklake_postgres_config(root, postgres_url, opts \\ [])
      when is_binary(root) and is_binary(postgres_url) and is_list(opts) do
    catalog = Keyword.get(opts, :catalog, "lake")
    path = Keyword.get(opts, :path, Path.join(root, "session.duckdb"))
    data_path = Path.join(root, "data")
    connection_string = postgres_connection_string(postgres_url)

    File.mkdir_p!(data_path)

    %{
      path: path,
      catalog: catalog,
      setup_sql: ["INSTALL ducklake", "LOAD ducklake", "INSTALL postgres", "LOAD postgres"],
      setup_fun: fn conn ->
        ensure_attached(
          conn,
          catalog,
          attach_postgres_ducklake_sql(connection_string, catalog, data_path)
        )
      end,
      ducklake_catalog_postgres_url: postgres_url,
      ducklake_catalog_connection_string: connection_string,
      ducklake_data_path: data_path
    }
  end

  def create_postgres_database!(base_postgres_url, database_name)
      when is_binary(base_postgres_url) and is_binary(database_name) do
    admin_url = replace_database(base_postgres_url, "postgres")
    admin_conn = start_postgres_conn!(admin_url)

    try do
      {:ok, _} = Postgrex.query(admin_conn, ~s(CREATE DATABASE "#{database_name}"), [])
    after
      safe_stop(admin_conn)
    end

    replace_database(base_postgres_url, database_name)
  end

  def drop_postgres_database!(base_postgres_url, database_name)
      when is_binary(base_postgres_url) and is_binary(database_name) do
    admin_url = replace_database(base_postgres_url, "postgres")
    admin_conn = start_postgres_conn!(admin_url)

    try do
      {:ok, _} =
        Postgrex.query(
          admin_conn,
          "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()",
          [database_name]
        )

      {:ok, _} = Postgrex.query(admin_conn, ~s(DROP DATABASE IF EXISTS "#{database_name}"), [])
    after
      safe_stop(admin_conn)
    end

    :ok
  end

  def query_duckdb!(duckdb, sql) when is_map(duckdb) and is_binary(sql) do
    {:ok, server} =
      DuckDBConnection.start_link(name: nil, path: Map.get(duckdb, :path))

    conn = DuckDBConnection.get_conn(server)

    try do
      Enum.each(Map.get(duckdb, :setup_sql, []), fn statement ->
        :ok = DuckDBClient.execute(conn, statement)
      end)

      case Map.get(duckdb, :setup_fun) do
        fun when is_function(fun, 1) -> :ok = fun.(conn)
        _ -> :ok
      end

      {:ok, result} = DuckDBClient.query_map(conn, sql)
      result
    after
      safe_stop(server)
    end
  end

  def flush_ducklake_inlined_data!(duckdb) when is_map(duckdb) do
    catalog = Map.fetch!(duckdb, :catalog)
    query_duckdb!(duckdb, "SELECT * FROM ducklake_flush_inlined_data('#{catalog}')")
  end

  def parquet_files(root) when is_binary(root) do
    root
    |> Path.join("**/*.parquet")
    |> Path.wildcard()
    |> Enum.sort()
  end

  def safe_stop(pid) when is_pid(pid) do
    _ = GenServer.stop(pid)
    :ok
  catch
    :exit, _reason -> :ok
  end

  def safe_stop(_other), do: :ok

  defp fetch_database_url!(kind) when kind in [:source, :meta] do
    config = integration_config()
    repo_key = String.to_atom("#{kind}_repo")
    url_key = String.to_atom("#{kind}_database_url")

    cond do
      repo = Keyword.get(config, repo_key) ->
        case Runtime.repo_postgres_url(repo) do
          {:ok, postgres_url} ->
            postgres_url

          {:error, reason} ->
            raise "failed to derive #{kind} database url from #{inspect(repo)}: #{inspect(reason)}"
        end

      postgres_url = Keyword.get(config, url_key) ->
        postgres_url

      true ->
        raise "set :duck_feeder, :integration, #{url_key} or #{repo_key} in config/test.exs"
    end
  end

  defp ensure_attached(conn, catalog, attach_sql)
       when is_pid(conn) and is_binary(catalog) and is_binary(attach_sql) do
    case DuckDBClient.query_map(
           conn,
           "SELECT database_name FROM duckdb_databases() WHERE database_name = '#{escape_sql_string(catalog)}'"
         ) do
      {:ok, %{"database_name" => [_already_attached | _]}} ->
        :ok

      {:ok, %{"database_name" => []}} ->
        DuckDBClient.execute(conn, attach_sql)

      {:ok, _other} ->
        DuckDBClient.execute(conn, attach_sql)

      {:error, _reason} ->
        DuckDBClient.execute(conn, attach_sql)
    end
  end

  defp attach_ducklake_sql(metadata_path, catalog, data_path) do
    "ATTACH 'ducklake:#{escape_sql_string(metadata_path)}' AS #{catalog} (DATA_PATH '#{escape_sql_string(data_path)}/')"
  end

  defp attach_postgres_ducklake_sql(connection_string, catalog, data_path) do
    "ATTACH 'ducklake:postgres:#{escape_sql_string(connection_string)}' AS #{catalog} (DATA_PATH '#{escape_sql_string(data_path)}/')"
  end

  defp postgres_connection_string(postgres_url) when is_binary(postgres_url) do
    {:ok, opts} = ConnectionOptions.parse_url(postgres_url)

    [
      {:database, "dbname"},
      {:hostname, "host"},
      {:port, "port"},
      {:username, "user"},
      {:password, "password"}
    ]
    |> Enum.reduce([], fn {opt_key, conn_key}, acc ->
      case Keyword.get(opts, opt_key) do
        nil -> acc
        value -> ["#{conn_key}=#{quote_connection_value(value)}" | acc]
      end
    end)
    |> Enum.reverse()
    |> Enum.join(" ")
  end

  defp quote_connection_value(value) when is_integer(value), do: Integer.to_string(value)

  defp quote_connection_value(value) when is_binary(value) do
    if String.contains?(value, [" ", "'", "\\"]) do
      "'" <> String.replace(value, "'", "\\'") <> "'"
    else
      value
    end
  end

  defp replace_database(postgres_url, database_name)
       when is_binary(postgres_url) and is_binary(database_name) do
    uri = URI.parse(postgres_url)
    URI.to_string(%{uri | path: "/#{database_name}"})
  end

  defp escape_sql_string(value) when is_binary(value) do
    String.replace(value, "'", "''")
  end
end
