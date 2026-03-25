defmodule DuckFeeder.TestSupport.DuckDBHelpers do
  @moduledoc false

  alias DuckFeeder.DuckDB.Client, as: DuckDBClient
  alias DuckFeeder.DuckDB.Connection, as: DuckDBConnection
  alias DuckFeeder.TestSupport.ProcessHelpers

  def temp_duckdb_path(prefix) when is_binary(prefix) do
    path =
      Path.join(
        System.tmp_dir!(),
        "#{prefix}_#{System.unique_integer([:positive])}.duckdb"
      )

    _ = File.rm(path)
    path
  end

  def query_duckdb_file(path, sql) when is_binary(path) and is_binary(sql) do
    {:ok, server} = DuckDBConnection.start_link(name: nil, path: path)
    conn = DuckDBConnection.get_conn(server)

    try do
      {:ok, result} = DuckDBClient.query_map(conn, sql)
      result
    after
      ProcessHelpers.safe_stop(server)
    end
  end
end
