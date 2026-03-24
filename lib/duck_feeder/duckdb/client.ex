defmodule DuckFeeder.DuckDB.Client do
  @moduledoc false

  def execute(conn, sql) when is_binary(sql) do
    Dux.Backend.execute(conn, sql)
    :ok
  rescue
    exception in [ArgumentError] -> {:error, {:duckdb_query_failed, sql, exception}}
    exception -> {:error, {:duckdb_query_exception, sql, exception}}
  end

  def query_map(conn, sql) when is_binary(sql) do
    table_ref = Dux.Backend.query(conn, sql)
    {:ok, Dux.Backend.table_to_columns(conn, table_ref)}
  rescue
    exception in [ArgumentError] -> {:error, {:duckdb_query_failed, sql, exception}}
    exception -> {:error, {:duckdb_query_exception, sql, exception}}
  end
end
