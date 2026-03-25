defmodule DuckFeeder.DuckDB.Client do
  @moduledoc false

  def execute(conn, sql) when is_binary(sql) do
    with :ok <- validate_conn(conn) do
      case Dux.Backend.execute(conn, sql) do
        :ok -> :ok
        other -> {:error, {:duckdb_query_failed, sql, {:unexpected_execute_result, other}}}
      end
    end
  rescue
    exception in [ArgumentError] -> {:error, {:duckdb_query_failed, sql, exception}}
  end

  def query_map(conn, sql) when is_binary(sql) do
    with :ok <- validate_conn(conn),
         table_ref when not is_nil(table_ref) <- Dux.Backend.query(conn, sql),
         result when is_map(result) <- Dux.Backend.table_to_columns(conn, table_ref) do
      {:ok, result}
    else
      {:error, _reason} = error -> error
      nil -> {:error, {:duckdb_query_failed, sql, :empty_query_result}}
      other -> {:error, {:duckdb_query_failed, sql, {:unexpected_query_result, other}}}
    end
  rescue
    exception in [ArgumentError] -> {:error, {:duckdb_query_failed, sql, exception}}
  end

  defp validate_conn(conn) when is_pid(conn), do: :ok

  defp validate_conn(other), do: {:error, {:invalid_duckdb_conn, other}}
end
