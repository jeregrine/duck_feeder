defmodule DuckFeeder.DuckDB.Init do
  @moduledoc false

  alias DuckFeeder.DuckDB.Client, as: DuckDBClient

  @applied_batch_schema "duck_feeder_internal"
  @applied_batch_table "applied_batches"

  @spec initialize(map()) :: :ok | {:error, term()}
  def initialize(%{server: server, conn: conn} = duckdb) when is_pid(server) and is_pid(conn) do
    catalog = Map.get(duckdb, :catalog)

    with :ok <- execute_setup_sql(conn, Map.get(duckdb, :setup_sql, [])),
         :ok <- execute_setup_fun(conn, Map.get(duckdb, :setup_fun)),
         :ok <- ensure_applied_batch_table(conn, catalog) do
      :ok
    end
  end

  def initialize(_duckdb), do: :ok

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

  defp ensure_applied_batch_table(conn, catalog) do
    with :ok <- execute(conn, "CREATE SCHEMA IF NOT EXISTS #{qualified_schema(@applied_batch_schema, catalog)}"),
         :ok <-
           execute(
             conn,
             "CREATE TABLE IF NOT EXISTS #{applied_batch_relation(catalog)} (" <>
               "checkpoint_key VARCHAR PRIMARY KEY, " <>
               "last_applied_lsn HUGEINT NOT NULL, " <>
               "last_applied_lsn_text VARCHAR NOT NULL)"
           ) do
      :ok
    end
  end

  defp applied_batch_relation(catalog),
    do: qualified_relation({@applied_batch_schema, @applied_batch_table}, catalog)

  defp qualified_relation({schema, table}, nil), do: qi(schema) <> "." <> qi(table)

  defp qualified_relation({schema, table}, catalog),
    do: qi(catalog) <> "." <> qi(schema) <> "." <> qi(table)

  defp qualified_schema(schema, nil), do: qi(schema)
  defp qualified_schema(schema, catalog), do: qi(catalog) <> "." <> qi(schema)

  defp qi(identifier) when is_binary(identifier),
    do: "\"" <> String.replace(identifier, "\"", "\"\"") <> "\""

  defp execute(conn, sql) when is_binary(sql) do
    case DuckDBClient.execute(conn, sql) do
      :ok -> :ok
      {:error, _reason} = error -> error
      _other -> :ok
    end
  end
end
