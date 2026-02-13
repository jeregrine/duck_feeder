defmodule DuckFeeder.CDC.InitialSnapshot.Runner do
  @moduledoc """
  Orchestrates initial snapshot table copy and row dispatch.
  """

  alias DuckFeeder.CDC.InitialSnapshot

  @type row_handler :: (map(), map() -> :ok | {:error, term()})

  @spec run(pid(), [map()], keyword()) :: {:ok, map()} | {:error, term()}
  def run(conn, designated_tables, opts \\ []) when is_list(designated_tables) do
    query_fun = Keyword.get(opts, :query_fun, &Postgrex.query/3)
    snapshot_module = Keyword.get(opts, :snapshot_module, InitialSnapshot)
    row_handler = Keyword.fetch!(opts, :row_handler)

    with {:ok, snapshot} <- snapshot_module.begin_snapshot(conn, query_fun),
         {:ok, table_counts} <-
           copy_tables(
             conn,
             designated_tables,
             snapshot.boundary_lsn,
             row_handler,
             snapshot_module,
             query_fun
           ),
         :ok <- snapshot_module.finish_snapshot(conn, :commit, query_fun) do
      {:ok,
       %{
         snapshot_id: snapshot.snapshot_id,
         boundary_lsn: snapshot.boundary_lsn,
         table_counts: table_counts
       }}
    else
      {:error, _reason} = error ->
        _ = snapshot_module.finish_snapshot(conn, :rollback, query_fun)
        error
    end
  end

  defp copy_tables(conn, designated_tables, boundary_lsn, row_handler, snapshot_module, query_fun) do
    designated_tables
    |> Enum.reduce_while({:ok, %{}}, fn designated_table, {:ok, counts} ->
      schema = Map.fetch!(designated_table, :source_schema)
      table = Map.fetch!(designated_table, :source_table)

      sql = snapshot_module.copy_query(schema, table)

      case query_fun.(conn, sql, []) do
        {:ok, result} ->
          rows = snapshot_module.result_rows_to_snapshot(result, boundary_lsn)

          with :ok <- dispatch_rows(designated_table, rows, row_handler) do
            key = {schema, table}
            {:cont, {:ok, Map.put(counts, key, length(rows))}}
          else
            {:error, reason} -> {:halt, {:error, reason}}
          end

        {:error, reason} ->
          {:halt, {:error, {:snapshot_query_failed, {schema, table}, reason}}}
      end
    end)
  end

  defp dispatch_rows(_designated_table, [], _row_handler), do: :ok

  defp dispatch_rows(designated_table, [row | rest], row_handler) do
    case row_handler.(designated_table, row) do
      :ok -> dispatch_rows(designated_table, rest, row_handler)
      {:error, _reason} = error -> error
    end
  end
end
