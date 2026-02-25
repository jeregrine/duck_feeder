defmodule DuckFeeder.CDC.InitialSnapshot do
  @moduledoc """
  Helpers for initial snapshot setup and row tagging.
  """

  alias DuckFeeder.CDC.SnapshotBoundary

  @begin_snapshot_sql "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"
  @export_snapshot_sql "SELECT pg_export_snapshot(), pg_current_wal_lsn()::text"

  @type query_fun :: (pid(), String.t(), list() -> {:ok, Postgrex.Result.t()} | {:error, term()})

  @spec begin_snapshot(pid(), query_fun()) ::
          {:ok, %{snapshot_id: String.t(), boundary_lsn: String.t()}} | {:error, term()}
  def begin_snapshot(conn, query_fun \\ &Postgrex.query/3) do
    with {:ok, _} <- query_fun.(conn, @begin_snapshot_sql, []),
         {:ok, %Postgrex.Result{rows: [[snapshot_id, boundary_lsn]]}} <-
           query_fun.(conn, @export_snapshot_sql, []) do
      {:ok, %{snapshot_id: snapshot_id, boundary_lsn: boundary_lsn}}
    else
      {:ok, %Postgrex.Result{rows: rows}} -> {:error, {:unexpected_snapshot_rows, rows}}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec finish_snapshot(pid(), :commit | :rollback, query_fun()) :: :ok | {:error, term()}
  def finish_snapshot(conn, action \\ :commit, query_fun \\ &Postgrex.query/3)
      when action in [:commit, :rollback] do
    sql = if action == :commit, do: "COMMIT", else: "ROLLBACK"

    case query_fun.(conn, sql, []) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @spec copy_query(String.t(), String.t(), keyword()) :: String.t()
  def copy_query(schema, table, opts \\ []) when is_binary(schema) and is_binary(table) do
    columns = Keyword.get(opts, :columns, :all)
    where_clause = Keyword.get(opts, :where)
    order_by = Keyword.get(opts, :order_by, [])

    select_columns =
      case columns do
        :all -> "*"
        list when is_list(list) and list != [] -> Enum.map_join(list, ", ", &quote_ident/1)
      end

    where_sql = build_where_sql(where_clause)

    order_sql =
      case order_by do
        list when is_list(list) and list != [] ->
          " ORDER BY " <> Enum.map_join(list, ", ", &quote_ident/1)

        _ ->
          ""
      end

    "SELECT #{select_columns} FROM #{quote_ident(schema)}.#{quote_ident(table)}#{where_sql}#{order_sql}"
  end

  @spec row_to_snapshot([String.t()], list(), String.t(), keyword()) :: map()
  def row_to_snapshot(columns, row, boundary_lsn, opts \\ [])
      when is_list(columns) and is_list(row) and is_binary(boundary_lsn) do
    columns
    |> Enum.zip(row)
    |> Map.new()
    |> SnapshotBoundary.tag_snapshot_row(boundary_lsn, opts)
  end

  @spec result_rows_to_snapshot(Postgrex.Result.t(), String.t(), keyword()) :: [map()]
  def result_rows_to_snapshot(
        %Postgrex.Result{columns: columns, rows: rows},
        boundary_lsn,
        opts \\ []
      )
      when is_binary(boundary_lsn) do
    Enum.map(rows, fn row ->
      row_to_snapshot(columns, row, boundary_lsn, opts)
    end)
  end

  defp build_where_sql(nil), do: ""

  defp build_where_sql(where_clause) when is_binary(where_clause) do
    case String.trim(where_clause) do
      "" ->
        ""

      trimmed ->
        if safe_where_clause?(trimmed) do
          " WHERE #{trimmed}"
        else
          raise ArgumentError,
                "unsafe snapshot where clause: rejected ';', '--', or block comment tokens"
        end
    end
  end

  defp build_where_sql(_other), do: ""

  defp safe_where_clause?(where_clause) when is_binary(where_clause) do
    not String.contains?(where_clause, [";", "--", "/*", "*/"])
  end

  defp quote_ident(identifier) do
    escaped = String.replace(identifier, "\"", "\"\"")
    "\"#{escaped}\""
  end
end
