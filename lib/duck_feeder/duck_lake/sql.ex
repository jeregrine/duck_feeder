defmodule DuckFeeder.DuckLake.SQL do
  @moduledoc """
  SQL statement provider for DuckLake commit transactions.

  Used by `DuckFeeder.DuckLake.Committer.Postgres`.
  """

  @type statement :: String.t() | {String.t(), list()}

  @default_commit_log_sql """
  INSERT INTO duckfeeder_meta.ducklake_commits
    (
      batch_id,
      designated_table_id,
      target_schema,
      target_table,
      object_key,
      lsn_end,
      row_count,
      file_size,
      inserted_at
    )
  SELECT
    batches.batch_id,
    batches.designated_table_id,
    designated_tables.target_schema,
    designated_tables.target_table,
    $2,
    batches.lsn_end::text,
    $3,
    $4,
    now()
  FROM duckfeeder_meta.batches batches
  JOIN duckfeeder_meta.designated_tables designated_tables
    ON designated_tables.id = batches.designated_table_id
  WHERE batches.batch_id = $1
  ON CONFLICT (batch_id, object_key) DO UPDATE SET
    row_count = EXCLUDED.row_count,
    file_size = EXCLUDED.file_size,
    inserted_at = now()
  """

  @spec commit_statements(String.t(), keyword()) :: [statement()]
  def commit_statements(batch_id, opts \\ []) when is_binary(batch_id) do
    case Keyword.fetch(opts, :ducklake_sql) do
      {:ok, statements} when is_list(statements) ->
        statements

      {:ok, fun} when is_function(fun, 1) ->
        List.wrap(fun.(batch_id))

      {:ok, _other} ->
        []

      :error ->
        default_commit_statements(batch_id, opts)
    end
  end

  defp default_commit_statements(batch_id, opts) do
    object_key = Keyword.get(opts, :object_key)
    write_result = Keyword.get(opts, :write_result, %{}) |> Map.new()

    if is_binary(object_key) and object_key != "" do
      [
        {@default_commit_log_sql,
         [
           batch_id,
           object_key,
           Map.get(write_result, :row_count, 0),
           Map.get(write_result, :file_size_bytes, 0)
         ]}
      ]
    else
      []
    end
  end
end
