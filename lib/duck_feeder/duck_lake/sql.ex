defmodule DuckFeeder.DuckLake.SQL do
  @moduledoc """
  SQL statement provider for DuckLake commit transactions.

  Used by `DuckFeeder.DuckLake.Committer.Postgres`.
  """

  @type statement :: String.t() | {String.t(), list()}

  @default_spec_snapshot_file_sql """
  WITH snapshot AS (
    INSERT INTO ducklake_metadata.ducklake_snapshot (table_id, lsn_end, committed_at)
    SELECT batches.designated_table_id, batches.lsn_end::text, now()
    FROM duckfeeder_meta.batches batches
    WHERE batches.batch_id = $1
    ON CONFLICT (table_id, lsn_end) DO UPDATE SET
      committed_at = EXCLUDED.committed_at
    RETURNING id
  ),
  data_file AS (
    INSERT INTO ducklake_metadata.ducklake_data_file (snapshot_id, object_key, row_count, file_size, inserted_at)
    SELECT snapshot.id, $2, $3, $4, now()
    FROM snapshot
    ON CONFLICT (snapshot_id, object_key) DO UPDATE SET
      row_count = EXCLUDED.row_count,
      file_size = EXCLUDED.file_size,
      inserted_at = now()
    RETURNING id, snapshot_id
  )
  INSERT INTO ducklake_metadata.ducklake_snapshot_changes (snapshot_id, change_kind, data_file_id, inserted_at)
  SELECT data_file.snapshot_id, 'append', data_file.id, now()
  FROM data_file
  ON CONFLICT (snapshot_id, data_file_id, change_kind) DO NOTHING
  """

  @default_spec_table_stats_sql """
  INSERT INTO ducklake_metadata.ducklake_table_stats (table_id, row_count, updated_at)
  SELECT
    base.table_id,
    COALESCE(SUM(data_file.row_count), 0),
    now()
  FROM (
    SELECT batches.designated_table_id AS table_id
    FROM duckfeeder_meta.batches batches
    WHERE batches.batch_id = $1
  ) AS base
  LEFT JOIN ducklake_metadata.ducklake_snapshot snapshot
    ON snapshot.table_id = base.table_id
  LEFT JOIN ducklake_metadata.ducklake_data_file data_file
    ON data_file.snapshot_id = snapshot.id
  GROUP BY base.table_id
  ON CONFLICT (table_id) DO UPDATE SET
    row_count = EXCLUDED.row_count,
    updated_at = now()
  """

  @default_schema_history_sql """
  INSERT INTO duckfeeder_meta.schema_history
    (
      designated_table_id,
      relation_oid,
      schema_version,
      event_type,
      payload,
      seen_lsn,
      inserted_at
    )
  SELECT
    batches.designated_table_id,
    NULL,
    1,
    'ducklake_commit_append',
    jsonb_build_object(
      'object_key', $2,
      'row_count', $3,
      'file_size', $4
    ),
    batches.lsn_end,
    now()
  FROM duckfeeder_meta.batches batches
  WHERE batches.batch_id = $1
  """

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
    include_commit_log? = Keyword.get(opts, :include_commit_log?, true)

    if is_binary(object_key) and object_key != "" do
      params = [
        batch_id,
        object_key,
        Map.get(write_result, :row_count, 0),
        Map.get(write_result, :file_size_bytes, 0)
      ]

      [
        {@default_spec_snapshot_file_sql, params},
        {@default_spec_table_stats_sql, [batch_id]},
        {@default_schema_history_sql, params}
        | maybe_commit_log_statement(include_commit_log?, params)
      ]
    else
      []
    end
  end

  defp maybe_commit_log_statement(true, params), do: [{@default_commit_log_sql, params}]
  defp maybe_commit_log_statement(false, _params), do: []
end
