defmodule DuckFeeder.DuckLake.SQL do
  @moduledoc """
  SQL statement provider for DuckLake commit transactions.

  Used by `DuckFeeder.DuckLake.Committer.Postgres`.
  """

  @type statement :: String.t() | {String.t(), list()}

  @insert_snapshot_sql """
  WITH latest AS (
    SELECT snapshot_id, schema_version, next_catalog_id, next_file_id
    FROM ducklake_metadata.ducklake_snapshot
    ORDER BY snapshot_id DESC
    LIMIT 1
    FOR UPDATE
  ),
  table_ref AS (
    SELECT batches.designated_table_id AS table_id
    FROM duckfeeder_meta.batches batches
    WHERE batches.batch_id = $1
  ),
  incoming AS (
    SELECT jsonb_array_elements_text($2::jsonb) AS column_name
  ),
  has_new AS (
    SELECT
      (
        NOT EXISTS (
          SELECT 1
          FROM ducklake_metadata.ducklake_table table_entry
          WHERE table_entry.table_id = table_ref.table_id
            AND table_entry.end_snapshot IS NULL
        )
      )
      OR EXISTS (
        SELECT 1
        FROM incoming
        WHERE NOT EXISTS (
          SELECT 1
          FROM ducklake_metadata.ducklake_column col
          WHERE col.table_id = table_ref.table_id
            AND col.column_name = incoming.column_name
            AND col.end_snapshot IS NULL
        )
      ) AS changed
    FROM table_ref
  )
  INSERT INTO ducklake_metadata.ducklake_snapshot
    (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id)
  SELECT
    latest.snapshot_id + 1,
    now(),
    CASE WHEN has_new.changed THEN latest.schema_version + 1 ELSE latest.schema_version END,
    latest.next_catalog_id,
    latest.next_file_id + 1
  FROM latest, has_new
  """

  @ensure_table_sql """
  INSERT INTO ducklake_metadata.ducklake_table
    (table_id, table_uuid, begin_snapshot, end_snapshot, schema_id, table_name, path, path_is_relative)
  SELECT
    designated_tables.id,
    ('00000000-0000-0000-0000-' || lpad(to_hex(designated_tables.id), 12, '0'))::uuid,
    current_snapshot.snapshot_id,
    NULL,
    0,
    designated_tables.target_table,
    designated_tables.target_schema || '/' || designated_tables.target_table || '/',
    true
  FROM duckfeeder_meta.batches batches
  JOIN duckfeeder_meta.designated_tables designated_tables
    ON designated_tables.id = batches.designated_table_id
  JOIN (
    SELECT snapshot_id
    FROM ducklake_metadata.ducklake_snapshot
    ORDER BY snapshot_id DESC
    LIMIT 1
  ) current_snapshot ON true
  WHERE batches.batch_id = $1
    AND NOT EXISTS (
      SELECT 1
      FROM ducklake_metadata.ducklake_table table_entry
      WHERE table_entry.table_id = designated_tables.id
        AND table_entry.end_snapshot IS NULL
    )
  """

  @ensure_column_sql """
  INSERT INTO ducklake_metadata.ducklake_column
    (
      column_id,
      begin_snapshot,
      end_snapshot,
      table_id,
      column_order,
      column_name,
      column_type,
      initial_default,
      default_value,
      nulls_allowed,
      parent_column
    )
  SELECT
    COALESCE(
      (
        SELECT MAX(col.column_id)
        FROM ducklake_metadata.ducklake_column col
        WHERE col.table_id = table_ref.table_id
      ),
      0
    ) + 1,
    current_snapshot.snapshot_id,
    NULL,
    table_ref.table_id,
    COALESCE(
      (
        SELECT MAX(col.column_order)
        FROM ducklake_metadata.ducklake_column col
        WHERE col.table_id = table_ref.table_id
          AND col.end_snapshot IS NULL
      ),
      0
    ) + 1,
    $2::varchar,
    $3::varchar,
    NULL,
    NULL,
    true,
    NULL
  FROM (
    SELECT batches.designated_table_id AS table_id
    FROM duckfeeder_meta.batches batches
    WHERE batches.batch_id = $1
  ) table_ref
  JOIN (
    SELECT snapshot_id
    FROM ducklake_metadata.ducklake_snapshot
    ORDER BY snapshot_id DESC
    LIMIT 1
  ) current_snapshot ON true
  WHERE NOT EXISTS (
    SELECT 1
    FROM ducklake_metadata.ducklake_column col
    WHERE col.table_id = table_ref.table_id
      AND col.column_name = $2::varchar
      AND col.end_snapshot IS NULL
  )
  """

  @ensure_mapping_sql """
  INSERT INTO ducklake_metadata.ducklake_column_mapping (mapping_id, table_id, type)
  SELECT table_ref.table_id, table_ref.table_id, 'map_by_name'
  FROM (
    SELECT batches.designated_table_id AS table_id
    FROM duckfeeder_meta.batches batches
    WHERE batches.batch_id = $1
  ) table_ref
  WHERE NOT EXISTS (
    SELECT 1
    FROM ducklake_metadata.ducklake_column_mapping mapping
    WHERE mapping.table_id = table_ref.table_id
  )
  """

  @ensure_name_mapping_sql """
  INSERT INTO ducklake_metadata.ducklake_name_mapping
    (mapping_id, column_id, source_name, target_field_id, parent_column, is_partition)
  SELECT
    table_ref.table_id,
    col.column_id,
    $2::varchar,
    col.column_id,
    NULL,
    false
  FROM (
    SELECT batches.designated_table_id AS table_id
    FROM duckfeeder_meta.batches batches
    WHERE batches.batch_id = $1
  ) table_ref
  JOIN ducklake_metadata.ducklake_column col
    ON col.table_id = table_ref.table_id
   AND col.column_name = $2::varchar
   AND col.end_snapshot IS NULL
  WHERE NOT EXISTS (
    SELECT 1
    FROM ducklake_metadata.ducklake_name_mapping name_mapping
    WHERE name_mapping.mapping_id = table_ref.table_id
      AND name_mapping.source_name = $2::varchar
      AND COALESCE(name_mapping.parent_column, -1) = -1
  )
  """

  @record_schema_version_sql """
  WITH current_snapshot AS (
    SELECT snapshot_id, schema_version
    FROM ducklake_metadata.ducklake_snapshot
    ORDER BY snapshot_id DESC
    LIMIT 1
  ),
  previous_snapshot AS (
    SELECT schema_version
    FROM ducklake_metadata.ducklake_snapshot
    WHERE snapshot_id = (SELECT snapshot_id - 1 FROM current_snapshot)
  )
  INSERT INTO ducklake_metadata.ducklake_schema_versions (begin_snapshot, schema_version)
  SELECT current_snapshot.snapshot_id, current_snapshot.schema_version
  FROM current_snapshot
  WHERE current_snapshot.snapshot_id = 0
     OR current_snapshot.schema_version <> COALESCE((SELECT schema_version FROM previous_snapshot), -1)
    AND NOT EXISTS (
      SELECT 1
      FROM ducklake_metadata.ducklake_schema_versions versions
      WHERE versions.begin_snapshot = current_snapshot.snapshot_id
    )
  """

  @insert_data_file_sql """
  WITH current_snapshot AS (
    SELECT snapshot_id, next_file_id
    FROM ducklake_metadata.ducklake_snapshot
    ORDER BY snapshot_id DESC
    LIMIT 1
  ),
  table_ref AS (
    SELECT batches.designated_table_id AS table_id
    FROM duckfeeder_meta.batches batches
    WHERE batches.batch_id = $1
  )
  INSERT INTO ducklake_metadata.ducklake_data_file
    (
      data_file_id,
      table_id,
      begin_snapshot,
      end_snapshot,
      file_order,
      path,
      path_is_relative,
      file_format,
      record_count,
      file_size_bytes,
      footer_size,
      row_id_start,
      partition_id,
      encryption_key,
      partial_file_info,
      mapping_id
    )
  SELECT
    current_snapshot.next_file_id - 1,
    table_ref.table_id,
    current_snapshot.snapshot_id,
    NULL,
    1,
    $2::varchar,
    true,
    'parquet',
    $3::bigint,
    $4::bigint,
    0,
    COALESCE(table_stats.next_row_id, 0),
    NULL,
    NULL,
    NULL,
    table_ref.table_id
  FROM current_snapshot
  JOIN table_ref ON true
  LEFT JOIN ducklake_metadata.ducklake_table_stats table_stats
    ON table_stats.table_id = table_ref.table_id
  ON CONFLICT (data_file_id) DO NOTHING
  """

  @upsert_table_stats_sql """
  INSERT INTO ducklake_metadata.ducklake_table_stats
    (table_id, record_count, next_row_id, file_size_bytes)
  SELECT table_ref.table_id, $2::bigint, $2::bigint, $3::bigint
  FROM (
    SELECT batches.designated_table_id AS table_id
    FROM duckfeeder_meta.batches batches
    WHERE batches.batch_id = $1
  ) table_ref
  ON CONFLICT (table_id) DO UPDATE SET
    record_count = ducklake_metadata.ducklake_table_stats.record_count + EXCLUDED.record_count,
    next_row_id = ducklake_metadata.ducklake_table_stats.next_row_id + EXCLUDED.record_count,
    file_size_bytes = ducklake_metadata.ducklake_table_stats.file_size_bytes + EXCLUDED.file_size_bytes
  """

  @upsert_table_column_stats_sql """
  INSERT INTO ducklake_metadata.ducklake_table_column_stats
    (table_id, column_id, contains_null, contains_nan, min_value, max_value, extra_stats)
  SELECT
    table_ref.table_id,
    col.column_id,
    ($4::bigint > 0),
    $7::boolean,
    $5::varchar,
    $6::varchar,
    ('value_count=' || $3::bigint::text)
  FROM (
    SELECT batches.designated_table_id AS table_id
    FROM duckfeeder_meta.batches batches
    WHERE batches.batch_id = $1
  ) table_ref
  JOIN ducklake_metadata.ducklake_column col
    ON col.table_id = table_ref.table_id
   AND col.column_name = $2::varchar
   AND col.end_snapshot IS NULL
  ON CONFLICT (table_id, column_id) DO UPDATE SET
    contains_null = ducklake_metadata.ducklake_table_column_stats.contains_null OR EXCLUDED.contains_null,
    contains_nan = ducklake_metadata.ducklake_table_column_stats.contains_nan OR EXCLUDED.contains_nan,
    min_value = CASE
      WHEN ducklake_metadata.ducklake_table_column_stats.min_value IS NULL THEN EXCLUDED.min_value
      WHEN EXCLUDED.min_value IS NULL THEN ducklake_metadata.ducklake_table_column_stats.min_value
      ELSE LEAST(ducklake_metadata.ducklake_table_column_stats.min_value, EXCLUDED.min_value)
    END,
    max_value = CASE
      WHEN ducklake_metadata.ducklake_table_column_stats.max_value IS NULL THEN EXCLUDED.max_value
      WHEN EXCLUDED.max_value IS NULL THEN ducklake_metadata.ducklake_table_column_stats.max_value
      ELSE GREATEST(ducklake_metadata.ducklake_table_column_stats.max_value, EXCLUDED.max_value)
    END,
    extra_stats = EXCLUDED.extra_stats
  """

  @insert_file_column_stats_sql """
  WITH current_snapshot AS (
    SELECT snapshot_id, next_file_id
    FROM ducklake_metadata.ducklake_snapshot
    ORDER BY snapshot_id DESC
    LIMIT 1
  ),
  table_ref AS (
    SELECT batches.designated_table_id AS table_id
    FROM duckfeeder_meta.batches batches
    WHERE batches.batch_id = $1
  )
  INSERT INTO ducklake_metadata.ducklake_file_column_stats
    (data_file_id, table_id, column_id, column_size_bytes, value_count, null_count, min_value, max_value, contains_nan, extra_stats)
  SELECT
    current_snapshot.next_file_id - 1,
    table_ref.table_id,
    col.column_id,
    NULL,
    $3::bigint,
    $4::bigint,
    $5::varchar,
    $6::varchar,
    $7::boolean,
    NULL
  FROM current_snapshot
  JOIN table_ref ON true
  JOIN ducklake_metadata.ducklake_column col
    ON col.table_id = table_ref.table_id
   AND col.column_name = $2::varchar
   AND col.end_snapshot IS NULL
  ON CONFLICT (data_file_id, column_id) DO UPDATE SET
    column_size_bytes = EXCLUDED.column_size_bytes,
    value_count = EXCLUDED.value_count,
    null_count = EXCLUDED.null_count,
    min_value = EXCLUDED.min_value,
    max_value = EXCLUDED.max_value,
    contains_nan = EXCLUDED.contains_nan,
    extra_stats = EXCLUDED.extra_stats
  """

  @insert_snapshot_changes_sql """
  WITH current_snapshot AS (
    SELECT snapshot_id
    FROM ducklake_metadata.ducklake_snapshot
    ORDER BY snapshot_id DESC
    LIMIT 1
  ),
  table_ref AS (
    SELECT batches.designated_table_id AS table_id
    FROM duckfeeder_meta.batches batches
    WHERE batches.batch_id = $1
  )
  INSERT INTO ducklake_metadata.ducklake_snapshot_changes
    (snapshot_id, changes_made, author, commit_message, commit_extra_info)
  SELECT
    current_snapshot.snapshot_id,
    'inserted_into_table:' || table_ref.table_id::text,
    'duck_feeder',
    'cdc commit ' || $1,
    NULL
  FROM current_snapshot
  JOIN table_ref ON true
  ON CONFLICT (snapshot_id) DO UPDATE SET
    changes_made = EXCLUDED.changes_made,
    author = EXCLUDED.author,
    commit_message = EXCLUDED.commit_message,
    commit_extra_info = EXCLUDED.commit_extra_info
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
      'object_key', $2::text,
      'row_count', ($3::bigint),
      'file_size', ($4::bigint)
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
    batch = Keyword.get(opts, :batch, %{}) |> Map.new()

    if is_binary(object_key) and object_key != "" do
      row_count = Map.get(write_result, :row_count, 0)
      file_size = Map.get(write_result, :file_size_bytes, 0)

      column_descriptors = extract_column_descriptors(batch)
      column_names_json = JSON.encode!(Enum.map(column_descriptors, & &1.name))

      [
        {@insert_snapshot_sql, [batch_id, column_names_json]},
        {@ensure_table_sql, [batch_id]}
      ] ++
        column_statements(batch_id, column_descriptors) ++
        [
          {@ensure_mapping_sql, [batch_id]}
        ] ++
        name_mapping_statements(batch_id, column_descriptors) ++
        [
          {@record_schema_version_sql, []},
          {@insert_data_file_sql, [batch_id, object_key, row_count, file_size]},
          {@upsert_table_stats_sql, [batch_id, row_count, file_size]}
        ] ++
        table_column_stats_statements(batch_id, column_descriptors) ++
        file_column_stats_statements(batch_id, column_descriptors) ++
        [
          {@insert_snapshot_changes_sql, [batch_id]},
          {@default_schema_history_sql, [batch_id, object_key, row_count, file_size]}
          | maybe_commit_log_statement(include_commit_log?, [
              batch_id,
              object_key,
              row_count,
              file_size
            ])
        ]
    else
      []
    end
  end

  defp column_statements(batch_id, column_descriptors) do
    Enum.map(column_descriptors, fn %{name: name, type: type} ->
      {@ensure_column_sql, [batch_id, name, type]}
    end)
  end

  defp name_mapping_statements(batch_id, column_descriptors) do
    Enum.map(column_descriptors, fn %{name: name} ->
      {@ensure_name_mapping_sql, [batch_id, name]}
    end)
  end

  defp table_column_stats_statements(batch_id, column_descriptors) do
    Enum.map(column_descriptors, fn %{name: name, stats: stats} ->
      {@upsert_table_column_stats_sql,
       [
         batch_id,
         name,
         stats.value_count,
         stats.null_count,
         stats.min_value,
         stats.max_value,
         stats.contains_nan
       ]}
    end)
  end

  defp file_column_stats_statements(batch_id, column_descriptors) do
    Enum.map(column_descriptors, fn %{name: name, stats: stats} ->
      {@insert_file_column_stats_sql,
       [
         batch_id,
         name,
         stats.value_count,
         stats.null_count,
         stats.min_value,
         stats.max_value,
         stats.contains_nan
       ]}
    end)
  end

  defp extract_column_descriptors(batch) do
    rows = Map.get(batch, :rows, [])

    rows
    |> Enum.reduce(%{}, fn row, acc ->
      if is_map(row) do
        Enum.reduce(row, acc, fn {key, value}, row_acc ->
          name = to_string(key)
          Map.update(row_acc, name, [value], fn values -> [value | values] end)
        end)
      else
        acc
      end
    end)
    |> Enum.map(fn {name, values} ->
      %{
        name: name,
        type: infer_ducklake_type(values),
        stats: infer_column_stats(values)
      }
    end)
    |> Enum.sort_by(& &1.name)
  end

  defp infer_column_stats(values) when is_list(values) do
    cleaned_values = Enum.reject(values, &is_nil/1)
    contains_nan = Enum.any?(cleaned_values, &nan?/1)

    min_max_values =
      cleaned_values
      |> Enum.reject(&nan?/1)
      |> Enum.map(&stat_string/1)

    {min_value, max_value} =
      case min_max_values do
        [] -> {nil, nil}
        [single] -> {single, single}
        values -> {Enum.min(values), Enum.max(values)}
      end

    %{
      value_count: length(values),
      null_count: Enum.count(values, &is_nil/1),
      min_value: min_value,
      max_value: max_value,
      contains_nan: contains_nan
    }
  end

  defp stat_string(value) when is_binary(value), do: value
  defp stat_string(value) when is_integer(value), do: Integer.to_string(value)

  defp stat_string(value) when is_float(value),
    do: :erlang.float_to_binary(value, [:compact, decimals: 16])

  defp stat_string(value) when is_boolean(value), do: to_string(value)
  defp stat_string(value) when is_tuple(value), do: value |> Tuple.to_list() |> stat_string()

  defp stat_string(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)
  defp stat_string(%NaiveDateTime{} = datetime), do: NaiveDateTime.to_iso8601(datetime)
  defp stat_string(%Date{} = date), do: Date.to_iso8601(date)
  defp stat_string(%Time{} = time), do: Time.to_iso8601(time)
  defp stat_string(value) when is_struct(value), do: inspect(value)

  defp stat_string(value) when is_map(value) or is_list(value) do
    try do
      JSON.encode!(value)
    rescue
      _ -> inspect(value)
    end
  end

  defp stat_string(value), do: to_string(value)

  defp nan?(value) when is_float(value), do: value != value
  defp nan?(_), do: false

  defp infer_ducklake_type(values) when is_list(values) do
    values
    |> Enum.reject(&is_nil/1)
    |> Enum.reject(&nan?/1)
    |> Enum.map(&value_type/1)
    |> Enum.uniq()
    |> resolve_type()
  end

  defp value_type(value) when is_integer(value), do: :bigint
  defp value_type(value) when is_float(value), do: :double
  defp value_type(value) when is_boolean(value), do: :boolean
  defp value_type(value) when is_binary(value), do: :varchar
  defp value_type(value) when is_map(value), do: :json
  defp value_type(value) when is_list(value), do: :json
  defp value_type(_), do: :varchar

  defp resolve_type([]), do: "VARCHAR"
  defp resolve_type([:bigint]), do: "BIGINT"
  defp resolve_type([:double]), do: "DOUBLE"
  defp resolve_type([:boolean]), do: "BOOLEAN"
  defp resolve_type([:json]), do: "JSON"
  defp resolve_type([:varchar]), do: "VARCHAR"

  defp resolve_type(types) do
    cond do
      Enum.all?(types, &(&1 in [:bigint, :double])) -> "DOUBLE"
      Enum.all?(types, &(&1 == :boolean)) -> "BOOLEAN"
      Enum.all?(types, &(&1 in [:json])) -> "JSON"
      true -> "VARCHAR"
    end
  end

  defp maybe_commit_log_statement(true, params), do: [{@default_commit_log_sql, params}]
  defp maybe_commit_log_statement(false, _params), do: []
end
