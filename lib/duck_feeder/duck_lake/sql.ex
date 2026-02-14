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
    SELECT unnest($2::text[]) AS column_name
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
    CASE WHEN has_new.changed OR $4::boolean THEN latest.schema_version + 1 ELSE latest.schema_version END,
    latest.next_catalog_id,
    latest.next_file_id + $3::bigint
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

  @schema_change_validate_rename_table_sql """
  WITH table_ref AS (
    SELECT batches.designated_table_id AS table_id
    FROM duckfeeder_meta.batches batches
    WHERE batches.batch_id = $1
  ),
  active_table AS (
    SELECT table_entry.*
    FROM ducklake_metadata.ducklake_table table_entry
    JOIN table_ref ON table_entry.table_id = table_ref.table_id
    WHERE table_entry.end_snapshot IS NULL
    ORDER BY table_entry.begin_snapshot DESC
    LIMIT 1
  ),
  checks AS (
    SELECT
      EXISTS (SELECT 1 FROM active_table) AS active_exists,
      EXISTS (
        SELECT 1
        FROM active_table
        WHERE $2::varchar IS NULL OR active_table.table_name = $2::varchar
      ) AS from_matches,
      EXISTS (
        SELECT 1
        FROM active_table
        WHERE active_table.table_name = $3::varchar
      ) AS already_renamed,
      EXISTS (
        SELECT 1
        FROM ducklake_metadata.ducklake_table existing
        JOIN active_table ON existing.schema_id = active_table.schema_id
        WHERE existing.table_name = $3::varchar
          AND existing.end_snapshot IS NULL
      ) AS to_exists
  )
  SELECT 1 /
    CASE
      WHEN (SELECT active_exists FROM checks)
        AND (
          ((SELECT from_matches FROM checks) AND NOT (SELECT to_exists FROM checks))
          OR (SELECT already_renamed FROM checks)
        ) THEN 1
      ELSE 0
    END
  """

  @schema_change_rename_table_close_sql """
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
  UPDATE ducklake_metadata.ducklake_table table_entry
  SET end_snapshot = current_snapshot.snapshot_id
  FROM current_snapshot, table_ref
  WHERE table_entry.table_id = table_ref.table_id
    AND table_entry.end_snapshot IS NULL
    AND ($2::varchar IS NULL OR table_entry.table_name = $2::varchar)
    AND table_entry.table_name <> $3::varchar
  """

  @schema_change_rename_table_insert_sql """
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
  ),
  previous AS (
    SELECT table_entry.*
    FROM ducklake_metadata.ducklake_table table_entry
    JOIN table_ref ON table_entry.table_id = table_ref.table_id
    JOIN current_snapshot ON true
    WHERE table_entry.end_snapshot = current_snapshot.snapshot_id
      AND ($2::varchar IS NULL OR table_entry.table_name = $2::varchar)
    ORDER BY table_entry.begin_snapshot DESC
    LIMIT 1
  )
  INSERT INTO ducklake_metadata.ducklake_table
    (table_id, table_uuid, begin_snapshot, end_snapshot, schema_id, table_name, path, path_is_relative)
  SELECT
    previous.table_id,
    previous.table_uuid,
    current_snapshot.snapshot_id,
    NULL,
    previous.schema_id,
    $3::varchar,
    previous.path,
    previous.path_is_relative
  FROM previous
  JOIN current_snapshot ON true
  WHERE NOT EXISTS (
    SELECT 1
    FROM ducklake_metadata.ducklake_table active
    WHERE active.table_id = previous.table_id
      AND active.table_name = $3::varchar
      AND active.end_snapshot IS NULL
  )
  """

  @schema_change_validate_rename_sql """
  WITH table_ref AS (
    SELECT batches.designated_table_id AS table_id
    FROM duckfeeder_meta.batches batches
    WHERE batches.batch_id = $1
  ),
  checks AS (
    SELECT
      EXISTS (
        SELECT 1
        FROM ducklake_metadata.ducklake_column col
        WHERE col.table_id = table_ref.table_id
          AND col.column_name = $2::varchar
          AND col.end_snapshot IS NULL
      ) AS from_exists,
      EXISTS (
        SELECT 1
        FROM ducklake_metadata.ducklake_column col
        WHERE col.table_id = table_ref.table_id
          AND col.column_name = $3::varchar
          AND col.end_snapshot IS NULL
      ) AS to_exists
    FROM table_ref
  )
  SELECT 1 /
    CASE
      WHEN ((SELECT from_exists FROM checks) AND NOT (SELECT to_exists FROM checks))
        OR (NOT (SELECT from_exists FROM checks) AND (SELECT to_exists FROM checks)) THEN 1
      ELSE 0
    END
  """

  @schema_change_rename_close_column_sql """
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
  UPDATE ducklake_metadata.ducklake_column col
  SET end_snapshot = current_snapshot.snapshot_id
  FROM current_snapshot, table_ref
  WHERE col.table_id = table_ref.table_id
    AND col.column_name = $2::varchar
    AND col.end_snapshot IS NULL
  """

  @schema_change_rename_insert_column_sql """
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
  ),
  previous AS (
    SELECT col.*
    FROM ducklake_metadata.ducklake_column col
    JOIN table_ref ON col.table_id = table_ref.table_id
    JOIN current_snapshot ON true
    WHERE col.column_name = $2::varchar
      AND col.end_snapshot = current_snapshot.snapshot_id
    ORDER BY col.begin_snapshot DESC
    LIMIT 1
  )
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
    previous.column_id,
    current_snapshot.snapshot_id,
    NULL,
    previous.table_id,
    previous.column_order,
    $3::varchar,
    previous.column_type,
    previous.initial_default,
    previous.default_value,
    previous.nulls_allowed,
    previous.parent_column
  FROM previous
  JOIN current_snapshot ON true
  """

  @schema_change_rename_mapping_sql """
  WITH table_ref AS (
    SELECT batches.designated_table_id AS table_id
    FROM duckfeeder_meta.batches batches
    WHERE batches.batch_id = $1
  )
  UPDATE ducklake_metadata.ducklake_name_mapping mapping
  SET source_name = $3::varchar
  FROM table_ref
  WHERE mapping.mapping_id = table_ref.table_id
    AND mapping.source_name = $2::varchar
    AND COALESCE(mapping.parent_column, -1) = -1
    AND NOT EXISTS (
      SELECT 1
      FROM ducklake_metadata.ducklake_name_mapping existing
      WHERE existing.mapping_id = table_ref.table_id
        AND existing.source_name = $3::varchar
        AND COALESCE(existing.parent_column, -1) = -1
    )
  """

  @schema_change_validate_drop_sql """
  WITH table_ref AS (
    SELECT batches.designated_table_id AS table_id
    FROM duckfeeder_meta.batches batches
    WHERE batches.batch_id = $1
  )
  SELECT 1 /
    CASE
      WHEN EXISTS (
        SELECT 1
        FROM ducklake_metadata.ducklake_column col
        WHERE col.table_id = table_ref.table_id
          AND col.column_name = $2::varchar
          AND col.end_snapshot IS NULL
      ) THEN 1
      ELSE 0
    END
  FROM table_ref
  """

  @schema_change_drop_column_sql """
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
  UPDATE ducklake_metadata.ducklake_column col
  SET end_snapshot = current_snapshot.snapshot_id
  FROM current_snapshot, table_ref
  WHERE col.table_id = table_ref.table_id
    AND col.column_name = $2::varchar
    AND col.end_snapshot IS NULL
  """

  @schema_change_drop_mapping_sql """
  WITH table_ref AS (
    SELECT batches.designated_table_id AS table_id
    FROM duckfeeder_meta.batches batches
    WHERE batches.batch_id = $1
  )
  DELETE FROM ducklake_metadata.ducklake_name_mapping mapping
  USING table_ref
  WHERE mapping.mapping_id = table_ref.table_id
    AND mapping.source_name = $2::varchar
    AND COALESCE(mapping.parent_column, -1) = -1
  """

  @schema_change_drop_table_column_stats_sql """
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
  ),
  dropped_cols AS (
    SELECT col.column_id
    FROM ducklake_metadata.ducklake_column col
    JOIN table_ref ON col.table_id = table_ref.table_id
    JOIN current_snapshot ON true
    WHERE col.column_name = $2::varchar
      AND col.end_snapshot = current_snapshot.snapshot_id
  )
  DELETE FROM ducklake_metadata.ducklake_table_column_stats stats
  USING table_ref, dropped_cols
  WHERE stats.table_id = table_ref.table_id
    AND stats.column_id = dropped_cols.column_id
  """

  @schema_change_drop_file_column_stats_sql """
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
  ),
  dropped_cols AS (
    SELECT col.column_id
    FROM ducklake_metadata.ducklake_column col
    JOIN table_ref ON col.table_id = table_ref.table_id
    JOIN current_snapshot ON true
    WHERE col.column_name = $2::varchar
      AND col.end_snapshot = current_snapshot.snapshot_id
  )
  DELETE FROM ducklake_metadata.ducklake_file_column_stats stats
  USING table_ref, dropped_cols
  WHERE stats.table_id = table_ref.table_id
    AND stats.column_id = dropped_cols.column_id
  """

  @schema_change_validate_type_sql """
  WITH table_ref AS (
    SELECT batches.designated_table_id AS table_id
    FROM duckfeeder_meta.batches batches
    WHERE batches.batch_id = $1
  ),
  active_col AS (
    SELECT upper(col.column_type) AS source_type
    FROM ducklake_metadata.ducklake_column col
    JOIN table_ref ON col.table_id = table_ref.table_id
    WHERE col.column_name = $2::varchar
      AND col.end_snapshot IS NULL
    LIMIT 1
  ),
  target AS (
    SELECT upper($3::varchar) AS target_type
  ),
  validation AS (
    SELECT
      EXISTS (SELECT 1 FROM active_col) AS column_exists,
      CASE
        WHEN NOT EXISTS (SELECT 1 FROM active_col) THEN false
        ELSE (
          SELECT CASE
            WHEN active_col.source_type = target.target_type THEN true
            WHEN active_col.source_type = 'TINYINT' AND target.target_type IN ('SMALLINT', 'INTEGER', 'BIGINT') THEN true
            WHEN active_col.source_type = 'SMALLINT' AND target.target_type IN ('INTEGER', 'BIGINT') THEN true
            WHEN active_col.source_type = 'INTEGER' AND target.target_type = 'BIGINT' THEN true
            WHEN active_col.source_type = 'UTINYINT' AND target.target_type IN ('USMALLINT', 'UINTEGER', 'UBIGINT') THEN true
            WHEN active_col.source_type = 'USMALLINT' AND target.target_type IN ('UINTEGER', 'UBIGINT') THEN true
            WHEN active_col.source_type = 'UINTEGER' AND target.target_type = 'UBIGINT' THEN true
            WHEN active_col.source_type IN ('FLOAT', 'REAL') AND target.target_type = 'DOUBLE' THEN true
            ELSE false
          END
          FROM active_col, target
        )
      END AS can_promote
  )
  SELECT 1 /
    CASE
      WHEN (SELECT column_exists FROM validation) AND (SELECT can_promote FROM validation) THEN 1
      ELSE 0
    END
  """

  @schema_change_type_close_column_sql """
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
  UPDATE ducklake_metadata.ducklake_column col
  SET end_snapshot = current_snapshot.snapshot_id
  FROM current_snapshot, table_ref
  WHERE col.table_id = table_ref.table_id
    AND col.column_name = $2::varchar
    AND col.end_snapshot IS NULL
  """

  @schema_change_type_insert_column_sql """
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
  ),
  previous AS (
    SELECT col.*
    FROM ducklake_metadata.ducklake_column col
    JOIN table_ref ON col.table_id = table_ref.table_id
    JOIN current_snapshot ON true
    WHERE col.column_name = $2::varchar
      AND col.end_snapshot = current_snapshot.snapshot_id
    ORDER BY col.begin_snapshot DESC
    LIMIT 1
  )
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
    previous.column_id,
    current_snapshot.snapshot_id,
    NULL,
    previous.table_id,
    previous.column_order,
    previous.column_name,
    $3::varchar,
    previous.initial_default,
    previous.default_value,
    previous.nulls_allowed,
    previous.parent_column
  FROM previous
  JOIN current_snapshot ON true
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
    current_snapshot.next_file_id - ($5::bigint + 1),
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

  @insert_delete_file_sql """
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
  ),
  fallback_data_file AS (
    SELECT data_file.data_file_id
    FROM ducklake_metadata.ducklake_data_file data_file
    JOIN table_ref ON data_file.table_id = table_ref.table_id
    WHERE data_file.end_snapshot IS NULL
    ORDER BY data_file.begin_snapshot DESC, data_file.data_file_id DESC
    LIMIT 1
  )
  INSERT INTO ducklake_metadata.ducklake_delete_file
    (
      delete_file_id,
      table_id,
      begin_snapshot,
      end_snapshot,
      data_file_id,
      path,
      path_is_relative,
      format,
      delete_count,
      file_size_bytes,
      footer_size,
      encryption_key
    )
  SELECT
    current_snapshot.next_file_id - $3::bigint + ($4::bigint - 1),
    table_ref.table_id,
    current_snapshot.snapshot_id,
    NULL,
    COALESCE($5::bigint, (SELECT data_file_id FROM fallback_data_file)),
    $2::varchar,
    $6::boolean,
    $7::varchar,
    $8::bigint,
    $9::bigint,
    $10::bigint,
    $11::varchar
  FROM current_snapshot
  JOIN table_ref ON true
  WHERE COALESCE($5::bigint, (SELECT data_file_id FROM fallback_data_file)) IS NOT NULL
  ON CONFLICT (delete_file_id) DO NOTHING
  """

  @retire_data_files_sql """
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
  ),
  target_files AS (
    SELECT unnest($2::bigint[]) AS data_file_id
  )
  UPDATE ducklake_metadata.ducklake_data_file data_file
  SET end_snapshot = current_snapshot.snapshot_id
  FROM current_snapshot, table_ref, target_files
  WHERE data_file.data_file_id = target_files.data_file_id
    AND data_file.table_id = table_ref.table_id
    AND data_file.end_snapshot IS NULL
  """

  @retire_delete_files_sql """
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
  ),
  target_files AS (
    SELECT unnest($2::bigint[]) AS data_file_id
  )
  UPDATE ducklake_metadata.ducklake_delete_file delete_file
  SET end_snapshot = current_snapshot.snapshot_id
  FROM current_snapshot, table_ref, target_files
  WHERE delete_file.data_file_id = target_files.data_file_id
    AND delete_file.table_id = table_ref.table_id
    AND delete_file.end_snapshot IS NULL
  """

  @schedule_files_for_deletion_sql """
  WITH table_ref AS (
    SELECT batches.designated_table_id AS table_id
    FROM duckfeeder_meta.batches batches
    WHERE batches.batch_id = $1
  ),
  target_files AS (
    SELECT unnest($2::bigint[]) AS data_file_id
  )
  INSERT INTO ducklake_metadata.ducklake_files_scheduled_for_deletion
    (data_file_id, path, path_is_relative, schedule_start)
  SELECT
    data_file.data_file_id,
    data_file.path,
    data_file.path_is_relative,
    now()
  FROM ducklake_metadata.ducklake_data_file data_file
  JOIN table_ref ON data_file.table_id = table_ref.table_id
  JOIN target_files ON target_files.data_file_id = data_file.data_file_id
  WHERE data_file.end_snapshot IS NOT NULL
    AND NOT EXISTS (
      SELECT 1
      FROM ducklake_metadata.ducklake_files_scheduled_for_deletion scheduled
      WHERE scheduled.data_file_id = data_file.data_file_id
        AND scheduled.path = data_file.path
    )
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
    current_snapshot.next_file_id - ($8::bigint + 1),
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
    SELECT
      batches.designated_table_id AS table_id,
      designated_tables.target_table AS table_name
    FROM duckfeeder_meta.batches batches
    JOIN duckfeeder_meta.designated_tables designated_tables
      ON designated_tables.id = batches.designated_table_id
    WHERE batches.batch_id = $1
  ),
  derived AS (
    SELECT
      CASE
        WHEN EXISTS (
          SELECT 1
          FROM ducklake_metadata.ducklake_table table_entry
          WHERE table_entry.table_id = table_ref.table_id
            AND table_entry.begin_snapshot = current_snapshot.snapshot_id
        ) THEN
          'created_table:' || '"' || replace(table_ref.table_name, '"', '""') || '"'
      END AS created_change,
      CASE
        WHEN EXISTS (
          SELECT 1
          FROM ducklake_metadata.ducklake_column col
          WHERE col.table_id = table_ref.table_id
            AND col.begin_snapshot = current_snapshot.snapshot_id
        )
        AND NOT EXISTS (
          SELECT 1
          FROM ducklake_metadata.ducklake_table table_entry
          WHERE table_entry.table_id = table_ref.table_id
            AND table_entry.begin_snapshot = current_snapshot.snapshot_id
        )
        AND position('altered_table:' in $2::varchar) = 0 THEN
          'altered_table:' || table_ref.table_id::text
      END AS altered_change
    FROM current_snapshot
    JOIN table_ref ON true
  )
  INSERT INTO ducklake_metadata.ducklake_snapshot_changes
    (snapshot_id, changes_made, author, commit_message, commit_extra_info)
  SELECT
    current_snapshot.snapshot_id,
    trim(
      BOTH ',' FROM concat_ws(
        ',',
        NULLIF(replace($2::varchar, '{table_id}', table_ref.table_id::text), ''),
        derived.created_change,
        derived.altered_change
      )
    ),
    'duck_feeder',
    'cdc commit ' || $1,
    NULL
  FROM current_snapshot
  JOIN table_ref ON true
  JOIN derived ON true
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
    delete_files = delete_file_descriptors(opts)
    replaced_data_file_ids = bigint_list(opts, :replace_data_file_ids)
    schema_changes = schema_change_descriptors(opts)

    if is_binary(object_key) and object_key != "" do
      row_count = normalize_non_neg_integer(Map.get(write_result, :row_count, 0), 0)
      file_size = normalize_non_neg_integer(Map.get(write_result, :file_size_bytes, 0), 0)

      table_stats_row_delta =
        normalize_non_neg_integer(Keyword.get(opts, :table_stats_row_delta, row_count), row_count)

      table_stats_file_size_delta =
        normalize_non_neg_integer(
          Keyword.get(opts, :table_stats_file_size_delta, file_size),
          file_size
        )

      delete_file_count = length(delete_files)
      schema_change_count = length(schema_changes)
      file_id_increment = 1 + delete_file_count

      column_descriptors = extract_column_descriptors(batch)
      column_names = Enum.map(column_descriptors, & &1.name)

      snapshot_changes =
        snapshot_changes(
          table_stats_row_delta,
          delete_file_count,
          replaced_data_file_ids,
          schema_change_count
        )

      force_schema_change? = schema_change_count > 0

      [
        {@insert_snapshot_sql, [batch_id, column_names, file_id_increment, force_schema_change?]},
        {@ensure_table_sql, [batch_id]},
        {@ensure_mapping_sql, [batch_id]}
      ] ++
        schema_change_statements(batch_id, schema_changes) ++
        column_statements(batch_id, column_descriptors) ++
        name_mapping_statements(batch_id, column_descriptors) ++
        [
          {@record_schema_version_sql, []},
          {@insert_data_file_sql, [batch_id, object_key, row_count, file_size, delete_file_count]}
        ] ++
        delete_file_statements(batch_id, delete_files) ++
        maybe_retire_data_files_statements(batch_id, replaced_data_file_ids) ++
        [
          {@upsert_table_stats_sql,
           [batch_id, table_stats_row_delta, table_stats_file_size_delta]}
        ] ++
        table_column_stats_statements(batch_id, column_descriptors) ++
        file_column_stats_statements(batch_id, column_descriptors, delete_file_count) ++
        [
          {@insert_snapshot_changes_sql, [batch_id, snapshot_changes]},
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

  defp file_column_stats_statements(batch_id, column_descriptors, delete_file_count) do
    Enum.map(column_descriptors, fn %{name: name, stats: stats} ->
      {@insert_file_column_stats_sql,
       [
         batch_id,
         name,
         stats.value_count,
         stats.null_count,
         stats.min_value,
         stats.max_value,
         stats.contains_nan,
         delete_file_count
       ]}
    end)
  end

  defp delete_file_statements(batch_id, delete_files) when is_list(delete_files) do
    total = length(delete_files)

    delete_files
    |> Enum.with_index(1)
    |> Enum.map(fn {delete_file, index} ->
      {@insert_delete_file_sql,
       [
         batch_id,
         delete_file.path,
         total,
         index,
         delete_file.data_file_id,
         delete_file.path_is_relative,
         delete_file.format,
         delete_file.delete_count,
         delete_file.file_size_bytes,
         delete_file.footer_size,
         delete_file.encryption_key
       ]}
    end)
  end

  defp maybe_retire_data_files_statements(_batch_id, []), do: []

  defp maybe_retire_data_files_statements(batch_id, replaced_data_file_ids) do
    [
      {@retire_data_files_sql, [batch_id, replaced_data_file_ids]},
      {@retire_delete_files_sql, [batch_id, replaced_data_file_ids]},
      {@schedule_files_for_deletion_sql, [batch_id, replaced_data_file_ids]}
    ]
  end

  defp schema_change_statements(batch_id, schema_changes) when is_list(schema_changes) do
    Enum.flat_map(schema_changes, fn
      %{op: :rename_table, from: from_name, to: to_name} ->
        [
          {@schema_change_validate_rename_table_sql, [batch_id, from_name, to_name]},
          {@schema_change_rename_table_close_sql, [batch_id, from_name, to_name]},
          {@schema_change_rename_table_insert_sql, [batch_id, from_name, to_name]}
        ]

      %{op: :rename_column, from: from_name, to: to_name} ->
        [
          {@schema_change_validate_rename_sql, [batch_id, from_name, to_name]},
          {@schema_change_rename_close_column_sql, [batch_id, from_name]},
          {@schema_change_rename_insert_column_sql, [batch_id, from_name, to_name]},
          {@schema_change_rename_mapping_sql, [batch_id, from_name, to_name]}
        ]

      %{op: :drop_column, column: column_name} ->
        [
          {@schema_change_validate_drop_sql, [batch_id, column_name]},
          {@schema_change_drop_column_sql, [batch_id, column_name]},
          {@schema_change_drop_mapping_sql, [batch_id, column_name]},
          {@schema_change_drop_table_column_stats_sql, [batch_id, column_name]},
          {@schema_change_drop_file_column_stats_sql, [batch_id, column_name]}
        ]

      %{op: :alter_column_type, column: column_name, type: column_type} ->
        [
          {@schema_change_validate_type_sql, [batch_id, column_name, column_type]},
          {@schema_change_type_close_column_sql, [batch_id, column_name]},
          {@schema_change_type_insert_column_sql, [batch_id, column_name, column_type]}
        ]

      _ ->
        []
    end)
  end

  defp schema_change_descriptors(opts) do
    opts
    |> Keyword.get(:schema_changes, [])
    |> List.wrap()
    |> Enum.flat_map(fn descriptor ->
      case normalize_schema_change_descriptor(descriptor) do
        nil -> []
        normalized -> [normalized]
      end
    end)
  end

  defp normalize_schema_change_descriptor(%{} = descriptor) do
    case normalize_schema_change_op(descriptor_get(descriptor, :op)) do
      :rename_table ->
        from_name = normalize_non_empty_string(descriptor_get(descriptor, :from))
        to_name = normalize_non_empty_string(descriptor_get(descriptor, :to))

        if is_binary(to_name) and (is_nil(from_name) or from_name != to_name) do
          %{op: :rename_table, from: from_name, to: to_name}
        else
          nil
        end

      :rename_column ->
        from_name = normalize_non_empty_string(descriptor_get(descriptor, :from))
        to_name = normalize_non_empty_string(descriptor_get(descriptor, :to))

        if is_binary(from_name) and is_binary(to_name) and from_name != to_name do
          %{op: :rename_column, from: from_name, to: to_name}
        else
          nil
        end

      :drop_column ->
        column_name = normalize_non_empty_string(descriptor_get(descriptor, :column))

        if is_binary(column_name) do
          %{op: :drop_column, column: column_name}
        else
          nil
        end

      :alter_column_type ->
        column_name = normalize_non_empty_string(descriptor_get(descriptor, :column))

        column_type =
          descriptor_get(descriptor, :type)
          |> normalize_non_empty_string()
          |> normalize_column_type()

        if is_binary(column_name) and is_binary(column_type) do
          %{op: :alter_column_type, column: column_name, type: column_type}
        else
          nil
        end

      _ ->
        nil
    end
  end

  defp normalize_schema_change_descriptor(_descriptor), do: nil

  defp normalize_schema_change_op(value) when is_binary(value) do
    value
    |> String.trim()
    |> String.downcase()
    |> case do
      "rename_table" -> :rename_table
      "rename_column" -> :rename_column
      "drop_column" -> :drop_column
      "alter_column_type" -> :alter_column_type
      _ -> nil
    end
  end

  defp normalize_schema_change_op(value) when is_atom(value) do
    value
    |> Atom.to_string()
    |> normalize_schema_change_op()
  end

  defp normalize_schema_change_op(_value), do: nil

  defp normalize_non_empty_string(value) when is_binary(value) do
    case String.trim(value) do
      "" -> nil
      trimmed -> trimmed
    end
  end

  defp normalize_non_empty_string(value) when is_atom(value),
    do: value |> Atom.to_string() |> normalize_non_empty_string()

  defp normalize_non_empty_string(_value), do: nil

  defp normalize_column_type(value) when is_binary(value), do: String.upcase(value)
  defp normalize_column_type(_value), do: nil

  defp delete_file_descriptors(opts) do
    opts
    |> Keyword.get(:delete_files, [])
    |> List.wrap()
    |> Enum.flat_map(fn descriptor ->
      case normalize_delete_file_descriptor(descriptor) do
        nil -> []
        normalized -> [normalized]
      end
    end)
  end

  defp normalize_delete_file_descriptor(%{} = descriptor) do
    path = descriptor_get(descriptor, :path)

    if is_binary(path) and path != "" do
      %{
        path: path,
        data_file_id: normalize_optional_integer(descriptor_get(descriptor, :data_file_id)),
        path_is_relative: descriptor_get(descriptor, :path_is_relative, true),
        format: descriptor_get(descriptor, :format, "parquet") |> to_string(),
        delete_count: normalize_non_neg_integer(descriptor_get(descriptor, :delete_count, 0), 0),
        file_size_bytes:
          normalize_non_neg_integer(descriptor_get(descriptor, :file_size_bytes, 0), 0),
        footer_size: normalize_non_neg_integer(descriptor_get(descriptor, :footer_size, 0), 0),
        encryption_key: normalize_optional_string(descriptor_get(descriptor, :encryption_key))
      }
    else
      nil
    end
  end

  defp normalize_delete_file_descriptor(_descriptor), do: nil

  defp descriptor_get(map, key, default \\ nil) when is_map(map) and is_atom(key) do
    Map.get(map, key, Map.get(map, Atom.to_string(key), default))
  end

  defp bigint_list(opts, key) do
    opts
    |> Keyword.get(key, [])
    |> List.wrap()
    |> Enum.flat_map(fn id ->
      case normalize_optional_integer(id) do
        nil -> []
        value -> [value]
      end
    end)
    |> Enum.uniq()
  end

  defp normalize_non_neg_integer(value, _default)
       when is_integer(value) and value >= 0,
       do: value

  defp normalize_non_neg_integer(value, default) when is_binary(value) do
    case Integer.parse(value) do
      {integer, ""} when integer >= 0 -> integer
      _ -> default
    end
  end

  defp normalize_non_neg_integer(_value, default), do: default

  defp normalize_optional_integer(value) when is_integer(value) and value >= 0, do: value

  defp normalize_optional_integer(value) when is_binary(value) do
    case Integer.parse(value) do
      {integer, ""} when integer >= 0 -> integer
      _ -> nil
    end
  end

  defp normalize_optional_integer(_value), do: nil

  defp normalize_optional_string(value) when is_binary(value) and value != "", do: value
  defp normalize_optional_string(_value), do: nil

  defp snapshot_changes(
         table_stats_row_delta,
         delete_file_count,
         replaced_data_file_ids,
         schema_change_count
       ) do
    change_parts =
      []
      |> maybe_add_change(table_stats_row_delta > 0, "inserted_into_table")
      |> maybe_add_change(delete_file_count > 0, "deleted_from_table")
      |> maybe_add_change(replaced_data_file_ids != [], "compacted_table")
      |> maybe_add_change(schema_change_count > 0, "altered_table")

    case change_parts do
      [] -> "inserted_into_table:{table_id}"
      parts -> Enum.join(parts, ",")
    end
  end

  defp maybe_add_change(changes, false, _change), do: changes
  defp maybe_add_change(changes, true, change), do: changes ++ [change <> ":{table_id}"]

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
