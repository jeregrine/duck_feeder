CREATE SCHEMA IF NOT EXISTS duckfeeder_meta;

CREATE TABLE IF NOT EXISTS duckfeeder_meta.sources (
  id BIGSERIAL PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  connection_info JSONB NOT NULL DEFAULT '{}'::jsonb,
  slot_name TEXT,
  publication_name TEXT,
  status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'paused', 'error')),
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS duckfeeder_meta.designated_tables (
  id BIGSERIAL PRIMARY KEY,
  source_id BIGINT NOT NULL REFERENCES duckfeeder_meta.sources(id) ON DELETE CASCADE,
  source_schema TEXT NOT NULL,
  source_table TEXT NOT NULL,
  target_schema TEXT NOT NULL,
  target_table TEXT NOT NULL,
  mode TEXT NOT NULL DEFAULT 'cdc_changelog' CHECK (mode IN ('cdc_changelog')),
  primary_keys TEXT[] NOT NULL DEFAULT '{}'::text[],
  partition_config JSONB NOT NULL DEFAULT '{}'::jsonb,
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT designated_tables_unique_source_table UNIQUE (source_id, source_schema, source_table)
);

CREATE TABLE IF NOT EXISTS duckfeeder_meta.checkpoints (
  designated_table_id BIGINT PRIMARY KEY REFERENCES duckfeeder_meta.designated_tables(id) ON DELETE CASCADE,
  last_committed_lsn PG_LSN NOT NULL DEFAULT '0/0',
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS duckfeeder_meta.batches (
  id BIGSERIAL PRIMARY KEY,
  batch_id TEXT NOT NULL UNIQUE,
  designated_table_id BIGINT NOT NULL REFERENCES duckfeeder_meta.designated_tables(id) ON DELETE CASCADE,
  lsn_start PG_LSN NOT NULL,
  lsn_end PG_LSN NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('pending', 'encoded', 'uploaded', 'committed', 'failed')),
  error_message TEXT,
  retry_count INTEGER NOT NULL DEFAULT 0,
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT batches_unique_lsn_range UNIQUE (designated_table_id, lsn_start, lsn_end)
);

CREATE INDEX IF NOT EXISTS batches_designated_table_state_idx
  ON duckfeeder_meta.batches (designated_table_id, state);

CREATE TABLE IF NOT EXISTS duckfeeder_meta.batch_files (
  id BIGSERIAL PRIMARY KEY,
  batch_id TEXT NOT NULL REFERENCES duckfeeder_meta.batches(batch_id) ON DELETE CASCADE,
  object_key TEXT NOT NULL,
  row_count BIGINT NOT NULL DEFAULT 0,
  file_size BIGINT NOT NULL DEFAULT 0,
  checksum TEXT,
  etag TEXT,
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT batch_files_unique_object UNIQUE (batch_id, object_key)
);

CREATE TABLE IF NOT EXISTS duckfeeder_meta.schema_history (
  id BIGSERIAL PRIMARY KEY,
  designated_table_id BIGINT NOT NULL REFERENCES duckfeeder_meta.designated_tables(id) ON DELETE CASCADE,
  relation_oid OID,
  schema_version INTEGER NOT NULL DEFAULT 1,
  event_type TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  seen_lsn PG_LSN NOT NULL,
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS schema_history_table_lsn_idx
  ON duckfeeder_meta.schema_history (designated_table_id, seen_lsn DESC);

CREATE SCHEMA IF NOT EXISTS ducklake_metadata;

CREATE TABLE IF NOT EXISTS ducklake_metadata.ducklake_metadata (
  key VARCHAR NOT NULL,
  value VARCHAR NOT NULL,
  scope VARCHAR,
  scope_id BIGINT
);

CREATE TABLE IF NOT EXISTS ducklake_metadata.ducklake_snapshot (
  snapshot_id BIGINT PRIMARY KEY,
  snapshot_time TIMESTAMPTZ,
  schema_version BIGINT,
  next_catalog_id BIGINT,
  next_file_id BIGINT
);

CREATE TABLE IF NOT EXISTS ducklake_metadata.ducklake_snapshot_changes (
  snapshot_id BIGINT PRIMARY KEY,
  changes_made VARCHAR,
  author VARCHAR,
  commit_message VARCHAR,
  commit_extra_info VARCHAR
);

CREATE TABLE IF NOT EXISTS ducklake_metadata.ducklake_schema (
  schema_id BIGINT PRIMARY KEY,
  schema_uuid UUID,
  begin_snapshot BIGINT,
  end_snapshot BIGINT,
  schema_name VARCHAR,
  path VARCHAR,
  path_is_relative BOOLEAN
);

CREATE TABLE IF NOT EXISTS ducklake_metadata.ducklake_table (
  table_id BIGINT,
  table_uuid UUID,
  begin_snapshot BIGINT,
  end_snapshot BIGINT,
  schema_id BIGINT,
  table_name VARCHAR,
  path VARCHAR,
  path_is_relative BOOLEAN
);

CREATE TABLE IF NOT EXISTS ducklake_metadata.ducklake_view (
  view_id BIGINT,
  view_uuid UUID,
  begin_snapshot BIGINT,
  end_snapshot BIGINT,
  schema_id BIGINT,
  view_name VARCHAR,
  dialect VARCHAR,
  sql VARCHAR,
  column_aliases VARCHAR
);

CREATE TABLE IF NOT EXISTS ducklake_metadata.ducklake_tag (
  object_id BIGINT,
  begin_snapshot BIGINT,
  end_snapshot BIGINT,
  key VARCHAR,
  value VARCHAR
);

CREATE TABLE IF NOT EXISTS ducklake_metadata.ducklake_column_tag (
  table_id BIGINT,
  column_id BIGINT,
  begin_snapshot BIGINT,
  end_snapshot BIGINT,
  key VARCHAR,
  value VARCHAR
);

CREATE TABLE IF NOT EXISTS ducklake_metadata.ducklake_data_file (
  data_file_id BIGINT PRIMARY KEY,
  table_id BIGINT,
  begin_snapshot BIGINT,
  end_snapshot BIGINT,
  file_order BIGINT,
  path VARCHAR,
  path_is_relative BOOLEAN,
  file_format VARCHAR,
  record_count BIGINT,
  file_size_bytes BIGINT,
  footer_size BIGINT,
  row_id_start BIGINT,
  partition_id BIGINT,
  encryption_key VARCHAR,
  partial_file_info VARCHAR,
  mapping_id BIGINT
);

CREATE TABLE IF NOT EXISTS ducklake_metadata.ducklake_file_column_stats (
  data_file_id BIGINT,
  table_id BIGINT,
  column_id BIGINT,
  column_size_bytes BIGINT,
  value_count BIGINT,
  null_count BIGINT,
  min_value VARCHAR,
  max_value VARCHAR,
  contains_nan BOOLEAN,
  extra_stats VARCHAR
);

CREATE TABLE IF NOT EXISTS ducklake_metadata.ducklake_delete_file (
  delete_file_id BIGINT PRIMARY KEY,
  table_id BIGINT,
  begin_snapshot BIGINT,
  end_snapshot BIGINT,
  data_file_id BIGINT,
  path VARCHAR,
  path_is_relative BOOLEAN,
  format VARCHAR,
  delete_count BIGINT,
  file_size_bytes BIGINT,
  footer_size BIGINT,
  encryption_key VARCHAR
);

CREATE TABLE IF NOT EXISTS ducklake_metadata.ducklake_column (
  column_id BIGINT,
  begin_snapshot BIGINT,
  end_snapshot BIGINT,
  table_id BIGINT,
  column_order BIGINT,
  column_name VARCHAR,
  column_type VARCHAR,
  initial_default VARCHAR,
  default_value VARCHAR,
  nulls_allowed BOOLEAN,
  parent_column BIGINT
);

CREATE TABLE IF NOT EXISTS ducklake_metadata.ducklake_table_stats (
  table_id BIGINT,
  record_count BIGINT,
  next_row_id BIGINT,
  file_size_bytes BIGINT
);

CREATE TABLE IF NOT EXISTS ducklake_metadata.ducklake_table_column_stats (
  table_id BIGINT,
  column_id BIGINT,
  contains_null BOOLEAN,
  contains_nan BOOLEAN,
  min_value VARCHAR,
  max_value VARCHAR,
  extra_stats VARCHAR
);

CREATE TABLE IF NOT EXISTS ducklake_metadata.ducklake_partition_info (
  partition_id BIGINT,
  table_id BIGINT,
  begin_snapshot BIGINT,
  end_snapshot BIGINT
);

CREATE TABLE IF NOT EXISTS ducklake_metadata.ducklake_partition_column (
  partition_id BIGINT,
  table_id BIGINT,
  partition_key_index BIGINT,
  column_id BIGINT,
  transform VARCHAR
);

CREATE TABLE IF NOT EXISTS ducklake_metadata.ducklake_file_partition_value (
  data_file_id BIGINT,
  table_id BIGINT,
  partition_key_index BIGINT,
  partition_value VARCHAR
);

CREATE TABLE IF NOT EXISTS ducklake_metadata.ducklake_files_scheduled_for_deletion (
  data_file_id BIGINT,
  path VARCHAR,
  path_is_relative BOOLEAN,
  schedule_start TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS ducklake_metadata.ducklake_inlined_data_tables (
  table_id BIGINT,
  table_name VARCHAR,
  schema_version BIGINT
);

CREATE TABLE IF NOT EXISTS ducklake_metadata.ducklake_column_mapping (
  mapping_id BIGINT,
  table_id BIGINT,
  type VARCHAR
);

CREATE TABLE IF NOT EXISTS ducklake_metadata.ducklake_name_mapping (
  mapping_id BIGINT,
  column_id BIGINT,
  source_name VARCHAR,
  target_field_id BIGINT,
  parent_column BIGINT,
  is_partition BOOLEAN
);

CREATE TABLE IF NOT EXISTS ducklake_metadata.ducklake_schema_versions (
  begin_snapshot BIGINT,
  schema_version BIGINT
);

CREATE UNIQUE INDEX IF NOT EXISTS ducklake_table_stats_table_id_idx
  ON ducklake_metadata.ducklake_table_stats (table_id);

CREATE UNIQUE INDEX IF NOT EXISTS ducklake_active_table_idx
  ON ducklake_metadata.ducklake_table (table_id)
  WHERE end_snapshot IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS ducklake_active_column_name_idx
  ON ducklake_metadata.ducklake_column (table_id, column_name)
  WHERE end_snapshot IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS ducklake_column_mapping_table_idx
  ON ducklake_metadata.ducklake_column_mapping (table_id);

CREATE UNIQUE INDEX IF NOT EXISTS ducklake_name_mapping_unique_idx
  ON ducklake_metadata.ducklake_name_mapping (mapping_id, source_name, COALESCE(parent_column, -1));

CREATE UNIQUE INDEX IF NOT EXISTS ducklake_file_column_stats_file_col_idx
  ON ducklake_metadata.ducklake_file_column_stats (data_file_id, column_id);

CREATE UNIQUE INDEX IF NOT EXISTS ducklake_table_column_stats_table_col_idx
  ON ducklake_metadata.ducklake_table_column_stats (table_id, column_id);

INSERT INTO ducklake_metadata.ducklake_snapshot (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id)
SELECT 0, now(), 0, 1, 1
WHERE NOT EXISTS (
  SELECT 1 FROM ducklake_metadata.ducklake_snapshot WHERE snapshot_id = 0
);

INSERT INTO ducklake_metadata.ducklake_snapshot_changes (snapshot_id, changes_made, author, commit_message, commit_extra_info)
SELECT 0, 'created_schema:"main"', NULL, NULL, NULL
WHERE NOT EXISTS (
  SELECT 1 FROM ducklake_metadata.ducklake_snapshot_changes WHERE snapshot_id = 0
);

INSERT INTO ducklake_metadata.ducklake_metadata (key, value, scope, scope_id)
SELECT key, value, NULL, NULL
FROM (VALUES ('version', '0.3'), ('created_by', 'duck_feeder')) AS seed(key, value)
WHERE NOT EXISTS (
  SELECT 1
  FROM ducklake_metadata.ducklake_metadata existing
  WHERE existing.key = seed.key
    AND existing.scope IS NULL
    AND existing.scope_id IS NULL
);

INSERT INTO ducklake_metadata.ducklake_schema (schema_id, schema_uuid, begin_snapshot, end_snapshot, schema_name, path, path_is_relative)
SELECT 0, '00000000-0000-0000-0000-000000000000'::uuid, 0, NULL, 'main', 'main/', true
WHERE NOT EXISTS (
  SELECT 1 FROM ducklake_metadata.ducklake_schema WHERE schema_id = 0
);

INSERT INTO ducklake_metadata.ducklake_schema_versions (begin_snapshot, schema_version)
SELECT 0, 0
WHERE NOT EXISTS (
  SELECT 1
  FROM ducklake_metadata.ducklake_schema_versions
  WHERE begin_snapshot = 0 AND schema_version = 0
);

CREATE TABLE IF NOT EXISTS duckfeeder_meta.ducklake_commits (
  id BIGSERIAL PRIMARY KEY,
  batch_id TEXT NOT NULL REFERENCES duckfeeder_meta.batches(batch_id) ON DELETE CASCADE,
  designated_table_id BIGINT NOT NULL REFERENCES duckfeeder_meta.designated_tables(id) ON DELETE CASCADE,
  target_schema TEXT NOT NULL,
  target_table TEXT NOT NULL,
  object_key TEXT NOT NULL,
  lsn_end TEXT NOT NULL,
  row_count BIGINT NOT NULL DEFAULT 0,
  file_size BIGINT NOT NULL DEFAULT 0,
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT ducklake_commits_unique_batch_object UNIQUE (batch_id, object_key)
);

CREATE INDEX IF NOT EXISTS ducklake_commits_table_lsn_idx
  ON duckfeeder_meta.ducklake_commits (designated_table_id, lsn_end DESC);
