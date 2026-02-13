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
