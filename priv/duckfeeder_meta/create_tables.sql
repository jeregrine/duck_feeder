CREATE SCHEMA IF NOT EXISTS duckfeeder_meta;

CREATE TABLE IF NOT EXISTS duckfeeder_meta.checkpoints (
  checkpoint_key TEXT PRIMARY KEY,
  last_committed_lsn PG_LSN NOT NULL DEFAULT '0/0',
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS duckfeeder_meta.snapshot_handoffs (
  source_name TEXT PRIMARY KEY,
  state TEXT NOT NULL CHECK (state IN ('pending', 'complete')),
  boundary_lsn PG_LSN,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  completed_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
