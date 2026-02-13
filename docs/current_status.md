# DuckFeeder Current Implementation Status

This tracks progress against `docs/plan_compact.md`.

## Completed

- [x] **Storage abstraction**
  - [x] `DuckFeeder.Storage.Adapter`
  - [x] `DuckFeeder.Storage.S3` (Req + SigV4, single PUT + multipart)
  - [x] `DuckFeeder.Storage.GCS` (Req + OAuth Bearer)

- [x] **Config validation**
  - [x] `DuckFeeder.Config` (source/storage/metadata/ingest)
  - [x] provider-specific validation (S3 creds, GCS token/token_fun)

- [x] **Meta control-plane schema + API**
  - [x] `priv/duckfeeder_meta/create_tables.sql`
  - [x] `DuckFeeder.Meta.Store` CRUD/state machine helpers
  - [x] idempotent `commit_uploaded_batch/2` checkpoint advancement

- [x] **CDC foundations**
  - [x] `DuckFeeder.CDC.Event`
  - [x] `DuckFeeder.CDC.TransactionBuffer`
  - [x] `DuckFeeder.CDC.Router`
  - [x] `DuckFeeder.CDC.ChangelogRow`
  - [x] `DuckFeeder.CDC.Lsn`
  - [x] `DuckFeeder.CDC.Setup` (publication/slot SQL helpers)
  - [x] `DuckFeeder.CDC.MessageMapper` (generic message->event mapping)
  - [x] `DuckFeeder.CDC.SnapshotBoundary`
  - [x] `DuckFeeder.CDC.Pipeline`

- [x] **Ingest/batching foundations**
  - [x] `DuckFeeder.Ingest.BatchBuffer`
  - [x] `DuckFeeder.TablePipeline`
  - [x] `DuckFeeder.Ingest` orchestrator

- [x] **Write/upload/commit orchestration foundation**
  - [x] `DuckFeeder.Writer` adapter interface
  - [x] temporary `DuckFeeder.Writer.Jsonl` adapter
  - [x] `DuckFeeder.BatchProcessor`
  - [x] `DuckFeeder.Service` end-to-end wiring module

- [x] **Observability foundations**
  - [x] telemetry helper module (`DuckFeeder.Telemetry`)
  - [x] CDC event telemetry (`[:duck_feeder, :cdc, :event]`)
  - [x] batch flush telemetry (`[:duck_feeder, :batch, :flushed]`)
  - [x] batch processed telemetry (`[:duck_feeder, :batch, :processed]`)

- [x] **Test harness foundations**
  - [x] unit tests for all current modules
  - [x] integration test file for meta store (env-gated)
  - [x] docker compose stack + helper script for integration runs

## Remaining to reach target architecture

- [ ] **Real Postgres logical replication client** (`Postgrex.ReplicationConnection`)
- [ ] **pgoutput wire decoding** into `DuckFeeder.CDC.Event`
- [ ] **Initial snapshot + WAL handoff**
- [ ] **Parquet writer adapter** (Rustler/NIF path)
- [ ] **DuckLake metadata SQL commit implementation** (spec-aligned)
- [ ] **Recovery/reconciler loop** (orphan files, stuck batches)
- [ ] **Full integration suite** (Postgres + S3 + GCS + metadata DB)

## Local test status

- `mix test` passes for current codebase.
- Integration tests require `DUCK_FEEDER_META_DATABASE_URL`.
