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
  - [x] config-driven metadata seeding helper (`DuckFeeder.Bootstrap.seed_meta/3`)
  - [x] seed-and-start convenience (`DuckFeeder.Bootstrap.seed_and_start_stream/3`)

- [x] **CDC foundations**
  - [x] `DuckFeeder.CDC.Event`
  - [x] `DuckFeeder.CDC.TransactionBuffer`
  - [x] `DuckFeeder.CDC.Router`
  - [x] `DuckFeeder.CDC.ChangelogRow`
  - [x] `DuckFeeder.CDC.Lsn`
  - [x] `DuckFeeder.CDC.Setup` (publication/slot SQL helpers)
  - [x] `DuckFeeder.CDC.Bootstrap` (startup orchestration for publication/slot/LSN)
  - [x] `DuckFeeder.CDC.ReplicationProtocol` (replication SQL + status message encoding)
  - [x] `DuckFeeder.CDC.LogicalReplication.Messages` (pgoutput wire message structs)
  - [x] `DuckFeeder.CDC.LogicalReplication.Decoder` (pgoutput wire decoding)
  - [x] `DuckFeeder.CDC.LogicalReplication.Converter` (wire message -> normalized event conversion)
  - [x] `DuckFeeder.CDC.MessageMapper` (generic message->event mapping)
  - [x] `DuckFeeder.CDC.SnapshotBoundary`
  - [x] `DuckFeeder.CDC.InitialSnapshot` (snapshot tx + copy SQL helpers)
  - [x] `DuckFeeder.CDC.InitialSnapshot.Runner` (table copy + row dispatch orchestration)
  - [x] optional runtime snapshot-before-stream hook (`DuckFeeder.Runtime.start_stream/4`)
  - [x] `DuckFeeder.CDC.Connection` (`Postgrex.ReplicationConnection` stream client)
  - [x] runtime bootstrap integration (`DuckFeeder.Runtime.start_stream/4` + `DuckFeeder.CDC.Bootstrap`)
  - [x] reconnect-backoff passthrough for replication startup (`reconnect_backoff`)
  - [x] max replication lag guard (`max_lag_bytes`)
  - [x] configurable CDC event sink mode (`event_sink_mode: :pid | :call`)
  - [x] `DuckFeeder.CDC.Pipeline`

- [x] **Ingest/batching foundations**
  - [x] `DuckFeeder.Ingest.BatchBuffer`
  - [x] `DuckFeeder.TablePipeline`
  - [x] `DuckFeeder.Ingest` orchestrator

- [x] **Write/upload/commit orchestration foundation**
  - [x] `DuckFeeder.Writer` adapter interface
  - [x] temporary `DuckFeeder.Writer.Jsonl` adapter
  - [x] `DuckFeeder.Writer.ParquetNif` (Rustler-backed parquet writer)
  - [x] writer format selection + parquet fallback wiring
  - [x] `DuckFeeder.BatchProcessor`
  - [x] `DuckFeeder.DuckLake.Committer` interface + no-op committer
  - [x] `DuckFeeder.DuckLake.Committer.Postgres` transactional scaffold
  - [x] default spec-aligned snapshot/file/change SQL scaffold (`ducklake_metadata.*`)
  - [x] default spec-aligned table stats refresh (`ducklake_metadata.ducklake_table_stats`)
  - [x] default schema/commit history record (`duckfeeder_meta.schema_history`)
  - [x] default commit-log SQL target (`duckfeeder_meta.ducklake_commits`)
  - [x] `DuckFeeder.Service` end-to-end wiring module
  - [x] `DuckFeeder.Runtime` metadata-driven service boot wiring
  - [x] `DuckFeeder.Runtime.start_stream/4` service + CDC stream startup wiring
  - [x] `DuckFeeder.Runtime.StreamWorker` managed stream lifecycle wrapper
  - [x] `DuckFeeder.Runtime.Supervisor` stream+reconciler lifecycle wrapper
  - [x] `DuckFeeder.Runtime.Manager` dynamic multi-source runtime manager
  - [x] existing-app supervision integration runbook (`docs/existing_app_supervision.md`)
  - [x] existing-app runtime child-spec helpers (`DuckFeeder.Integration`)

- [x] **Observability foundations**
  - [x] telemetry helper module (`DuckFeeder.Telemetry`)
  - [x] CDC event telemetry (`[:duck_feeder, :cdc, :event]`)
  - [x] batch flush telemetry (`[:duck_feeder, :batch, :flushed]`)
  - [x] batch processed telemetry (`[:duck_feeder, :batch, :processed]`)

- [x] **Recovery foundation**
  - [x] basic stale batch reconciler helper (`DuckFeeder.Reconciler`)
  - [x] failed-batch cleanup/retry option (delete known files + move `failed` -> `pending`)
  - [x] uploaded-object verification option (`verify_uploaded_objects?`)
  - [x] reconcile safety controls (`max_batches`, `stop_on_error?`)
  - [x] scheduled reconciler worker (`DuckFeeder.Reconciler.Worker`)

- [x] **Test harness foundations**
  - [x] unit tests for all current modules
  - [x] integration test file for meta store (env-gated)
  - [x] integration test file for CDC connection stream (env-gated)
  - [x] integration test file for runtime start_stream end-to-end flow (env-gated)
  - [x] helper script for integration runs (local pg + duckdb prerequisites)

## Remaining to reach target architecture

- [ ] **Replication client hardening** (bootstrap lifecycle, reconnect policy tuning, backpressure/metrics)
- [ ] **Production initial snapshot + WAL handoff path** (direct ingest integration + replay validation)
- [ ] **Parquet writer hardening** (type fidelity, performance tuning, and compatibility validation)
- [ ] **Full DuckLake metadata SQL commit implementation** (table metadata/stats/history beyond snapshot+file append path)
- [ ] **Advanced recovery/reconciler loop** (orphan detection, policy tuning, large-scale cleanup safety)
- [ ] **Full integration suite** (Postgres + S3 + GCS + metadata DB)

## Local test status

- `mix test` passes for current codebase.
- Integration tests require `DUCK_FEEDER_META_DATABASE_URL` and `DUCK_FEEDER_SOURCE_DATABASE_URL`.
