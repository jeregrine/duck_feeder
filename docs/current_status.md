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
  - [x] integration test file for meta store (test-config-gated)
  - [x] integration test file for CDC connection stream (test-config-gated)
  - [x] integration test file for runtime start_stream end-to-end flow (test-config-gated)
  - [x] helper script for integration runs (local pg + duckdb prerequisites)

- [x] **Third-party license compliance (ElectricSQL LSN reference)**
  - [x] added source attribution + modification notes in `lib/duck_feeder/postgrex/extensions/pg_lsn.ex`
  - [x] vendored Apache-2.0 text at `third_party/electric/LICENSE`
  - [x] documented compliance notes in `docs/third_party_licenses.md`
  - [x] linked third-party licensing notes from `README.md`

## Ongoing Apache-2.0 obligations (ElectricSQL LSN-derived code)

- Keep attribution/modification comments in `lib/duck_feeder/postgrex/extensions/pg_lsn.ex`.
- Keep Apache-2.0 license text in `third_party/electric/LICENSE` in redistributions.
- If additional ElectricSQL code is copied/adapted, document file-level provenance in `docs/third_party_licenses.md`.
- If upstream adds a NOTICE file and we distribute covered code, include applicable NOTICE content.

## Remaining to reach target architecture

- [ ] **Replication client hardening** (bootstrap lifecycle, reconnect policy tuning, backpressure/metrics)
- [ ] **Production initial snapshot + WAL handoff path** (direct ingest integration + replay validation)
- [ ] **Parquet writer hardening** (type fidelity, performance tuning, and compatibility validation)
- [ ] **Full DuckLake metadata SQL commit implementation** (table metadata/stats/history beyond snapshot+file append path)
- [ ] **Advanced recovery/reconciler loop** (orphan detection, policy tuning, large-scale cleanup safety)
- [ ] **Full integration suite** (Postgres + S3 + GCS + metadata DB)

## Next steps (soft plan)

1. **Stabilize tracer-shot E2E gate**
   - keep `mix test --only integration` green as a release gate
   - add value-level assertions (not only row counts) on DuckDB DuckLake reads

2. **Parquet typing hardening**
   - improve scalar type fidelity (int/float/bool/timestamp) in parquet output
   - keep safe fallback behavior for mixed or ambiguous columns

3. **DuckLake metadata maturation**
   - incrementally align metadata writes toward fuller DuckLake spec coverage
   - preserve current ingest path while expanding snapshot/history/table metadata depth

4. **Recovery/reconcile hardening**
   - add failure-injection integration scenarios (write/upload/commit boundaries)
   - verify orphan cleanup and state convergence under retries

5. **Runtime reliability tuning**
   - tune reconnect/backoff/lag guard defaults under sustained load
   - add long-run restart/reconnect behavior tests

6. **Developer ergonomics**
   - keep `config/test.exs` as the default integration harness config surface
   - document local Postgres logical-replication prerequisites clearly in README

7. **Explicit table-selection config/API**
   - add a clear Elixir-first table registration surface ("sync these tables")
   - support per-table mapping from Postgres source table to DuckLake target table
   - document parity goals with tools that provide table-creation/sync helpers (e.g. Moonlink-style workflows)

## Local test status

- `mix test` passes for current codebase.
- Integration tests require `:duck_feeder, :integration` DB URLs in `config/test.exs`.
