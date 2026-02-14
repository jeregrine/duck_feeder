# DuckFeeder Current Implementation Status

This is the single source of truth task list for project status and next work.

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
  - [x] explicit Elixir table-selection for seeding (`seed_meta/3` `tables:` option)
  - [x] per-table target remapping (`{"target_table", "source_table"}` selection form)

- [x] **Migration integration**
  - [x] `DuckFeeder.Migrations` / `DuckFeeder.Migration` helpers for Ecto migration wrappers
  - [x] migration up/down/version flow via repo SQL execution

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
  - [x] default snapshot row replay into service when `snapshot_before_stream?: true` and no explicit row handler
  - [x] snapshot replay uses boundary-based synthetic LSN allocation for stable snapshot/WAL handoff
  - [x] snapshot-before-stream defaults to cold-start behavior (skip replay when checkpoint already exists; opt-in `snapshot_on_restart?: true`)
  - [x] snapshot/bootstrapping runtime hardening: query connection cleanup + exception-safe bootstrap/snapshot execution paths
  - [x] snapshot replay ingestion hardening: catches service ingest exits/exceptions and tears down service safely
  - [x] durable snapshot handoff markers in metadata (`duckfeeder_meta.snapshot_handoffs`: `pending`/`complete`)
  - [x] fail-closed restart behavior for incomplete snapshot handoff (`resume_incomplete_snapshot?: true` required)
  - [x] pending-handoff resume can fast-forward to `complete` without rerunning snapshot when checkpoint is already at/after boundary
  - [x] partial snapshot replay resumes from checkpoint progress within the synthetic LSN window (skip already-replayed snapshot rows)
  - [x] durable handoff marker writes include bounded retry policy (`snapshot_handoff_mark_retries`, `snapshot_handoff_mark_retry_delay_ms`)
  - [x] `DuckFeeder.CDC.Connection` (`Postgrex.ReplicationConnection` stream client)
  - [x] replication setup hardening for bootstrap races (duplicate publication/slot create treated as `:exists`)
  - [x] replication slot lifecycle validation (existing slot type/plugin checks in `ensure_slot`)
  - [x] CDC disconnect telemetry now includes lag/reason context for reconnect tuning (`:disconnecting`, `:disconnected`)
  - [x] runtime bootstrap integration (`DuckFeeder.Runtime.start_stream/4` + `DuckFeeder.CDC.Bootstrap`)
  - [x] reconnect-backoff passthrough for replication startup (`reconnect_backoff`)
  - [x] reconnect-backoff tuning knobs for runtime startup (`reconnect_backoff_min_ms`, `reconnect_backoff_max_ms`, `reconnect_backoff_jitter_ms`, `reconnect_backoff_jitter_fun`)
  - [x] default reconnect-backoff applied for CDC stream startup (`1_000ms` when unset)
  - [x] max replication lag guard (`max_lag_bytes`)
  - [x] backpressure threshold instrumentation (`backpressure_lag_bytes` with enter/clear telemetry transitions)
  - [x] configurable CDC event sink mode (`event_sink_mode: :pid | :call`)
  - [x] `DuckFeeder.CDC.Pipeline`

- [x] **Ingest/batching foundations**
  - [x] `DuckFeeder.Ingest.BatchBuffer`
  - [x] `DuckFeeder.TablePipeline`
  - [x] `DuckFeeder.Ingest` orchestrator
  - [x] `DuckFeeder.AppendStream` generic append-event batching pipeline (table-name keyed)

- [x] **Write/upload/commit orchestration foundation**
  - [x] `DuckFeeder.Writer` adapter interface
  - [x] temporary `DuckFeeder.Writer.Jsonl` adapter
  - [x] `DuckFeeder.Writer.ParquetNif` (Rustler-backed parquet writer)
  - [x] optional Elixir-side datetime normalization to unix microseconds (`writer.datetime_encoding`)
  - [x] writer format selection + parquet fallback wiring
  - [x] `DuckFeeder.BatchProcessor`
  - [x] `DuckFeeder.DuckLake.Committer` interface + no-op committer
  - [x] `DuckFeeder.DuckLake.Committer.Postgres` transactional scaffold
  - [x] DuckLake spec-table bootstrap aligned to `ducklake-web` table definitions (`ducklake_metadata.*`)
  - [x] default spec commit path writes snapshot/table/column/mapping/data_file/stats/snapshot_changes/schema_versions
  - [x] default spec commit path writes table/file column stats (`ducklake_table_column_stats`, `ducklake_file_column_stats`)
  - [x] default spec commit path supports optional delete-file metadata + replacement end-snapshot transitions (`ducklake_delete_file`, retire prior `ducklake_data_file`/`ducklake_delete_file` rows)
  - [x] replacement flow now schedules retired data files in `ducklake_files_scheduled_for_deletion`
  - [x] snapshot change summaries include schema-evolution/conflict hints (`created_table`, `altered_table`, plus insert/delete/compact markers)
  - [x] optional schema-change directives in commit path (`schema_changes`: `rename_table`/`rename_column`/`drop_column`/`alter_column_type`)
  - [x] nested-field style schema-change aliases in commit path (`rename_field`/`drop_field`/`alter_field_type` with dotted paths)
  - [x] optional partition metadata writes in commit path (`partition_by`, `partition_values` -> `ducklake_partition_info` / `ducklake_partition_column` / `ducklake_file_partition_value`)
  - [x] schema-change directives now use history-style versioning for table/column rename and type-change (close prior row + insert new version)
  - [x] schema-change conflict guards added for rename/drop/type-change (including type-promotion checks)
  - [x] batch processor supports optional physical delete-file production/upload/validation (`committer_opts[:delete_files_fun]` / `committer_opts[:delete_files]` + `validate_delete_files?`)
  - [x] default schema/commit history record (`duckfeeder_meta.schema_history`)
  - [x] default commit-log SQL target (`duckfeeder_meta.ducklake_commits`)
  - [x] `DuckFeeder.Service` end-to-end wiring module
  - [x] `DuckFeeder.Runtime` metadata-driven service boot wiring
  - [x] `DuckFeeder.Runtime.start_stream/4` service + CDC stream startup wiring
  - [x] documented migration-ordering contract for schema-evolution rollouts (start runtime/replication before migrations)
  - [x] `DuckFeeder.Runtime.StreamWorker` managed stream lifecycle wrapper
  - [x] `DuckFeeder.Runtime.Supervisor` stream+reconciler lifecycle wrapper
  - [x] `DuckFeeder.Runtime.Manager` dynamic multi-source runtime manager
  - [x] runtime restart behavior coverage (stream worker restart on child failure, manager re-start after source exit)
  - [x] existing-app runtime child-spec helpers (`DuckFeeder.Integration`)

- [x] **Observability foundations**
  - [x] telemetry helper module (`DuckFeeder.Telemetry`)
  - [x] CDC event telemetry (`[:duck_feeder, :cdc, :event]`)
  - [x] CDC lag telemetry (`[:duck_feeder, :cdc, :lag]`)
  - [x] CDC backpressure telemetry (`[:duck_feeder, :cdc, :backpressure]`)
  - [x] batch flush telemetry (`[:duck_feeder, :batch, :flushed]`)
  - [x] batch processed telemetry (`[:duck_feeder, :batch, :processed]`)

- [x] **Recovery foundation**
  - [x] basic stale batch reconciler helper (`DuckFeeder.Reconciler`)
  - [x] failed-batch cleanup/retry option (delete known files + move `failed` -> `pending`)
  - [x] optional failed-batch file requirement guard (`require_failed_batch_files?`)
  - [x] uploaded-object verification option (`verify_uploaded_objects?`)
  - [x] reconcile safety controls (`max_batches`, `stop_on_error?`)
  - [x] scheduled reconciler worker (`DuckFeeder.Reconciler.Worker`)

- [x] **Test harness foundations**
  - [x] unit tests for all current modules
  - [x] unit coverage for delete-file/replacement SQL statement generation and execution path
  - [x] integration test file for meta store (test-config-gated)
  - [x] integration test file for CDC connection stream (test-config-gated)
  - [x] integration test file for runtime start_stream end-to-end flow (test-config-gated)
  - [x] integration coverage for snapshot->WAL handoff replay behavior (preexisting rows + post-snapshot WAL)
  - [x] integration coverage for restart handoff behavior (checkpointed restart skips duplicate snapshot replay)
  - [x] integration coverage for larger snapshot replay windows (multi-batch snapshot replay before WAL continuation)
  - [x] integration coverage for pending snapshot handoff gating + explicit resume path
  - [x] integration coverage for CDC-start failure after snapshot replay (pending handoff marker then explicit resume)
  - [x] integration test file for append stream end-to-end flow (test-config-gated)
  - [x] integration coverage for optional delete-file metadata commits (`ducklake_delete_file` + snapshot change marker)
  - [x] integration coverage for physically produced delete files (`delete_files_fun` rows -> writer/upload -> metadata rows)
  - [x] integration coverage for replacement/end-snapshot transitions (retire prior `ducklake_data_file` + `ducklake_delete_file` rows)
  - [x] integration coverage for replacement cleanup scheduling (`ducklake_files_scheduled_for_deletion`)
  - [x] integration coverage for schema-evolution snapshot markers (`created_table`, `altered_table`)
  - [x] integration coverage for schema-change directives (`rename_table`, `rename_column`, `drop_column`, `alter_column_type`)
  - [x] integration coverage for nested-field style schema-change aliases (`rename_field`, `drop_field`, `alter_field_type`)
  - [x] integration coverage for schema-change conflict rejection (non-promotable type change)
  - [x] integration coverage for guarded rename conflict scenario (competing target rename)
  - [x] integration coverage for partition metadata writes (`ducklake_partition_info`, `ducklake_partition_column`, `ducklake_file_partition_value`)
  - [x] integration coverage for add-files style compatibility subset (missing/extra columns across commits)
  - [x] integration coverage for replacement scheduling idempotence
  - [x] integration coverage for stats robustness subset (null/nan flags + min/max continuity)
  - [x] tracer-shot assertions include row-level values, parquet type checks, and DuckLake metadata row verification (spec-table columns)
  - [x] failure-injection integration scenario for reconcile cleanup (`failed` -> `pending` + file deletion)
  - [x] strict failed-cleanup integration scenario for missing file metadata (`require_failed_batch_files?`)
  - [x] optional provider-backed storage integration roundtrips (S3-compatible + GCS, env-gated)
  - [x] Ecto demo integration for B2B SaaS-style schemas/writes (insert/update/delete) with ADBC DuckDB parquet verification (`:ecto_integration` tag)
  - [x] baseline Benchee suite (single-writer CDC tx benchmarks + multi-writer append-stream benchmarks)
  - [x] helper script for integration runs (local pg + duckdb prerequisites)

- [x] **Third-party license compliance (ElectricSQL LSN reference)**
  - [x] added source attribution + modification notes in `lib/duck_feeder/postgrex/extensions/pg_lsn.ex`
  - [x] vendored Apache-2.0 text at `third_party/electric/LICENSE`
  - [x] documented compliance notes in this status file

## Ongoing Apache-2.0 obligations (ElectricSQL LSN-derived code)

- Keep attribution/modification comments in `lib/duck_feeder/postgrex/extensions/pg_lsn.ex`.
- Keep Apache-2.0 license text in `third_party/electric/LICENSE` in redistributions.
- If additional ElectricSQL code is copied/adapted, document file-level provenance in this status file.
- If upstream adds a NOTICE file and we distribute covered code, include applicable NOTICE content.

## Remaining to reach target architecture

- [ ] **Full integration suite** (Postgres + S3 + GCS + metadata DB)
- [ ] **Replication client hardening** (bootstrap lifecycle, reconnect policy tuning, backpressure/metrics)
- [ ] **Parquet writer hardening** (type fidelity, performance tuning, and compatibility validation)
- [ ] **Advanced recovery/reconciler loop** (orphan detection, policy tuning, large-scale cleanup safety)
- [ ] **Append event stream integrations** (`:telemetry`/Logger/error adapters over `DuckFeeder.AppendStream`)
- [ ] **DuckLake metadata SQL commit phase 2 (deep parity)** (full nested-field tree semantics, broader conflict-rule/concurrency matrix, compaction policy hardening)

## DuckLake write-path parity checklist (reference: `/tmp/ducklake/test/sql`)

Scope note: this checklist compares **write semantics only** (not read/query planner behavior),
using DuckLake SQLLogicTests as inspiration for metadata/write-path coverage.

- [x] **Top-level schema evolution directives** (`rename_table`, `rename_column`, `drop_column`, `alter_column_type`)
  - Status: **covered**
  - Our tests: `test/duck_feeder/duck_lake/committer/postgres_integration_test.exs`, `sql_test.exs`, `postgres_test.exs`

- [x] **Nested-field schema evolution (top-level path alias form)**
  - Status: **covered for ingest-writer scope**
  - Covered via dotted-path directives (`rename_field`/`drop_field`/`alter_field_type`) and integration assertions
  - Note: full DuckLake nested parent/child tree semantics are future deep-parity work

- [x] **Delete/replacement lifecycle semantics** (delete files, end_snapshot transitions, scheduled cleanup)
  - Status: **covered for ingest-writer scope**
  - Covered: delete metadata, physical delete-file production, replacement retirement, scheduled deletion rows, idempotent scheduling checks

- [x] **Compaction maintenance semantics (metadata-oriented subset)**
  - Status: **covered for ingest-writer scope**
  - Covered: replacement cleanup scheduling + idempotence + compacted snapshot markers
  - Note: full policy/tiered compaction matrix remains future deep-parity work

- [x] **Transaction conflict/concurrency write semantics (guarded conflict subset)**
  - Status: **covered for ingest-writer scope**
  - Covered: rename/drop/type conflict guards + competing rename conflict integration case

- [x] **Add-files style compatibility matrix (core subset)**
  - Status: **covered for ingest-writer scope**
  - Covered: missing/extra column commits across batches with schema continuity assertions

- [x] **Partition-aware write metadata semantics (core subset)**
  - Status: **covered for ingest-writer scope**
  - Covered: partition info/column/file-partition-value writes with integration assertions

- [x] **Stats robustness matrix (core subset)**
  - Status: **covered for ingest-writer scope**
  - Covered: high-water mark behavior for `contains_null`/`contains_nan` and min/max continuity assertions

## Next steps (soft plan)

1. **Full integration suite expansion**
   - keep local filesystem-backed integration as the primary gate now
   - expand provider-backed S3/GCS from storage roundtrips into broader runtime/commit matrix coverage

2. **Replication client hardening (phase 2)**
   - reconnect policy tuning refinements and reconnect/backpressure alerting policy

3. **Parquet writer hardening (phase 2)**
   - add more precise typing for temporal/decimal-like fields where practical
   - tune performance and compatibility across DuckDB/object-store readers
   - prefer Elixir-side normalization/casting for temporal values to keep Rust deps minimal

4. **Recovery/reconcile hardening (phase 2)**
   - add orphan-detection integration cases and larger-batch cleanup safety checks

5. **Append event stream integrations**
   - implement `:telemetry` / Logger / error adapters over `DuckFeeder.AppendStream`

6. **DuckLake metadata maturation (phase 2, deep-parity)**
   - expand from ingest-writer subset to full DuckLake nested parent/child field-tree semantics
   - extend conflict/concurrency matrix toward DuckLake extension breadth
   - complete compaction policy/tiered maintenance hardening and related integration assertions

7. **Dependency footprint minimization (Elixir + Rust)**
   - keep runtime deps minimal and avoid heavy crates unless required
   - bias toward Elixir-side transforms over Rust parsing when both are viable

## Local test status

- `mix test` passes for current codebase.
- Integration tests require `:duck_feeder, :integration` DB URLs in `config/test.exs`.
