# DuckFeeder Current Implementation Status

This is the active execution board for current priorities.
Detailed historical inventory moved to `docs/current_status_archive.md`.

## Priority TODO (pre-production, pre-compaction context)

- [x] **P0 data-integrity: WAL ack safety**
  - replication ack advancement moved off commit-decode path and onto durable checkpoint feedback (`Service` -> `CDC.Connection` ack messages)
  - keepalive replies no longer auto-advance applied/flushed LSN
- [x] **P0 data-integrity: bootstrap/start LSN correctness for existing slots**
  - runtime now ignores bootstrap skip-forward candidate when slot already exists (preserves checkpoint-based restart)
  - bootstrap-provided start LSN is only applied when slot creation occurs in bootstrap flow
- [x] **P0 recovery: reconcile `:encoded` batches by default**
  - `:encoded` included in default reconcile states and retried back to `:pending`
  - default encoded cleanup path deletes known files when storage is configured
- [x] **P1 runtime safety: bounded ingest/backpressure**
  - batch processing moved off `DuckFeeder.Service` mailbox hot path into bounded task execution
  - service now enforces bounded pending batch queue (`max_inflight_batches`, `max_pending_batches`) and fails closed on overflow
- [ ] **P1 reliability: storage retry/timeout policy**
  - [x] retry/backoff + explicit timeout defaults added for S3/GCS request paths
  - [ ] expand provider-backed failure matrix (transient 5xx, timeout, retry recovery, reconcile cleanup)
- [x] **P1 append-stream runtime parity with service**
  - [x] moved `DuckFeeder.AppendStream` batch processing off GenServer mailbox hot path (async bounded worker model)
  - [x] added append-stream queue bounds/concurrency controls equivalent to `Service` (`max_inflight_batches`, `max_pending_batches`)
  - [x] added append-only overload shedding mode (`overflow_strategy: :drop_oldest`) while keeping CDC `Service` fail-closed
- [x] **P1/P2 observability + operator guidance**
  - [x] emit telemetry for service/append queue depth + inflight tasks + overflow risk
  - [x] emit/standardize ack-vs-checkpoint lag telemetry and alerting guidance
  - [x] document `max_lag_bytes` / `backpressure_lag_bytes` as effectively required for bounded WAL retention in production
  - [x] document credential flow expectations now that persisted connection info is redacted (runtime creds must come from startup opts/env/secret source)
- [ ] **P2 hardening/perf/security follow-ups**
  - [x] stream GCS uploads (avoid full-file memory reads)
  - [x] remove dynamic atom creation from external config normalization / connection override key paths
  - [x] optimize `BatchBuffer` row-size estimation hot path (remove `term_to_binary` per-row sizing)
  - [x] optimize parquet NIF serialization hot path (removed Elixir JSON encode + Rust JSON decode bridge; decode terms directly in Rust NIF)
  - [x] add best-effort temp-file reaper (`DuckFeeder.TempFileReaper`) integrated into writer temp-path creation
  - [x] persisted source connection info now strips credentials (password/secret/token keys removed; URL fields persisted in redacted form)
  - [x] add poison-row/dead-letter policy for non-serializable row isolation (`poison_row_mode: :drop`, poison row sink callbacks/messages, telemetry)
  - [x] optimize wide-table DuckLake commit SQL fanout (bulk DuckLake column/mapping/stats statements)

## Current snapshot

- End-to-end architecture is implemented: **Postgres CDC/WAL → Parquet → object storage → DuckLake metadata in Postgres**.
- Snapshot/WAL handoff hardening is in place with durable pending/complete markers and explicit resume semantics.
- Core write/upload/commit flow is shared across CDC runtime and append streams.
- Added app-facing smart-default runtime wrapper (`use DuckFeeder.Runtime`) with repo/schema inference and configurable `metadata_repo` (defaults to `repo`).
- Provider-backed integration now covers:
  - storage roundtrips,
  - append-stream commit path,
  - runtime CDC commit path,
  - snapshot-handoff recovery path (pending → explicit resume).
- Replication/client observability includes lag/disconnect/backpressure telemetry, service+append queue telemetry, append dropped-batch telemetry, and service ack-vs-checkpoint lag telemetry.
- Overflow policy is explicit: CDC `Service` remains fail-closed; `AppendStream` supports `overflow_strategy: :drop_oldest` for availability-first lossy workloads.
- Added `DuckFeeder.TelemetryForwarder` helper for safe app telemetry ingestion (DuckFeeder-event summarization/debounce + recursion suppression).
- Ecto demo integration exists for realistic B2B SaaS write/update/delete flows with ADBC DuckDB parquet verification.

## Remaining workstreams (condensed)

- **Provider-backed failure/reconcile depth:** expand transient failure matrix (timeouts/retries/restarts/orphan cleanup) on S3/GCS.
- **Operational alert policy tuning:** wire queue/ack lag telemetry into concrete SLO-driven alert thresholds.
- **Parquet hardening:** typing/perf/compatibility improvements beyond current term-bridge removal.
- **Security/ops hardening:** expand poison-row policy ergonomics (routing/retention/audit tooling) for production ops.
- **DuckLake deep parity (phase 2):** broader nested-field and conflict/concurrency semantics.

## Ongoing Apache-2.0 obligations (ElectricSQL LSN-derived code)

- Keep attribution/modification comments in `lib/duck_feeder/postgrex/extensions/pg_lsn.ex`.
- Keep Apache-2.0 license text in `third_party/electric/LICENSE` in redistributions.
- If additional ElectricSQL code is copied/adapted, document file-level provenance and any NOTICE obligations.

## Local test status

- `mix test` passes for current codebase.
- Integration tests require `:duck_feeder, :integration` DB URLs in `config/test.exs`.
- Optional test tags:
  - `:provider_integration` (excluded by default)
  - `:ecto_integration` (excluded by default)

## Archive / detailed history

See `docs/current_status_archive.md` for:
- exhaustive completed checklists,
- detailed DuckLake write-parity checklist,
- prior expanded planning context.
