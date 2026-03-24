# AGENTS.md

## Project Overview
DuckFeeder is an Elixir runtime for mirroring Postgres data into DuckDB-managed tables:

- **Postgres logical replication (WAL/CDC)** — decode pgoutput, route changes, apply as table operations
- **Direct DuckDB writes** via ADBC — inserts, merges, deletes, and truncate-clears into real target tables
- **Append-only event streams** — non-CDC producers (telemetry, logs, domain events) into the same DuckDB database
- **Checkpoint durability** — Postgres-backed metadata store for restart correctness

DuckFeeder owns replication correctness, batching, snapshot/WAL handoff, and checkpoint discipline.
DuckDB owns table storage and query surface.

Core durability rule: **WAL ACK advances only after DuckDB table writes are committed and the checkpoint is durably persisted.**

## Current Runtime/Product Decisions
- Elixir-first architecture, OTP-supervised runtime.
- Source/table registry is **config-first**: source settings and designated tables come from app config / Ecto schemas, not persisted metadata tables.
- DuckDB access is through **ADBC** (`{:adbc, "~> 0.8"}`). No Parquet NIF, no object storage.
- CDC path is fail-closed under sustained overload.
- Append stream defaults to fail-closed but can optionally use lossy overflow policy (`overflow_strategy: :drop_oldest`).
- Telemetry helper shipped: `DuckFeeder.TelemetryForwarder`.
- Single-writer model for DuckDB (coordinated via Elixir supervision, similar to how `fly_postgres` works with Ecto).

## Dependencies
Runtime deps (from `mix.exs`):
- `postgrex ~> 0.20` — Postgres connections + replication protocol
- `adbc ~> 0.8` — DuckDB access
- `nimble_options ~> 1.1` — config validation
- `jason ~> 1.4` — optional dependency

Dev/test only:
- `ecto_sql ~> 3.12` (test) — migration integration tests
- `benchee ~> 1.3` (dev)
- `ex_doc ~> 0.38` (dev)

## Data Flow

### CDC path
```
Postgres WAL
  → CDC.Connection (Postgrex.ReplicationConnection)
  → CDC.Pipeline (TransactionBuffer → Ingest)
  → TablePipeline (micro-batch buffer)
  → Sink.DuckDB (MERGE/DELETE/INSERT via ADBC)
  → Meta.Store.upsert_checkpoint (Postgres)
  → CDC.Connection ack_lsn
```

### Append path
```
AppendStream.append/4
  → TablePipeline (micro-batch buffer)
  → Sink.DuckDB (INSERT via ADBC)
  → Meta.Store.upsert_checkpoint (Postgres)
```

## VCS / Workflow Conventions
- Use **jj** for local commit/bookmark flow.
- Main branch is authoritative for release config.
- Prefer version bumps for releases; avoid retagging old versions unless explicitly needed.

## Metadata Store
Durable control-plane state lives in `duckfeeder_meta` schema in Postgres:
- `checkpoints` — per-target-table last committed LSN, keyed by stable `checkpoint_key` values such as `source-a:raw.users`
- `snapshot_handoffs` — initial snapshot state machine, keyed by `source_name`
- `migration_versions` — migration bookkeeping created by `DuckFeeder.Migration`

Bootstrap via `DuckFeeder.Meta.Store.bootstrap/1` or `DuckFeeder.Migration.up/1`.

## Current Cleanup / Design Direction
- Keep metadata minimal and durable.
- Keep source name / slot / publication / designated tables in app config or Ecto-derived runtime config.
- Prefer real DuckDB tables over warehouse-specific envelopes.
- Preserve loud failure semantics over silent fallback behavior.

## Known Technical Debt
Current notable items:
- `Sink.DuckDB` still builds SQL via string interpolation; continue auditing validation/escaping paths.
- `rows_source/1` still generates large `VALUES` clauses and may not scale to very large batches.
- `infer_columns/1` is still more expensive than it should be for wide batches.
- There is still private helper duplication between `Service` and `AppendStream`.
- Append-stream restart semantics still rely on caller-provided synthetic LSN continuity.

## Hex Publish Notes
Before publish:
- Ensure `mix.exs` package `files` all exist (license file included).
- Ensure README badges/links/docs metadata are sane.

Publish commands:
```bash
mix hex.publish
mix hex.publish docs
```
Non-interactive (CI/headless) requires `HEX_API_KEY`.

## Test Notes
- Integration tests are env-gated; default test run should stay stable.
- Keep credentials out of committed config.

## Operational Notes
- Queue/lag telemetry exists and is important for alerting/backpressure tuning.
- Backpressure: `max_lag_bytes` disconnects CDC on sustained overload; `backpressure_lag_bytes` emits telemetry events.
- Remaining workstreams: schema evolution matrix, failure recovery playbook, integration test coverage.
