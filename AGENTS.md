# AGENTS.md

## Project Overview

- DO NOT CARE ABOUT BACKAWARDS COMPATIBILITY OR LEGACY CONSIDERATIONS.
- DO NOT add dependencies without justification. Every dep is a liability. Check if Erlang/OTP already provides it as a primitive.
- DO NOT hedge for unexpected input types. Define typespecs properly, and let Elixir’s type system and Dialzyer do the job.
- DO NOT use unnecessary “escape hatch” message types or catch-alls. Design the system’s interfaces properly.
- DO NOT add fallback paths in large refactors. Make the changes in one go.
- DO NOT use unnecessary mocks or default values.
- DO NOT mark some tests “skipped” if they require third-party dependencies.
- DO use real processes with isolated state (Briefly temporary directories, test-scoped ETS table-backed servers, components that are provided by the framework itself).
- DO declare test dependencies, not inherit them.

DuckFeeder is an Elixir runtime for mirroring Postgres data into DuckDB-managed tables:

- **Postgres logical replication (WAL/CDC)** — decode pgoutput, route changes, apply as table operations
- **Direct DuckDB writes** via Dux/DuckDB — inserts, merges, deletes, and truncate-clears into real target tables
- **Append-only event streams** — non-CDC producers (telemetry, logs, domain events) into the same DuckDB database
- **Checkpoint durability** — Postgres-backed metadata store for restart correctness

DuckFeeder owns replication correctness, batching, snapshot/WAL handoff, and checkpoint discipline.
DuckDB owns table storage and query surface.

Core durability rule: **WAL ACK advances only after DuckDB table writes are committed and the checkpoint is durably persisted.**

## Current Runtime/Product Decisions

- Elixir-first architecture, OTP-supervised runtime.
- Source/table registry is **config-first**: source settings and designated tables come from app config / Ecto schemas, not persisted metadata tables.
- DuckDB access is through **Dux** (`{:dux, "~> 0.2"}`), which uses ADBC under the hood.
- CDC path is fail-closed under sustained overload.
- Append stream defaults to fail-closed but can optionally use lossy overflow policy (`overflow_strategy: :drop_oldest`).
- Telemetry helper shipped: `DuckFeeder.TelemetryForwarder`.
- Single-writer model for DuckDB (coordinated via Elixir supervision, similar to how `fly_postgres` works with Ecto).

## Dependencies

Runtime deps (from `mix.exs`):

- `postgrex ~> 0.20` — Postgres connections + replication protocol
- `dux ~> 0.2` — DuckDB access layer over DuckDB/ADBC
- `nimble_options ~> 1.1` — config validation

Dev/test only:

- `ecto_sql ~> 3.12` (test) — migration integration tests
- `benchee ~> 1.3` (dev)
- `ex_doc ~> 0.38` (dev)

Note: JSON encoding uses Elixir 1.19+'s built-in `JSON` module. The `jason` optional dependency has been removed.

## Data Flow

### CDC path

```
Postgres WAL
  → CDC.Connection (Postgrex.ReplicationConnection)
  → CDC.Pipeline (TransactionBuffer → Ingest)
  → TablePipeline (micro-batch buffer)
  → Sink.DuckDB (dedup check → MERGE/DELETE/INSERT via Dux/DuckDB → record applied batch)
  → Meta.Store.upsert_checkpoint (Postgres)
  → CDC.Connection ack_lsn
```

### Append path

```
AppendStream.append/4
  → TablePipeline (micro-batch buffer)
  → Sink.DuckDB (dedup check → INSERT via Dux/DuckDB → record applied batch)
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
- Move DuckDB access back behind `dux ~> 0.2` before expanding the broader integration matrix.
- Prefer real DuckDB tables over warehouse-specific envelopes.
- Preserve loud failure semantics over silent fallback behavior.

## Known Technical Debt

Current notable items:

- `Sink.DuckDB` builds SQL via string interpolation with `validate_sql_type/1` allowlisting and `escape_sql_string/1` hardening; continue auditing validation/escaping paths.
- DuckLake-backed end-to-end integration coverage is still too thin, especially for local filesystem-backed setups.
- Append-stream restart semantics still rely on caller-provided synthetic LSN continuity.
- `StreamSupport.maybe_start_duckdb_connection/2` starts a `Dux.Connection` process that is linked to the caller but not explicitly supervised; it will leak if the caller traps exits.
- `snapshot_handoff_source_key/2` in `Runtime` has dead code branches for `source.id` (string/integer) that are never hit in the config-first model.

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
