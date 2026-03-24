# DuckFeeder plan

## Goal

DuckFeeder should be a focused tool for Elixir apps using Ecto + Postgres:

- choose Ecto schemas to mirror into DuckLake,
- take an initial snapshot,
- follow Postgres WAL,
- keep real DuckLake tables up to date,
- also support appending analytics/log/event data into the same DuckLake,
- store DuckLake metadata in the current Postgres by default, but allow configuration.

The outcome should be a low-ops warehouse/lakehouse path with DuckLake time travel and minimal load on the source Postgres.

---

## Hard rules for this branch

- **No backwards compatibility.** Nobody uses this library yet.
- **Optimize for the best end state, not incremental migration safety.**
- **Use the latest package versions.**
- **Delete old architecture aggressively when it is in the way.**
- **Do not preserve legacy data shapes just because they already exist.**

---

## Product modes

### 1. Mirror mode

Given:

- `repo: MyApp.Repo`
- `schemas: [MyApp.Users, MyApp.Orders, ...]`

DuckFeeder should:

- infer source schema/table/primary keys from Ecto schemas,
- snapshot current rows into DuckLake,
- follow WAL,
- apply inserts/updates/deletes into real DuckLake tables.

### 2. Append mode

DuckFeeder should also support writing append-only rows into the same DuckLake for:

- analytics events,
- audit logs,
- telemetry,
- other app-generated event streams.

---

## Storage model

### Mirrored business tables

Mirrored tables should be stored as normal DuckLake tables with normal columns.

That means:

- source table shape maps to target table shape,
- updates/deletes are applied as table operations,
- DuckLake snapshots provide time travel.

### Not allowed as the primary model

Do **not** use a custom persisted CDC envelope for mirrored business tables.

In particular, mirrored-table storage should **not** revolve around:

- `_record`
- `_old_record`
- nested CDC blobs as the default warehouse shape

If we need change-log or JSON-style payloads later, those are optional side outputs or append-table patterns, not the core mirrored-table design.

---

## Sink model

DuckFeeder should use DuckDB through Dux and/or direct DuckDB SQL.

Preferred order:

1. use Dux where convenient,
2. use raw DuckDB SQL whenever that is simpler or more correct,
3. do not keep hand-rolled DuckLake metadata mutation logic inside DuckFeeder.

DuckFeeder owns replication correctness.
DuckDB/Dux owns actual DuckLake writes.

---

## Runtime flow

### Mirror mode

```text
Ecto schemas
  -> infer source tables
  -> snapshot into DuckLake
  -> start/resume WAL streaming
  -> decode + route changes
  -> apply changes to real DuckLake tables
  -> persist checkpoint
  -> ack WAL
```

### Append mode

```text
app events / logs / telemetry
  -> append API
  -> sink
  -> append-oriented DuckLake tables
```

---

## Durability rule

This remains the core correctness rule:

- **WAL ACK advances only after durable downstream commit and persisted checkpoint.**

---

## Snapshot / resume / WAL behavior

The system must support the normal safe handoff:

1. capture a replication boundary,
2. snapshot selected tables,
3. replay WAL from that boundary,
4. continue streaming.

This covers row changes that happen while the snapshot is running.

On restart, DuckFeeder should:

- load the last durable checkpoint LSN,
- reconnect to the logical replication slot,
- resume from that LSN,
- replay retained WAL.

This works as long as:

- the slot still exists,
- WAL has been retained,
- the checkpoint was durably persisted.

---

## Schema changes

Schema changes must be handled conservatively and correctly.

What should work first:

- snapshot + WAL handoff while writes continue,
- restart from durable checkpoints,
- additive changes such as new columns.

Important reality:

- `pgoutput` gives row changes and relation metadata,
- it does **not** give a perfect high-level DDL event stream.

So initial policy should be:

- **auto-handle safe additive changes** when possible,
- **fail closed** on ambiguous/destructive changes until explicitly supported.

That means we should not try to silently guess our way through:

- column rename,
- column drop,
- incompatible type change,
- other destructive migration sequences.

Track relation metadata by LSN boundary and stop loudly when we cannot apply changes safely.

---

## Minimal control plane

Keep only the metadata needed for replication correctness:

- sources,
- selected/mirrored tables,
- checkpoints,
- snapshot handoff state,
- optional small commit journal if needed for idempotent recovery.

Default location:

- current Postgres.

Configurable later:

- separate Postgres for DuckLake metadata/control plane if needed.

---

## What to delete

Delete old architecture aggressively when replacing it.

Target removals include:

- custom Parquet writer,
- storage adapters / object upload flow,
- hand-written DuckLake metadata commit layer,
- nested changelog-envelope persistence model,
- compatibility code that only exists to preserve the old design.

---

## Current branch status

- `DuckFeeder.Sink.DuckDB` is now the default downstream path.
- `DuckFeeder.Service` and `DuckFeeder.AppendStream` write through the DuckDB sink.
- The old downstream stack has been deleted:
  - batch processor,
  - writer modules,
  - storage modules,
  - custom DuckLake commit modules,
  - reconciler,
  - temp file reaper,
  - old NIF/precompiled release plumbing,
  - stale generated docs and old benchmarks.
- Runtime/config now use `duckdb` config instead of storage-shaped config.
- Tests now target the DuckDB sink and the reduced runtime surface.

## Next execution steps

1. Keep rewriting docs and public API wording around DuckDB-first behavior.
2. Tighten runtime/service options so only the intended end-state surface remains.
3. Improve DuckDB setup/attach ergonomics for the final warehouse shape.
4. Add more mirror-mode coverage around schema evolution and failure behavior.
5. Continue deleting any code that still exists only because of the old architecture.

## Relevant files / read these first

These are the main files used to form this plan. Read these first in the next session instead of rediscovering the repo structure.

### DuckFeeder: current runtime pieces worth keeping or reshaping

- `AGENTS.md`
- `README.md`
- `mix.exs`
- `lib/duck_feeder.ex`
- `lib/duck_feeder/runtime.ex`
- `lib/duck_feeder/service.ex`
- `lib/duck_feeder/batch_queue.ex`
- `lib/duck_feeder/ingest.ex`
- `lib/duck_feeder/table_pipeline.ex`
- `lib/duck_feeder/cdc/connection.ex`
- `lib/duck_feeder/cdc/pipeline.ex`
- `lib/duck_feeder/cdc/router.ex`
- `lib/duck_feeder/cdc/changelog_row.ex`
- `lib/duck_feeder/cdc/initial_snapshot.ex`
- `lib/duck_feeder/cdc/initial_snapshot/runner.ex`
- `lib/duck_feeder/cdc/snapshot_boundary.ex`
- `lib/duck_feeder/cdc/transaction_buffer.ex`
- `lib/duck_feeder/meta.ex`
- `lib/duck_feeder/meta/store.ex`
- `lib/duck_feeder/meta/batch_state.ex`
- `priv/duckfeeder_meta/create_tables.sql`

### DuckFeeder: old architecture likely to delete or heavily replace

- `lib/duck_feeder/batch_processor.ex`
- `lib/duck_feeder/append_stream.ex`
- `lib/duck_feeder/reconciler.ex`
- `lib/duck_feeder/storage.ex`
- `lib/duck_feeder/storage/*`
- `lib/duck_feeder/writer.ex`
- `lib/duck_feeder/writer/*`
- `lib/duck_feeder/duck_lake/*`

### Dux: sink/runtime files to use as the new downstream path

Paths in `~/projects/dux`:

- `~/projects/dux/README.md`
- `~/projects/dux/mix.exs`
- `~/projects/dux/lib/dux.ex`
- `~/projects/dux/lib/dux/application.ex`
- `~/projects/dux/lib/dux/connection.ex`
- `~/projects/dux/lib/dux/backend.ex`
- `~/projects/dux/lib/dux/query_builder.ex`
- `~/projects/dux/lib/dux/sql.ex`
- `~/projects/dux/lib/dux/table_reader.ex`

Important Dux capabilities already present:

- `Dux.insert_into/3`
- `Dux.attach/3`
- `Dux.from_attached/3`
- `Dux.to_parquet/3`
- raw DuckDB access via `Dux.Connection.get_conn()` + `Adbc.Connection.query!/2`

### Tests that reflect the current reality

- `test/duck_feeder/runtime_test.exs`
- `test/duck_feeder/runtime_stream_integration_test.exs`
- `test/duck_feeder/meta/store_integration_test.exs`
- `test/duck_feeder/cdc/*`
- `~/projects/dux/test/dux/io_test.exs`
- `~/projects/dux/test/dux/postgres_attach_test.exs`
- `~/projects/dux/test/dux/coordinator_test.exs`

## Source of truth for this branch

This branch is optimizing for the final architecture, not a compatibility bridge.

When making implementation decisions, prefer:

- simpler system,
- real DuckLake tables,
- minimal control plane,
- standard DuckDB/DuckLake behavior,
- aggressive removal of obsolete code.
