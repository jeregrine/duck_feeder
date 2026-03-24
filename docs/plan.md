# DuckFeeder plan

## Goal

DuckFeeder should be the cleanest way for an Elixir app to mirror Postgres data into DuckDB-managed tables.

A great DuckFeeder experience should feel like:

- pick Ecto schemas,
- point DuckFeeder at a DuckDB database,
- start the runtime,
- query mirrored tables immediately,
- append app events into the same database,
- trust restart/checkpoint behavior without thinking about pipeline internals.

This branch should optimize for a beautiful developer experience, strong correctness, and clear docs/examples.

---

## Product shape

### Mirror mode

Given:

- `repo: MyApp.Repo`
- `schemas: [MyApp.Users, MyApp.Orders, ...]`
- `duckdb: %{path: "/path/to/app.duckdb"}`

DuckFeeder should:

- infer source schema/table/primary keys from Ecto schemas,
- snapshot current rows,
- follow WAL,
- apply inserts/updates/deletes into real target tables,
- persist checkpoints,
- resume safely after restart.

### Append mode

DuckFeeder should also support append-only app data in the same DuckDB database for:

- analytics events,
- audit logs,
- telemetry,
- domain event streams.

---

## Core architecture

DuckFeeder owns:

- snapshot + WAL handoff,
- CDC decoding and routing,
- batching,
- checkpoint durability,
- restart correctness.

DuckDB owns:

- real table writes,
- table schema state,
- query surface.

Core downstream path:

```text
Postgres WAL
  -> CDC pipeline
  -> table batches
  -> DuckDB sink
  -> checkpoint persisted
  -> WAL ack
```

---

## Durability rule

This remains the most important invariant:

- **WAL ACK advances only after downstream table changes are committed and the checkpoint is durably persisted.**

---

## Table model

Mirrored tables should be normal tables with normal columns.

That means:

- source table shape maps to target table shape,
- inserts become upserts,
- updates become upserts,
- deletes delete by primary key,
- truncates clear the target table.

Append tables should also be normal tables.

No special warehouse-only envelope should define the primary product experience.

---

## Schema handling

What should work well first:

- additive columns,
- snapshot + WAL handoff while writes continue,
- restart from durable checkpoints,
- failure that is loud and clear when a change cannot be applied safely.

Target behavior:

- automatically add safe new columns,
- fail closed on ambiguous/destructive changes,
- make the failure message obvious and actionable.

---

## Runtime/config shape

The runtime should be DuckDB-first and simple.

Source and table selection should come from app config / Ecto schemas, not from a separately managed persisted registry.

Expected config direction:

```elixir
config :my_app, MyApp.DuckFeeder,
  enabled: true,
  repo: MyApp.Repo,
  schemas: [MyApp.Users, MyApp.Orders],
  duckdb: %{
    path: "/var/lib/my_app/analytics.duckdb"
  }
```

DuckDB config should stay small and ergonomic:

- `path`
- `catalog` when needed
- `setup_sql`
- `setup_fun`

### Metadata should be minimal and durable

Postgres metadata should only persist the state needed for correctness and restart behavior.

Keep in Postgres:

- checkpoints
- snapshot handoffs
- migration version bookkeeping

Keep in app config / code:

- source name / slot / publication
- source-to-target table mapping
- primary keys
- other runtime table options derived from Ecto schemas/config

---

## Developer experience goals

This branch should become genuinely pleasant to use.

### Golden-path setup

It should be easy to:

- add one migration,
- define one runtime module,
- configure one DuckDB path,
- start supervision,
- query `raw.users` and `raw.orders` right away.

### Great errors

Common problems should produce clear messages:

- missing primary keys,
- invalid DuckDB config,
- unsupported schema changes,
- checkpoint/startup issues,
- snapshot handoff failures.

### Great defaults

Defaults should make local development delightful:

- simple local DuckDB file path,
- sensible target schema naming,
- minimal required configuration,
- examples that run without ceremony.

---

## Docs and examples goals

A major priority now is polished docs/examples.

We should ship a really strong DevUX story:

### Docs

- concise README with a real golden path,
- clear config reference,
- append-stream docs,
- checkpoint/restart model explained simply,
- schema evolution behavior documented clearly,
- troubleshooting section for common failures.

### Examples

We should add examples that are easy to copy:

1. **Minimal Ecto app mirror**
   - repo + schemas + runtime module + DuckDB path
2. **Append stream example**
   - application events into `raw.app_events`
3. **Local dev demo**
   - seed data, run app, query DuckDB
4. **Telemetry example**
   - forward events into append tables safely

### Nice finishing touches

- example queries people actually want,
- clear naming conventions,
- readable generated docs,
- small end-to-end snippets instead of giant walls of config.

---

## Current branch status

This branch already has the core new path in place:

- DuckDB sink is the default downstream path.
- Service and append stream write through the DuckDB sink.
- CDC batches apply as direct table operations:
  - `MERGE`
  - `DELETE`
  - clear target table on truncate
- Checkpoints persist through `DuckFeeder.Meta.upsert_checkpoint/3`.
- Runtime/config uses `duckdb` config.
- Metadata bootstrap is now trimmed to the durable runtime state only:
  - `duckfeeder_meta.checkpoints`
  - `duckfeeder_meta.snapshot_handoffs`
  - migration version bookkeeping
- Source/table registry data now lives in app config / Ecto-derived runtime config instead of persisted metadata tables.
- Legacy batch-pipeline metadata/store code has been removed from the active codebase.
- DuckDB `setup_sql` / `setup_fun` run once per connection/config instead of once per batch.
- Tests cover the new sink and reduced runtime surface.

---

## Next execution steps

### 1. Tighten the public API

Make naming and behavior consistent everywhere:

- prefer `duckdb_config` / `duckdb` terminology consistently,
- keep only the intended runtime/service options,
- improve option validation and error messages.

### 2. Polish DuckDB setup ergonomics

Make startup/setup pleasant:

- smooth path handling,
- clear support for `catalog`, `setup_sql`, `setup_fun`,
- obvious story for local dev vs app-managed deployment.

### 3. Improve mirror behavior coverage

Add more tests around:

- primary key changes,
- additive schema changes,
- restart/resume behavior,
- failure semantics when a table change is unsafe.

### 4. Build excellent docs and examples

This is the biggest product-quality step left:

- README polish,
- docs reference cleanup,
- example apps/snippets,
- troubleshooting guide,
- query examples.

### 5. Make it feel great

Push on the details that make the system shine:

- better names,
- better defaults,
- better messages,
- fewer required decisions,
- clearer onboarding.

---

## Relevant files for the next session

Read these first:

- `README.md`
- `docs/plan.md`
- `mix.exs`
- `lib/duck_feeder.ex`
- `lib/duck_feeder/runtime.ex`
- `lib/duck_feeder/runtime/embedded.ex`
- `lib/duck_feeder/runtime/supervisor.ex`
- `lib/duck_feeder/runtime/stream_worker.ex`
- `lib/duck_feeder/runtime/manager.ex`
- `lib/duck_feeder/service.ex`
- `lib/duck_feeder/append_stream.ex`
- `lib/duck_feeder/designated_table.ex`
- `lib/duck_feeder/sink.ex`
- `lib/duck_feeder/sink/duckdb.ex`
- `lib/duck_feeder/duckdb/connection.ex`
- `lib/duck_feeder/config.ex`
- `lib/duck_feeder/bootstrap.ex`
- `lib/duck_feeder/meta.ex`
- `lib/duck_feeder/meta/store.ex`
- `lib/duck_feeder/cdc/*`
- `test/duck_feeder/runtime_test.exs`
- `test/duck_feeder/service_test.exs`
- `test/duck_feeder/append_stream_test.exs`
- `test/duck_feeder/sink/duckdb_test.exs`
- `test/duck_feeder/config_test.exs`
- `test/duck_feeder/bootstrap_test.exs`

---

## Source of truth for this branch

When making decisions, prefer:

- simpler runtime shape,
- real DuckDB tables,
- strong durability semantics,
- clear docs,
- copy-pasteable examples,
- beautiful DevUX.
