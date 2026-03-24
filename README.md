# DuckFeeder

[![Hex.pm](https://img.shields.io/hexpm/v/duck_feeder.svg)](https://hex.pm/packages/duck_feeder)
[![Hex Docs](https://img.shields.io/badge/hex-docs-blue.svg)](https://hexdocs.pm/duck_feeder)

DuckFeeder mirrors Postgres tables into real DuckDB-managed tables from inside your Elixir app.

It also supports append-only streams for app events, telemetry, audit logs, and similar analytics data in the same DuckDB database.

## Core idea

DuckFeeder owns replication correctness:

- initial snapshot + WAL handoff
- CDC decoding and routing
- batching
- checkpoint durability
- restart correctness

DuckDB owns the actual target tables and query surface.

Core flow:

```text
Postgres WAL
  -> DuckFeeder CDC pipeline
  -> DuckDB sink
  -> checkpoint persisted in Postgres
  -> WAL ack
```

Most important invariant:

- **WAL ACK only advances after DuckDB table writes are committed and the checkpoint is durably persisted.**

Mirrored tables are normal DuckDB tables. Append tables are normal DuckDB tables too.

The old custom Parquet/object-storage/DuckLake runtime path has been removed. DuckFeeder should rely on DuckDB/DuckLake directly rather than carrying its own warehouse layer.

## Quick start

### 1. Add the dependency

```elixir
defp deps do
  [
    {:duck_feeder, "~> 0.1"}
  ]
end
```

### 2. Add the metadata migration

DuckFeeder stores only minimal durable runtime state in Postgres.

```elixir
defmodule MyApp.Repo.Migrations.AddDuckFeeder do
  use Ecto.Migration

  def up, do: DuckFeeder.Migrations.up(repo: repo())
  def down, do: DuckFeeder.Migrations.down(repo: repo())
end
```

### 3. Configure a runtime module

Recommended app-facing setup:

```elixir
# config/runtime.exs
config :my_app, MyApp.DuckFeeder,
  enabled: System.get_env("DUCK_FEEDER_ENABLED") == "true",
  repo: MyApp.Repo,
  schemas: [
    MyApp.Users,
    MyApp.Orders,
    {MyApp.Invoices, target_schema: "raw", target_table: "invoice_events"}
  ],
  duckdb: %{
    path: System.get_env("DUCK_FEEDER_DUCKDB_PATH") || "/var/lib/my_app/analytics.duckdb"
  }
```

```elixir
defmodule MyApp.DuckFeeder do
  use DuckFeeder.Runtime, otp_app: :my_app
end
```

### 4. Supervise it

```elixir
children = [
  MyApp.Repo,
  MyAppWeb.Endpoint,
  MyApp.DuckFeeder
]
```

### 5. Start querying DuckDB

If your DuckDB path is `/var/lib/my_app/analytics.duckdb`:

```bash
duckdb /var/lib/my_app/analytics.duckdb
```

Then query the mirrored tables directly:

```sql
SELECT * FROM raw.users LIMIT 10;

SELECT user_id, count(*) AS order_count
FROM raw.orders
GROUP BY user_id
ORDER BY order_count DESC
LIMIT 20;
```

## What the runtime wrapper does

When you use `DuckFeeder.Runtime` with `repo`, `schemas`, and `duckdb` config, DuckFeeder will:

- infer source schema/table names from your Ecto schemas
- infer primary keys from your Ecto schemas
- default target tables to `raw.<source_table>`
- create the minimal Postgres metadata schema
- run an initial snapshot before streaming on first start
- resume from persisted checkpoints on restart
- resume an incomplete snapshot handoff safely by default
- start logical replication and keep applying changes into DuckDB

The wrapper defaults are optimized for the intended golden path.

If you use the lower-level APIs directly, snapshot behavior remains explicit through runtime options like `snapshot_before_stream?: true`.

## Mirror semantics

CDC batches are applied as direct table operations:

- inserts and replica rows -> upsert
- updates -> upsert
- deletes -> delete by primary key
- truncates -> clear the target table

Primary keys are required for correct update/delete behavior.

Additive schema changes are handled by adding missing columns on the DuckDB side.
Ambiguous or destructive changes fail closed instead of being guessed.

## What lives in app config vs Postgres metadata

DuckFeeder is config-first.

Keep in app config / code:

- source selection
- Ecto schemas
- source-to-target table mapping
- primary keys
- slot/publication naming
- DuckDB path and setup hooks

Keep in Postgres metadata:

- checkpoints
- snapshot handoffs
- migration version bookkeeping

That metadata lives under `duckfeeder_meta`.

Useful inspection queries:

```sql
SELECT *
FROM duckfeeder_meta.checkpoints
ORDER BY checkpoint_key;

SELECT *
FROM duckfeeder_meta.snapshot_handoffs;
```

## Append streams

DuckFeeder can also write append-only events into the same DuckDB database.

```elixir
{:ok, stream} =
  DuckFeeder.start_append_stream(
    designated_tables: [
      %{target_schema: "raw", target_table: "app_events"}
    ],
    meta_conn: meta_conn,
    duckdb: %{path: "/var/lib/my_app/analytics.duckdb"},
    object_prefix: "my_app_events"
  )

:ok =
  DuckFeeder.append_event(stream, "app_events", %{
    "type" => "page_view",
    "path" => "/dashboard",
    "user_id" => 123,
    "at" => DateTime.utc_now()
  })
```

Append streams reuse the same batching and sink path.

Overflow behavior is configurable:

- `overflow_strategy: :fail` - fail closed
- `overflow_strategy: :drop_oldest` - lossy mode for availability-first streams

See [docs/append-streams.md](docs/append-streams.md) for restart/checkpoint guidance and telemetry forwarding.

## Explicit runtime config

If you do not want the repo/schema wrapper, DuckFeeder also supports the explicit engine-shaped config:

```elixir
%{
  source: %{
    postgres_url: "postgres://...",
    slot_name: "duck_feeder_default_slot",
    publication_name: "duck_feeder_default_pub",
    designated_tables: [
      %{
        source_schema: "public",
        source_table: "users",
        target_schema: "raw",
        target_table: "users",
        primary_keys: ["id"]
      }
    ]
  },
  duckdb: %{
    path: "/var/lib/my_app/analytics.duckdb",
    catalog: nil,
    setup_sql: [],
    setup_fun: nil
  },
  metadata: %{
    postgres_url: "postgres://..."
  },
  ingest: %{
    max_rows: 10_000,
    max_bytes: 134_217_728,
    flush_interval_ms: 5_000,
    table_worker_concurrency: 4
  }
}
```

Useful DuckDB options:

- `path` - database file path; omit for in-memory
- `catalog` - optional catalog prefix
- `setup_sql` - SQL statements to run before writes
- `setup_fun` - one-arg callback receiving the DuckDB connection

## Public API highlights

Main entrypoints:

- `DuckFeeder.validate_config/1`
- `DuckFeeder.seed_meta/3`
- `DuckFeeder.seed_and_start_stream/3`
- `DuckFeeder.start_stream/4`
- `DuckFeeder.start_service/4`
- `DuckFeeder.start_append_stream/1`
- `DuckFeeder.append_event/4`
- `DuckFeeder.start_telemetry_forwarder/1`

## More docs

- [docs/runtime.md](docs/runtime.md) - config-first runtime setup and metadata model
- [docs/append-streams.md](docs/append-streams.md) - append semantics, restart guidance, telemetry
- [docs/troubleshooting.md](docs/troubleshooting.md) - common failures and what to check
- [docs/plan.md](docs/plan.md) - branch direction and product plan
