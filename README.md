# DuckFeeder

[![Hex.pm](https://img.shields.io/hexpm/v/duck_feeder.svg)](https://hex.pm/packages/duck_feeder)
[![Hex Docs](https://img.shields.io/badge/hex-docs-blue.svg)](https://hexdocs.pm/duck_feeder)

DuckFeeder streams Postgres WAL/CDC into real DuckDB-managed tables from inside your Elixir app.

Current direction:
- snapshot selected tables,
- follow logical replication,
- apply inserts/updates/deletes into target tables,
- persist checkpoints in Postgres,
- support append streams for app events.

The old Parquet/object-storage/custom DuckLake pipeline has been removed from the runtime path.

## Status

Experimental, but the core shape is now:

```text
Postgres WAL
  -> DuckFeeder CDC pipeline
  -> DuckDB sink
  -> checkpoint in Postgres
  -> WAL ack
```

Durability rule:

- WAL ACK only advances after downstream table changes are committed and the checkpoint is persisted.

## Quick start

### 1. Add the metadata tables

```elixir
defmodule MyApp.Repo.Migrations.AddDuckFeeder do
  use Ecto.Migration

  def up, do: DuckFeeder.Migrations.up(repo: repo())
  def down, do: DuckFeeder.Migrations.down(repo: repo())
end
```

### 2. Configure a runtime module

```elixir
# config/runtime.exs
config :my_app, MyApp.DuckFeeder,
  enabled: System.get_env("DUCK_FEEDER_ENABLED") == "true",
  repo: MyApp.Repo,
  schemas: [MyApp.Users, MyApp.Orders, MyApp.Products],
  duckdb: %{
    path: System.get_env("DUCK_FEEDER_DUCKDB_PATH") || "/var/lib/my_app/analytics.duckdb"
  }
```

```elixir
defmodule MyApp.DuckFeeder do
  use DuckFeeder.Runtime, otp_app: :my_app
end
```

### 3. Supervise it

```elixir
children = [
  MyApp.Repo,
  MyAppWeb.Endpoint,
  MyApp.DuckFeeder
]
```

That starts the managed runtime wrapper:
- resolves tables from your Ecto schemas,
- seeds metadata,
- starts snapshot/WAL streaming,
- writes into DuckDB tables.

## Query the data

If your DuckDB path is `/var/lib/my_app/analytics.duckdb`:

```bash
duckdb /var/lib/my_app/analytics.duckdb
```

Then query your mirrored tables directly:

```sql
SELECT * FROM raw.users LIMIT 10;

SELECT user_id, count(*) AS order_count
FROM raw.orders
GROUP BY user_id
ORDER BY order_count DESC
LIMIT 20;
```

## Append streams

DuckFeeder also supports append-only streams for telemetry, audit logs, or app events.

```elixir
{:ok, stream} =
  DuckFeeder.start_append_stream(
    designated_tables: [
      %{id: 1, target_schema: "raw", target_table: "app_events"}
    ],
    meta_conn: meta_conn,
    duckdb: %{path: "/var/lib/my_app/analytics.duckdb"}
  )

:ok =
  DuckFeeder.append_event(stream, "app_events", %{
    "type" => "page_view",
    "path" => "/dashboard",
    "user_id" => 123,
    "at" => DateTime.utc_now()
  })
```

## Runtime config shape

Validated config now looks like:

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
    flush_interval_ms: 5_000
  }
}
```

Useful DuckDB options:
- `path` — database file path; omit for in-memory
- `catalog` — optional catalog prefix for fully-qualified relations
- `setup_sql` — SQL statements to run before writes
- `setup_fun` — custom one-arg setup callback receiving the ADBC connection

## Mirror semantics

DuckFeeder currently applies CDC batches as direct table operations:
- inserts and replica rows -> upsert
- updates -> upsert, plus delete old PK row when the primary key changed
- deletes -> delete by primary key
- truncates -> clear the target table

Primary keys are required for correct update/delete semantics.

Additive schema evolution is handled by adding missing columns on the target table.
Ambiguous destructive changes should fail closed rather than be guessed.

## Public API highlights

Main entrypoints:
- `DuckFeeder.start_stream/4`
- `DuckFeeder.start_service/4`
- `DuckFeeder.start_append_stream/1`
- `DuckFeeder.append_event/4`
- `DuckFeeder.seed_meta/3`
- `DuckFeeder.seed_and_start_stream/3`

## Development notes

This branch is intentionally optimized for the new end state, not backward compatibility.

If you are looking for the old Parquet/object-storage/DuckLake implementation, it has been removed from the active runtime path and much of it has been deleted outright.

See `docs/plan.md` for the branch direction.
