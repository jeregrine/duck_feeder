# DuckFeeder 🦆

**Stream every change from your Postgres database into a DuckDB-queryable lakehouse — with one Elixir module.**

DuckFeeder connects to Postgres logical replication (WAL/CDC), writes Parquet files to object storage (S3/GCS), and commits metadata to [DuckLake](https://ducklake.select/) — all inside your OTP supervision tree. The result is a continuously-updated analytic copy of your production data that DuckDB can query directly, with no ETL pipelines, no Kafka, and no Spark jobs.

```text
┌──────────────┐       ┌──────────────┐       ┌──────────────┐       ┌──────────────┐
│  Postgres    │  WAL  │  DuckFeeder  │ .parq │    S3/GCS    │ read  │   DuckDB     │
│  (source)    │──────▶│  (Elixir)    │──────▶│  (storage)   │◀──────│  (analytics) │
│              │       │              │──┐    │              │       │              │
└──────────────┘       └──────────────┘  │    └──────────────┘       └──────────────┘
                                         │    ┌──────────────┐
                                         └───▶│  Postgres    │
                                     metadata │  (DuckLake)  │
                                              └──────────────┘
```

### Why?

Most teams eventually need analytics on their production data. The usual path is a warehouse, an ETL tool, and a pipeline to keep them in sync. DuckFeeder collapses that into a library you add to your existing Elixir app:

- **No external services** — runs in your supervision tree alongside Phoenix/Ecto.
- **Parquet on object storage** — cheap, columnar, open format. Query from DuckDB, Snowflake, pandas, Polars, or anything else.
- **DuckLake metadata** — DuckDB's open table format with snapshot isolation, schema evolution, and column-level stats. One `ATTACH` and you're querying.
- **WAL-based CDC** — captures inserts, updates, and deletes with transactional ordering. No polling, no triggers, no dual-writes.
- **Append streams** — also push non-CDC events (telemetry, audit logs, domain events) through the same Parquet pipeline.

---

## Quick start

### 1. Migration

```elixir
defmodule MyApp.Repo.Migrations.AddDuckFeeder do
  use Ecto.Migration

  def up, do: DuckFeeder.Migrations.up(repo: repo())
  def down, do: DuckFeeder.Migrations.down(repo: repo())
end
```

### 2. Config

```elixir
# config/runtime.exs
config :my_app, MyApp.DuckFeeder,
  enabled: System.get_env("DUCK_FEEDER_ENABLED") == "true",
  repo: MyApp.Repo,
  schemas: [MyApp.Users, MyApp.Orders, MyApp.Products],
  storage: %{
    provider: :s3,
    bucket: System.fetch_env!("DUCK_FEEDER_BUCKET"),
    access_key_id: System.fetch_env!("AWS_ACCESS_KEY_ID"),
    secret_access_key: System.fetch_env!("AWS_SECRET_ACCESS_KEY")
  }
```

### 3. Runtime module

```elixir
defmodule MyApp.DuckFeeder do
  use DuckFeeder.Runtime, otp_app: :my_app
end
```

### 4. Supervise it

```elixir
# application.ex
children = [
  MyApp.Repo,
  MyAppWeb.Endpoint,
  MyApp.DuckFeeder
]
```

That's it. DuckFeeder will create the replication slot, start streaming WAL changes, write Parquet batches to S3, and commit DuckLake metadata — all automatically.

---

## Query with DuckDB

The whole point is making your data queryable. Once DuckFeeder is running, open a DuckDB session and attach the lakehouse:

```sql
-- Connect DuckDB to your DuckLake metadata
ATTACH 'ducklake:postgres:host=localhost dbname=my_app_dev' AS lake;

-- Browse what's there
SHOW ALL TABLES;

-- Query CDC data directly — DuckDB reads Parquet from S3 automatically
SELECT
    user_id,
    count(*) AS order_count,
    sum(total_cents) / 100.0 AS revenue
FROM lake.raw.orders
WHERE _df_op = 'insert'
GROUP BY user_id
ORDER BY revenue DESC
LIMIT 10;

-- Time-travel: see the state at a previous snapshot
FROM ducklake_snapshot_at(lake, 42, 'raw', 'orders')
SELECT count(*);

-- Changelog queries — reconstruct what changed and when
SELECT _df_op, _df_timestamp, id, status
FROM lake.raw.orders
WHERE id = 1234
ORDER BY _df_timestamp;
```

Every row includes CDC metadata columns (`_df_op`, `_df_timestamp`, `_df_lsn`) so you can distinguish inserts from updates from deletes, reconstruct change history, and build incremental materializations.

---

## Append streams (non-CDC events)

Push telemetry, audit logs, or domain events through the same pipeline — no CDC required:

```elixir
# Start an append stream for custom events
{:ok, stream} = DuckFeeder.start_append_stream(
  designated_tables: [%{id: 1, target_schema: "raw", target_table: "app_events"}],
  meta_conn: meta_conn,
  storage: storage_config
)

# Push events from anywhere in your app
DuckFeeder.append_event(stream, "app_events", %{
  "type" => "page_view",
  "path" => "/dashboard",
  "user_id" => user_id,
  "at" => DateTime.utc_now()
})
```

Then query them the same way:

```sql
SELECT date_trunc('hour', at) AS hour, count(*) AS views
FROM lake.raw.app_events
WHERE type = 'page_view'
GROUP BY 1
ORDER BY 1;
```

### Safe telemetry forwarding

Forward Phoenix/Ecto telemetry events without recursive ingestion loops:

```elixir
DuckFeeder.start_telemetry_forwarder(
  stream: stream,
  table: "app_events",
  events: [
    [:phoenix, :endpoint, :stop],
    [:ecto, :repo, :query]
  ],
  summarize_duck_feeder?: true
)
```

---

## How it works

```text
Postgres WAL ──▶ CDC.Connection ──▶ Service ──▶ TablePipeline(s)
                                                       │
                                               flush batches
                                                       │
                                                       ▼
                                               BatchProcessor
                                          (encode → upload → commit)
                                                       │
                                    ┌──────────────────┼──────────────────┐
                                    ▼                  ▼                  ▼
                              Write Parquet     Upload to S3/GCS   Commit DuckLake
                              (Rust NIF)                           metadata to PG
                                                                         │
                                                                         ▼
                                                              Checkpoint LSN in PG
                                                                         │
                                                                         ▼
                                                              Ack to CDC.Connection
```

**Key design choices:**

- **Rust NIF for Parquet** — fast columnar encoding without JVM overhead.
- **Postgres for DuckLake metadata** — the same database you already run. DuckDB reads this metadata catalog to locate and query the Parquet files.
- **Bounded backpressure** — configurable inflight/pending batch limits. CDC fails closed on overflow; append streams optionally shed load (`overflow_strategy: :drop_oldest`).
- **Crash-safe checkpointing** — WAL position is only acknowledged after metadata is durably committed. Restart replays from the last checkpoint.
- **Schema evolution** — new columns are auto-detected and registered in DuckLake. Rename/drop/type-change directives are supported.

---

## Schema inference

DuckFeeder infers table configuration from your Ecto schemas:

| Inferred from | Used for |
|---|---|
| `__schema__(:source)` | Source table name |
| `__schema__(:prefix)` | Source schema (default: `"public"`) |
| `__schema__(:primary_key)` | Primary key columns |

Override anything per-schema:

```elixir
schemas: [
  MyApp.Users,
  {MyApp.Orders, target_table: "order_events", target_schema: "analytics"},
  {MyApp.InternalAudit, enabled?: false}
]
```

---

## Production configuration

```elixir
config :my_app, MyApp.DuckFeeder,
  repo: MyApp.Repo,
  schemas: [MyApp.Users, MyApp.Orders],
  storage: %{
    provider: :s3,
    bucket: "my-data-lake",
    access_key_id: System.fetch_env!("AWS_ACCESS_KEY_ID"),
    secret_access_key: System.fetch_env!("AWS_SECRET_ACCESS_KEY")
  },
  # Replication slot/publication names (auto-generated if omitted)
  slot_name: "duck_feeder_prod_slot",
  publication_name: "duck_feeder_prod_pub",
  # Backpressure — effectively required for bounded WAL retention
  runtime_opts: [
    max_lag_bytes: 128 * 1024 * 1024,
    backpressure_lag_bytes: 64 * 1024 * 1024
  ]
```

### Migration ordering

If you rely on schema-change directives, deploy DuckFeeder first, then run migrations:

1. Deploy with DuckFeeder enabled.
2. Confirm replication is live.
3. Run Ecto migrations.

---

## Advanced APIs

Most apps should use the `DuckFeeder.Runtime` wrapper above. For custom orchestration, see module docs for:

- `DuckFeeder.Runtime.Supervisor` / `DuckFeeder.Runtime.StreamWorker`
- `DuckFeeder.CDC.Connection`
- `DuckFeeder.BatchProcessor`
- `DuckFeeder.Reconciler`
- `DuckFeeder.TelemetryForwarder`
- `DuckFeeder.Storage` (S3, GCS)
- `DuckFeeder.Writer` (Parquet, JSONL)

---

## Status

End-to-end architecture is implemented: **Postgres CDC → Parquet → S3/GCS → DuckLake metadata → DuckDB queries**.

See [`docs/current_status.md`](docs/current_status.md) for the detailed roadmap.

## License

See LICENSE file.
