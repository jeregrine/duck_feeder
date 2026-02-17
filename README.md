# DuckFeeder

Elixir-first Postgres CDC -> Parquet -> object storage -> DuckLake metadata.

## What this is

DuckFeeder runs in your OTP supervision tree and keeps an append-only analytic trail
of Postgres table changes.

```text
Postgres WAL
   |
   v
DuckFeeder.CDC.Connection
   |
   v
DuckFeeder.Service
   |
   +--> CDC.Pipeline -> Ingest -> TablePipeline(s)
   |                                  |
   |                                  v
   |                      {:duck_feeder_batch, table, batch}
   |__________________________________|
                  |
                  v
        DuckFeeder.BatchProcessor
        (write -> upload -> commit)
                  |
                  v
   checkpoint_lsn persisted in Postgres metadata
                  |
                  v
   Service -> {:duck_feeder_ack_lsn, checkpoint_lsn} -> CDC.Connection
```

Append stream path (non-CDC app events):

```text
producer rows -> DuckFeeder.AppendStream -> TablePipeline(s) -> BatchProcessor
```

---

## Phoenix/Ecto quick start (smart defaults)

### 1) Add DuckFeeder metadata migrations

```elixir
defmodule AcmeApp.Repo.Migrations.AddDuckFeeder do
  use Ecto.Migration

  def up, do: DuckFeeder.Migrations.up(repo: repo())
  def down, do: DuckFeeder.Migrations.down(repo: repo())
end
```

### 2) Configure DuckFeeder from Repo + Schemas

```elixir
# config/runtime.exs
config :acme_app, AcmeApp.DuckFeeder,
  enabled: System.get_env("DUCK_FEEDER_ENABLED") == "true",
  repo: AcmeApp.Repo,
  # optional; defaults to :repo
  metadata_repo: AcmeApp.Repo,
  schemas: [
    AcmeApp.Tenants,
    AcmeApp.Users,
    {AcmeApp.Invoices, target_table: "invoice_events"}
  ],
  storage: %{
    provider: :s3,
    bucket: System.fetch_env!("DUCK_FEEDER_BUCKET"),
    access_key_id: System.fetch_env!("AWS_ACCESS_KEY_ID"),
    secret_access_key: System.fetch_env!("AWS_SECRET_ACCESS_KEY")
  },
  runtime_opts: [
    max_lag_bytes: 128 * 1024 * 1024,
    backpressure_lag_bytes: 64 * 1024 * 1024
  ]
```

Schema inference defaults:
- source table/schema from `__schema__(:source)` and `__schema__(:prefix)`
- primary keys from `__schema__(:primary_key)`
- target schema defaults to `"raw"`
- entry format can be `MySchema` or `{MySchema, opts}`

### 3) Add a runtime module

```elixir
defmodule AcmeApp.DuckFeeder do
  use DuckFeeder.Runtime, otp_app: :acme_app
end
```

### 4) Supervise it

```elixir
children = [
  AcmeApp.Repo,
  AcmeAppWeb.Endpoint,
  AcmeApp.DuckFeeder
]
```

That is enough for normal CRUD apps.

---

## Is metadata bootstrap required?

In smart-default mode (`use DuckFeeder.Runtime`), bootstrap/seeding is handled automatically at startup.

Manual `DuckFeeder.seed_meta/3` is an advanced/manual flow (custom orchestration, scripts, tests), not a required step for normal Phoenix setup.

---

## Append stream for app events (telemetry/logs/errors)

Use append stream when you want to write app events that are not row-level CDC.

```elixir
{:ok, stream} =
  DuckFeeder.start_append_stream(
    designated_tables: [%{id: 1, target_schema: "raw", target_table: "app_events"}],
    meta_conn: meta_conn,
    storage: storage_config,
    writer: %{format: :parquet},
    pipeline_opts: %{max_rows: 5_000, max_bytes: 64 * 1_024 * 1_024, flush_interval_ms: 2_000}
  )

:ok =
  DuckFeeder.append_event(stream, "app_events", %{
    "type" => "telemetry",
    "name" => "user_login",
    "tenant_id" => tenant_id,
    "at" => DateTime.utc_now()
  })
```

Queue controls:
- `max_inflight_batches` (default `1`)
- `max_pending_batches` (default `1000`)
- `overflow_strategy: :fail | :drop_oldest` (default `:fail`)

Use `:drop_oldest` only for availability-first/loss-tolerant streams.

---

## Avoid recursive telemetry ingestion

If you forward Telemetry events into DuckFeeder append stream, do **not** forward DuckFeeder's
own telemetry events back into the same stream.

```elixir
:telemetry.attach_many(
  "acme-app-events",
  [
    [:phoenix, :endpoint, :stop],
    [:ecto, :repo, :query]
  ],
  fn event, measurements, metadata, %{stream: stream} ->
    # guard against recursive/self-ingest
    unless match?([:duck_feeder | _], event) do
      DuckFeeder.append_event(stream, "app_events", %{
        "type" => "telemetry",
        "event" => Enum.join(Enum.map(event, &to_string/1), "."),
        "measurements" => measurements,
        "metadata" => metadata,
        "at" => DateTime.utc_now()
      })
    end
  end,
  %{stream: stream}
)
```

---

## Migration ordering contract (important)

If you rely on schema-change semantics (`committer_opts[:schema_changes]`), start DuckFeeder first,
then run source DB migrations.

Recommended rollout order:
1. Deploy app with DuckFeeder runtime enabled (`AcmeApp.DuckFeeder` supervised).
2. Confirm replication is live (or start explicitly via `DuckFeeder.start_stream/4` in advanced setups).
3. Run Ecto migrations.
4. Emit matching `schema_changes` directives in the same rollout window.

---

## Advanced/low-level APIs

Most apps should stay with the smart-default runtime wrapper above.

For advanced flows, see module docs for:
- `DuckFeeder.Runtime` / `DuckFeeder.Runtime.Supervisor` / `DuckFeeder.Runtime.StreamWorker`
- `DuckFeeder.CDC.Connection`
- `DuckFeeder.BatchProcessor`
- `DuckFeeder.Reconciler`
- `DuckFeeder.Storage`
- `DuckFeeder.Writer`

And status/roadmap:
- `docs/current_status.md`
