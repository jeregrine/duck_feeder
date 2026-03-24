# Append streams

Append streams let you write non-CDC application data into the same DuckDB database as your mirrored Postgres tables.

Good fits:

- analytics events
- audit logs
- telemetry
- domain event streams
- app-side derived facts

## Basic usage

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
    "path" => "/billing",
    "user_id" => 123,
    "at" => DateTime.utc_now()
  })
```

Append batches reuse the same downstream sink path as CDC batches:

```text
append/4
  -> table pipeline
  -> DuckDB write
  -> checkpoint persisted in Postgres
```

## Guarantees

For append streams, DuckFeeder still persists checkpoints only after the DuckDB write succeeds.

There is no WAL ACK step here, but the same durable ordering idea applies:

- write rows into DuckDB
- persist checkpoint
- report success

## Synthetic LSNs and restart continuity

Append streams generate synthetic LSNs when you do not provide one explicitly.

By default, a fresh stream starts at `0/0` and increments from there.

If you want checkpoint continuity across restarts, read the last checkpoint first and pass it back as `start_lsn`.

Using a fixed `object_prefix` makes the checkpoint key predictable:

```elixir
object_prefix = "my_app_events"
checkpoint_key = "#{object_prefix}:raw.app_events"

{:ok, start_lsn} = DuckFeeder.Meta.fetch_checkpoint(meta_conn, checkpoint_key)

{:ok, stream} =
  DuckFeeder.start_append_stream(
    designated_tables: [
      %{target_schema: "raw", target_table: "app_events"}
    ],
    meta_conn: meta_conn,
    duckdb: %{path: "/var/lib/my_app/analytics.duckdb"},
    object_prefix: object_prefix,
    start_lsn: start_lsn
  )
```

If you already have your own producer offsets or sequence numbers, you can also pass explicit LSNs per append:

```elixir
DuckFeeder.append_event(stream, "app_events", row, lsn: "0/101")
```

## Overflow behavior

Append streams support two queue policies:

### `overflow_strategy: :fail`

Default.

If the bounded batch queue stays full, the stream exits.

Use this when you want append ingestion to fail closed.

### `overflow_strategy: :drop_oldest`

Lossy mode.

If the queue is full, DuckFeeder drops the oldest pending batch and keeps the stream alive.

Use this for availability-first telemetry or event pipelines where dropping old data is acceptable.

## Tuning knobs

Useful options:

- `pipeline_opts: %{max_rows: ..., max_bytes: ..., flush_interval_ms: ...}`
- `max_inflight_batches`
- `max_pending_batches`
- `overflow_strategy`
- `default_target_schema`
- `object_prefix`

## Target table selection

Append streams write only to the target tables you declare up front.

You can append using either:

- a plain table name like `"app_events"` which resolves against `default_target_schema`
- an explicit `{schema, table}` tuple

Trying to append to an undeclared target table returns an error.

## Telemetry forwarding

DuckFeeder ships a helper for forwarding Telemetry events into an append stream:

```elixir
{:ok, forwarder} =
  DuckFeeder.start_telemetry_forwarder(
    stream: stream,
    table: "app_events",
    events: [
      [:my_app, :billing, :settled],
      [:my_app, :auth, :login]
    ]
  )
```

Default behavior is split-path:

- non-`[:duck_feeder, ...]` events are appended as raw rows
- DuckFeeder's own telemetry is summarized before append to avoid noisy self-recursive event spam

You can flush pending summaries manually:

```elixir
:ok = DuckFeeder.flush_telemetry_forwarder(forwarder)
```

## When to use append streams vs mirrored tables

Use mirrored tables when the source of truth is Postgres row state and you want CDC semantics.

Use append streams when the source of truth is an application event stream and you want an append-only analytics table.
