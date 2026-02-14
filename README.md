# DuckFeeder

Elixir-first CDC ingest library (Postgres WAL -> Parquet -> object storage -> DuckLake metadata).

## Project goal

Production-focused Elixir runtime that:
- runs inside your OTP supervision tree,
- streams Postgres CDC/WAL,
- batches writes to Parquet,
- uploads to object storage,
- and commits metadata into a DuckLake-compatible Postgres catalog.

Current implementation keeps HTTP/storage interactions Req-only and keeps Elixir/Rust dependencies minimal.
`DuckFeeder.Config` validates runtime config (source/storage/metadata/ingest) with NimbleOptions.

## Metadata bootstrap from config

You can seed `duckfeeder_meta` source + designated table rows from runtime config:

```elixir
{:ok, validated} = DuckFeeder.validate_config(runtime_config)
{:ok, %{source_id: _id, designated_table_ids: _ids}} =
  DuckFeeder.seed_meta(meta_conn, validated, source_name: "primary")

# Optional: select exactly which tables to sync from Elixir opts
# - "users" keeps default users->users mapping
# - {"orders_iceberg", "orders"} remaps source orders -> target orders_iceberg
{:ok, _} =
  DuckFeeder.seed_meta(meta_conn, validated,
    source_name: "primary",
    tables: [
      "users",
      {"orders_iceberg", "orders"}
    ]
  )

# Convenience: seed metadata and immediately start stream runtime
{:ok, %{runtime: %{service_pid: _service, cdc_pid: _cdc}}} =
  DuckFeeder.seed_and_start_stream(meta_conn, validated,
    seed_opts: [source_name: "primary"],
    start_opts: [bootstrap_replication?: false]
  )
```

## Ecto migration integration

DuckFeeder provides migration helpers intended to be wrapped by your Ecto migrations:

```elixir
defmodule MyApp.Repo.Migrations.AddDuckFeeder do
  use Ecto.Migration

  def up, do: DuckFeeder.Migrations.up(repo: repo())
  def down, do: DuckFeeder.Migrations.down(repo: repo())
end
```

You can also check the applied DuckFeeder migration version:

```elixir
DuckFeeder.Migrations.migrated_version(repo: MyApp.Repo)
```

## Writer API

`DuckFeeder.Writer` supports both JSONL and Parquet output.
`DuckFeeder.Writer.ParquetNif` now writes Parquet via a Rustler NIF.

You can select by format (`:jsonl | :parquet`) or explicit adapter module.
Optional fallback is supported (e.g. `format: :parquet, fallback_format: :jsonl`).
For parquet, `datetime_encoding: :unix_microseconds` is available to keep timestamp
normalization in Elixir and avoid extra Rust parsing dependencies.

```elixir
{:ok, write_result} = DuckFeeder.write_batch(%{}, %{rows: [%{"id" => 1}]})
:ok = DuckFeeder.cleanup_written_batch(%{}, write_result)
```

## Batch processing API

`DuckFeeder.BatchProcessor` connects flushed batches to write/upload/meta commit steps.
It supports pluggable committers via `DuckFeeder.DuckLake.Committer` (default no-op committer).
`DuckFeeder.DuckLake.Committer.Postgres` is available as a transactional scaffold for
running DuckLake SQL statements + checkpoint commit in one transaction.
By default it writes spec-aligned rows into DuckLake metadata tables, including
`ducklake_snapshot`, `ducklake_table`, `ducklake_column`, `ducklake_column_mapping`,
`ducklake_name_mapping`, `ducklake_data_file`, `ducklake_table_stats`,
`ducklake_snapshot_changes`, and `ducklake_schema_versions`, plus DuckFeeder control-plane
history/audit rows in `duckfeeder_meta.schema_history` and `duckfeeder_meta.ducklake_commits`.
It also supports optional delete-file/compaction transitions via `committer_opts`:
`:delete_files`, `:delete_files_fun`, `:replace_data_file_ids`, and
`:validate_delete_files?` (for storage `head_object` verification).
Replacement flows also mark retired files in
`ducklake_files_scheduled_for_deletion`, and snapshot summaries include
`created_table`/`altered_table` conflict hints.
Optional `:schema_changes` directives are supported for metadata evolution
(`rename_table`, `rename_column`, `drop_column`, `alter_column_type`), plus
nested-field style aliases with dotted paths (`rename_field`, `drop_field`, `alter_field_type`).
Optional partition metadata directives are also supported via
`:partition_by` and `:partition_values`.
(override via `committer_opts[:ducklake_sql]`).
Runtime/service startup accepts `committer_module` and `committer_opts` passthrough.

```elixir
{:ok, result} = DuckFeeder.process_batch(context, {"raw", "users"}, batch)
```

## Append event stream pipeline (non-CDC producers)

`DuckFeeder.AppendStream` reuses the same batching/writer/upload/commit flow for
non-CDC event producers (e.g. `:telemetry`, logs, error streams), keyed by target
table name.

```elixir
{:ok, stream} =
  DuckFeeder.start_append_stream(
    designated_tables: [%{id: 1, target_schema: "raw", target_table: "events"}],
    meta_conn: meta_conn,
    storage: storage_config,
    writer: %{format: :parquet},
    pipeline_opts: %{max_rows: 5_000, max_bytes: 64 * 1_024 * 1_024, flush_interval_ms: 2_000}
  )

:ok = DuckFeeder.append_event(stream, "events", %{"kind" => "telemetry", "value" => 1})
{:ok, _batch} = DuckFeeder.flush_append_table(stream, "events")
```

## Runtime service wiring

`DuckFeeder.Runtime` builds and starts `DuckFeeder.Service` using metadata rows.

```elixir
{:ok, service_opts} = DuckFeeder.service_options(meta_conn, "source-a", storage_config)
{:ok, service_pid} = DuckFeeder.start_service(meta_conn, "source-a", storage_config)

# Start both service + replication stream client
# (publication/slot bootstrap is enabled by default)
{:ok, %{service_pid: _service, cdc_pid: _cdc}} =
  DuckFeeder.start_stream(meta_conn, "source-a", storage_config)

# Managed worker variant (monitors service + cdc pids)
{:ok, worker} =
  DuckFeeder.start_stream_worker(
    meta_conn: meta_conn,
    source_name: "source-a",
    storage_config: storage_config
  )

{:ok, _info} = DuckFeeder.stream_worker_info(worker)

# Optional higher-level supervisor wrapper
{:ok, runtime_sup} =
  DuckFeeder.start_runtime_supervisor(
    meta_conn: meta_conn,
    source_name: "source-a",
    storage_config: storage_config,
    start_reconciler?: true
  )

# Dynamic multi-source manager
{:ok, mgr} =
  DuckFeeder.start_runtime_manager(
    meta_conn: meta_conn,
    storage_config: storage_config
  )

{:ok, _pid} = DuckFeeder.start_source_runtime(mgr, "source-a")
%{"source-a" => _pid} = DuckFeeder.list_source_runtimes(mgr)
:ok = DuckFeeder.stop_source_runtime(mgr, "source-a")
```

Optional snapshot-before-stream mode is available via:
- `snapshot_before_stream?: true`
- `snapshot_row_handler: fn designated_table, row -> ... end`
- `snapshot_on_restart?: true | false` (default `false`; cold-start snapshot by default)
- `resume_incomplete_snapshot?: true | false` (default `false`; fail closed when durable handoff marker is pending)
- `snapshot_handoff_mark_retries: non_neg_integer()` (default `2`)
- `snapshot_handoff_mark_retry_delay_ms: non_neg_integer()` (default `0`)

If no explicit `snapshot_row_handler` is provided, snapshot rows are replayed into the
service ingest path by default (`snapshot_ingest?: true`). Set `snapshot_ingest?: false`
to require an explicit row handler.

Snapshot replay uses boundary-based synthetic LSN allocation so checkpoint advancement
tracks the snapshot/WAL handoff boundary more closely.

When snapshot replay is active, runtime persists a durable handoff marker
(`duckfeeder_meta.snapshot_handoffs`) and only marks it complete after CDC start succeeds.
If startup is interrupted mid-handoff, next start fails closed unless
`resume_incomplete_snapshot?: true` is provided.
When resuming, if checkpoint LSN is already at/after the handoff boundary,
runtime can finalize the pending handoff without rerunning snapshot rows.
If checkpoint progress falls within the synthetic snapshot LSN window, runtime skips
already-replayed snapshot rows and continues from the remaining suffix.

### Migration ordering contract (important)

For schema-evolution semantics (`schema_changes`, snapshot conflict markers, and WAL handoff),
start DuckFeeder replication/runtime **before** running source DB migrations.

Recommended rollout order:
1. Start DuckFeeder (`start_stream/4` / runtime supervisor/manager) and confirm replication is live.
2. Apply DB migrations.
3. Ensure matching `committer_opts[:schema_changes]` directives are emitted during the same rollout window.

This avoids losing schema-intent context around rename/drop/type-change operations that may not be
recoverable from row-shape inference alone after the fact.

Replication connection tuning options include:
- `auto_reconnect: true | false`
- `reconnect_backoff: milliseconds` (defaults to `1000` when unset)
- `max_lag_bytes: non_neg_integer()` (disconnect guard for unacked lag growth)
- `event_sink_mode: :pid | :call` (default `:pid`)

## Replication connection API

`DuckFeeder.CDC.Connection` provides a live `Postgrex.ReplicationConnection` client
that decodes pgoutput and emits normalized `DuckFeeder.CDC.Event` values to an `event_sink`.

## Reconciliation

`DuckFeeder.Reconciler` provides stale-batch reconciliation helpers.
It currently retries stale `uploaded` batches via `commit_uploaded_batch/2`.

`DuckFeeder.Reconciler.Worker` runs reconciliation on an interval.
`DuckFeeder.reconcile/2` supports:
- `cleanup_failed_uploads?: true` to delete known failed batch files and transition
  failed batches back to `pending`
- `require_failed_batch_files?: true` to error when a failed batch has no recorded files
  during cleanup (orphan-safety guard)
- `verify_uploaded_objects?: true` to HEAD-check uploaded files before committing stale
  uploaded batches
- `max_batches: positive_integer()` to cap per-run work
- `stop_on_error?: true` to halt run after first reconciliation error

```elixir
{:ok, worker} = DuckFeeder.start_reconciler(context: %{meta_conn: meta_conn})
{:ok, summary} = DuckFeeder.run_reconcile_once(worker)
```

## Storage API

```elixir
{:ok, config} = DuckFeeder.validate_config(runtime_config)
storage_config = DuckFeeder.Config.storage_config(config)

DuckFeeder.put_file(storage_config, "/tmp/batch.parquet", "events/table=users/part-0001.parquet")
DuckFeeder.head_object(storage_config, "events/table=users/part-0001.parquet")
DuckFeeder.delete_object(storage_config, "events/table=users/part-0001.parquet")
```

## S3 config example

The S3 adapter uses direct HTTP calls with Req + AWS SigV4.
It supports single PUT and multipart upload modes.

```elixir
%{
  provider: :s3,
  bucket: "ducklake-data",
  prefix: "prod",
  region: "us-east-1",
  access_key_id: System.fetch_env!("AWS_ACCESS_KEY_ID"),
  secret_access_key: System.fetch_env!("AWS_SECRET_ACCESS_KEY"),
  session_token: System.get_env("AWS_SESSION_TOKEN"),
  endpoint: System.get_env("S3_ENDPOINT"), # optional for S3-compatible/localstack
  force_path_style: true,                   # common for S3-compatible services
  adapter_opts: %{
    multipart_threshold: 64 * 1_024 * 1_024,
    part_size: 8 * 1_024 * 1_024,
    chunk_size: 8 * 1_024 * 1_024
  }
}
```

## GCS config example

```elixir
%{
  provider: :gcs,
  bucket: "ducklake-data",
  prefix: "prod",
  token: System.fetch_env!("GCS_OAUTH_TOKEN")
}
```

You can also provide `token_fun: fn -> token end` for rotating tokens.

## CDC buffering + routing foundations

Added normalized CDC event and routing modules:
- `DuckFeeder.CDC.Event`
- `DuckFeeder.CDC.TransactionBuffer`
- `DuckFeeder.CDC.Router`
- `DuckFeeder.CDC.ChangelogRow`
- `DuckFeeder.CDC.Setup`
- `DuckFeeder.CDC.Bootstrap`
- `DuckFeeder.CDC.ReplicationProtocol`
- `DuckFeeder.CDC.LogicalReplication.Messages`
- `DuckFeeder.CDC.LogicalReplication.Decoder`
- `DuckFeeder.CDC.LogicalReplication.Converter`
- `DuckFeeder.CDC.MessageMapper`
- `DuckFeeder.CDC.SnapshotBoundary`
- `DuckFeeder.CDC.InitialSnapshot`
- `DuckFeeder.CDC.InitialSnapshot.Runner`
- `DuckFeeder.CDC.Connection`
- `DuckFeeder.CDC.Pipeline`
- `DuckFeeder.Service`

`TransactionBuffer` emits committed transactions only at commit boundaries.
`Router` maps committed changes to designated target tables.

Added batching/pipeline foundations:
- `DuckFeeder.Ingest.BatchBuffer`
- `DuckFeeder.TablePipeline`
- `DuckFeeder.Ingest`

## Meta schema + checkpoint/batch state machine

Added `duckfeeder_meta` bootstrap SQL:
- `priv/duckfeeder_meta/create_tables.sql`

Added Postgres-backed control-plane modules:
- `DuckFeeder.Meta.SQL`
- `DuckFeeder.Meta.Store`
- `DuckFeeder.Meta.BatchState`
- `DuckFeeder.Meta`

Typical bootstrap/use flow:

```elixir
{:ok, conn} = Postgrex.start_link(...)
:ok = DuckFeeder.Meta.bootstrap(conn)

{:ok, source_id} = DuckFeeder.Meta.register_source(conn, %{name: "primary"})

{:ok, designated_table_id} =
  DuckFeeder.Meta.register_designated_table(conn, %{
    source_id: source_id,
    source_schema: "public",
    source_table: "users",
    target_schema: "raw",
    target_table: "users"
  })

{:ok, "0/0"} = DuckFeeder.Meta.fetch_checkpoint(conn, designated_table_id)

# after files are uploaded and batch state is :uploaded
{:ok, %{checkpoint_lsn: _lsn, committed?: true}} =
  DuckFeeder.Meta.commit_uploaded_batch(conn, batch_id)
```

## Integration testing

Requirements:
- local Postgres instances available for:
  - metadata DB (`meta_database_url`)
  - source DB with logical replication enabled (`source_database_url`)
- source DB must support logical replication (`wal_level=logical`, replication slot/publication privileges)
- `duckdb` CLI on PATH
- integration DB URLs configured in `config/test.exs` under `:duck_feeder, :integration`

Example `config/test.exs`:

```elixir
config :duck_feeder, :integration,
  meta_database_url: "postgres://postgres:postgres@localhost:5432/duck_feeder_meta_test",
  source_database_url: "postgres://postgres:postgres@localhost:5432/duck_feeder_source_test"
```

Helper script:
- `scripts/test_integration.sh`

Run:

```bash
scripts/test_integration.sh
```

## Telemetry

Core events currently emitted:
- `[:duck_feeder, :cdc, :event]`
- `[:duck_feeder, :cdc, :connection]`
- `[:duck_feeder, :cdc, :frame]`
- `[:duck_feeder, :batch, :flushed]`
- `[:duck_feeder, :batch, :processed]`
- `[:duck_feeder, :reconciler, :run]`

## Progress tracking

See `docs/current_status.md` for the canonical status/task list and Apache-2.0 compliance notes.

Convenience helpers:
- `DuckFeeder.runtime_child_spec/4`
- `DuckFeeder.runtime_child_spec_from_config/3`

## Notes

- Object keys are built from `prefix + relative_key`.
- Adapter override is supported via `adapter: MyAdapter` for tests/custom providers.
- Batch states: `pending -> encoded -> uploaded -> committed` (with `failed` + retry path to `pending`).
