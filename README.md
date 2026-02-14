# DuckFeeder

Elixir-first CDC ingest library (Postgres WAL -> Parquet -> object storage -> DuckLake metadata).

## Current status

Started with a **semi-generic storage interface** supporting:
- `:s3` (AWS S3 and S3-compatible)
- `:gcs` (Google Cloud Storage JSON API)

HTTP stack is **Req-only** (no hackney dependency in this project).

`DuckFeeder.Config` validates runtime config (source/storage/metadata/ingest) with NimbleOptions.

## Metadata bootstrap from config

You can seed `duckfeeder_meta` source + designated table rows from runtime config:

```elixir
{:ok, validated} = DuckFeeder.validate_config(runtime_config)
{:ok, %{source_id: _id, designated_table_ids: _ids}} =
  DuckFeeder.seed_meta(meta_conn, validated, source_name: "primary")

# Convenience: seed metadata and immediately start stream runtime
{:ok, %{runtime: %{service_pid: _service, cdc_pid: _cdc}}} =
  DuckFeeder.seed_and_start_stream(meta_conn, validated,
    seed_opts: [source_name: "primary"],
    start_opts: [bootstrap_replication?: false]
  )
```

## Writer API

`DuckFeeder.Writer` supports both JSONL and Parquet output.
`DuckFeeder.Writer.ParquetNif` now writes Parquet via a Rustler NIF.

You can select by format (`:jsonl | :parquet`) or explicit adapter module.
Optional fallback is supported (e.g. `format: :parquet, fallback_format: :jsonl`).

```elixir
{:ok, write_result} = DuckFeeder.write_batch(%{}, %{rows: [%{"id" => 1}]})
:ok = DuckFeeder.cleanup_written_batch(%{}, write_result)
```

## Batch processing API

`DuckFeeder.BatchProcessor` connects flushed batches to write/upload/meta commit steps.
It supports pluggable committers via `DuckFeeder.DuckLake.Committer` (default no-op committer).
`DuckFeeder.DuckLake.Committer.Postgres` is available as a transactional scaffold for
running DuckLake SQL statements + checkpoint commit in one transaction.
By default it writes spec-aligned snapshot/file/change rows into
`ducklake_metadata.ducklake_snapshot`, `ducklake_metadata.ducklake_data_file`, and
`ducklake_metadata.ducklake_snapshot_changes`, refreshes `ducklake_metadata.ducklake_table_stats`,
records schema/commit history in `duckfeeder_meta.schema_history`, and writes an audit row in
`duckfeeder_meta.ducklake_commits`.
(override via `committer_opts[:ducklake_sql]`).
Runtime/service startup accepts `committer_module` and `committer_opts` passthrough.

```elixir
{:ok, result} = DuckFeeder.process_batch(context, {"raw", "users"}, batch)
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

Replication connection tuning options include:
- `auto_reconnect: true | false`
- `reconnect_backoff: milliseconds`
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
- `duckdb` CLI on PATH
- integration DB URLs configured in `config/test.exs` under `:duck_feeder, :integration`

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

See `docs/current_status.md` for implemented modules vs remaining target architecture work.

## Third-party licensing notes

See `docs/third_party_licenses.md` for Apache-2.0 attribution/compliance notes for
referenced ElectricSQL LSN extension patterns.

For embedding DuckFeeder in an existing OTP app, see:
- `docs/existing_app_supervision.md`

Convenience helpers:
- `DuckFeeder.runtime_child_spec/4`
- `DuckFeeder.runtime_child_spec_from_config/3`

## Notes

- Object keys are built from `prefix + relative_key`.
- Adapter override is supported via `adapter: MyAdapter` for tests/custom providers.
- Batch states: `pending -> encoded -> uploaded -> committed` (with `failed` + retry path to `pending`).
