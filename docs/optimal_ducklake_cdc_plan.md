# DuckFeeder: Optimal Elixir-First Plan

## Objective
Build an Elixir library/service that:
- reads PostgreSQL WAL (logical replication / CDC),
- writes Parquet files,
- uploads to **S3-compatible object stores or GCS**, with a semi-generic storage interface,
- maintains **DuckLake metadata in PostgreSQL**,
- stays **Elixir-first** with the **smallest possible native surface**.

This plan intentionally avoids multi-phase architecture. It defines the target end-state directly.

---

## Hard Decisions (Chosen)

1. **Elixir owns orchestration and correctness**
   - CDC stream lifecycle
   - batching/ordering
   - checkpoints and idempotency
   - object storage upload retries (S3/GCS)
   - DuckLake catalog commits

2. **Rustler NIF is write-only and minimal**
   - only Parquet encoding + file metadata extraction
   - no networking, no replication logic, no catalog writes

3. **No DuckDB in ingest hot path**
   - we write DuckLake metadata directly in Postgres with SQL templates aligned to DuckLake spec version
   - this keeps runtime simple and Elixir-centric

4. **Object storage uploads done in Elixir via adapters**
   - `S3` adapter: direct HTTP (Req + SigV4) for AWS + S3-compatible providers
   - `GCS` adapter: direct HTTP (Req) with OAuth Bearer token
   - explicit control for retries, concurrency, checksums, and provider-specific settings

5. **Write mode is CDC changelog (first-class)**
   - every insert/update/delete becomes an appended row with op metadata
   - avoids complex positional delete generation in v1
   - keeps ingestion robust and simple while preserving full history

---

## Architecture (Target State)

```text
Postgres (logical slot/publication)
        |
        v
Elixir CDC Client (Postgrex.ReplicationConnection)
        |
        v
Txn-aware Router + Table Buffers
        |
        v
Rustler Parquet Writer (local temp files)
        |
        v
Elixir Object Storage Uploader (S3 multipart / GCS resumable)
        |
        v
DuckLake Committer (Postgres SQL transaction)
        |
        v
Checkpoint + Batch Manifest update
```

---

## What We Borrow (inspiration)

From Electric sync-service:
- replication state machine patterns
- relation cache/schema tracking
- WAL message decode flow and commit-boundary handling

From Moonlink:
- transaction-aware sink behavior
- initial snapshot + WAL handoff pattern
- sharded initial copy idea

---

## Module Layout (Elixir)

- `DuckFeeder.Config`
  - validates source/target configs (NimbleOptions)

- `DuckFeeder.CDC.Connection`
  - wraps `Postgrex.ReplicationConnection`
  - publication/slot setup
  - standby status updates

- `DuckFeeder.CDC.Decoder`
  - pgoutput decode + relation cache
  - emits normalized events (`begin`, `insert`, `update`, `delete`, `commit`, `relation`)

- `DuckFeeder.Router`
  - routes events to designated table pipelines
  - preserves per-table commit ordering

- `DuckFeeder.TablePipeline`
  - per designated table GenServer/GenStage-style worker
  - aggregates committed events into micro-batches

- `DuckFeeder.BatchBuffer`
  - transaction-aware row buffering
  - size/time thresholds for flush

- `DuckFeeder.Parquet`
  - Elixir wrapper around Rustler NIF
  - prepares rows + schema for NIF call

- `DuckFeeder.Storage`
  - semi-generic storage behavior (provider-agnostic contract)

- `DuckFeeder.Storage.S3`
  - multipart upload, retries, checksums, endpoint/path-style compatibility options

- `DuckFeeder.Storage.GCS`
  - resumable upload, retries, auth handling, and GCS-specific options

- `DuckFeeder.DuckLake.Committer`
  - performs catalog writes in one Postgres transaction
  - idempotency checks + advisory locking

- `DuckFeeder.CheckpointStore`
  - last committed LSN per designated table
  - pending/failed/committed batches

- `DuckFeeder.InitialSnapshot`
  - snapshot export + table copy + WAL handoff

- `DuckFeeder.Reconciler`
  - orphan file cleanup
  - stuck-batch recovery

---

## Semi-generic Storage Interface

Keep the storage interface intentionally narrow (semi-generic, not a full object-store abstraction layer):

```elixir
defmodule DuckFeeder.Storage.Adapter do
  @type object_ref :: %{bucket: String.t(), key: String.t()}

  @type put_opts :: %{
          optional(:content_type) => String.t(),
          optional(:metadata) => map(),
          optional(:checksum) => String.t(),
          optional(:adapter_opts) => map()
        }

  @type put_result :: %{
          etag: String.t() | nil,
          version_id: String.t() | nil,
          size: non_neg_integer()
        }

  @callback put_file(local_path :: Path.t(), object_ref(), put_opts()) ::
              {:ok, put_result()} | {:error, term()}

  @callback head_object(object_ref()) :: {:ok, map()} | {:error, term()}

  @callback delete_object(object_ref()) :: :ok | {:error, term()}
end
```

`DuckFeeder.Storage` dispatches to `DuckFeeder.Storage.S3` or `DuckFeeder.Storage.GCS` by provider config.
Provider-specific tuning lives in `adapter_opts` so ingest/commit logic stays provider-agnostic.

---

## Rustler NIF (smallest viable contract)

### NIF API

- `write_parquet(file_path, schema, rows, opts) :: {:ok, manifest} | {:error, reason}`

Where `manifest` includes:
- `row_count`
- `file_size_bytes`
- `footer_size`
- per-column stats (`null_count`, `min`, `max`) for DuckLake file stats tables

### Constraints
- dirty scheduler only (`DirtyCpu` / `DirtyIo` as needed)
- no object storage/network calls
- no process-global mutable state
- panic-safe boundary (`catch_unwind` + typed errors)

### Why this is optimal
- native code remains tiny and testable
- Elixir still controls orchestration/retries/checkpoint logic
- good performance without sidecar complexity

---

## DuckLake Metadata Strategy (Postgres)

Use official DuckLake schema (spec-pinned, e.g. `0.3`) in a dedicated metadata DB/schema.

### Commit transaction steps

For each batch (per table):

1. `pg_advisory_xact_lock(table_id)`
2. read checkpoint row `FOR UPDATE`
3. if `batch_end_lsn <= checkpoint_lsn`: no-op (already committed)
4. allocate new snapshot/file IDs
5. insert new `ducklake_snapshot` row + `ducklake_snapshot_changes`
6. insert `ducklake_data_file` rows for new Parquet files
7. update `ducklake_table_stats` and file/column stats tables
8. update `duckfeeder_meta.checkpoints` to `batch_end_lsn`
9. mark `duckfeeder_meta.batches` as committed
10. commit

This is the core exactly-once boundary.

---

## Control Plane Schema (`duckfeeder_meta`)

Required tables:

- `sources`
  - source DSN, slot, publication, status

- `designated_tables`
  - source schema/table -> target schema/table
  - mode (`cdc_changelog`)
  - primary key columns (for future materialization)
  - partition config

- `checkpoints`
  - designated table id
  - last_committed_lsn
  - updated_at

- `batches`
  - batch_id (deterministic)
  - lsn_start, lsn_end
  - state (`pending|encoded|uploaded|committed|failed`)
  - error info, retry_count

- `batch_files`
  - batch_id
  - object_key
  - row_count
  - file_size
  - checksum/etag

- `schema_history`
  - relation OID mapping + evolution records

---

## Event Model in Target Table (CDC changelog)

Each target DuckLake table appends rows with metadata columns:

- `_op` (`I|U|D|R`)  
- `_commit_lsn` (BIGINT)
- `_xid` (BIGINT)
- `_source_ts` (TIMESTAMPTZ if available)
- `_ingest_ts` (TIMESTAMPTZ)

`R` = initial snapshot row.

This gives full fidelity and avoids fragile delete-file mechanics in v1.

---

## Initial Snapshot + WAL Handoff

1. create/verify publication + slot
2. begin repeatable-read snapshot transaction
3. capture `snapshot_id` + boundary LSN
4. copy table data (optionally CTID-sharded)
5. write/upload Parquet and commit as snapshot rows (`_op='R'`, boundary LSN)
6. start CDC stream
7. ignore CDC transactions with commit LSN `<= boundary LSN`
8. continue normal streaming

---

## Exactly-Once and Idempotency

- deterministic `batch_id = hash(table_id, lsn_start, lsn_end, file_index_set)`
- deterministic object keys from LSN range
- checkpoint advances **only after** DuckLake commit transaction succeeds
- repeated runs safely no-op on already committed LSN ranges

If upload succeeds but commit fails:
- batch remains `uploaded/failed`
- retry reuses same object keys
- reconciler cleans true orphans after TTL

---

## Object Storage Requirements (S3 + GCS)

### Shared config
- `provider` (`:s3 | :gcs`)
- `bucket`
- `prefix`
- `max_concurrency`
- retry/backoff settings
- checksum policy

### S3 / S3-compatible config
- `endpoint` (custom)
- `region`
- `access_key_id` / `secret_access_key`
- `force_path_style` (critical for many S3-compatible providers)
- TLS verify toggle / custom CA bundle

### GCS config
- `project_id`
- service account credentials (JSON path or env)
- optional impersonated service account
- optional storage class
- optional HMAC interop mode (routes through S3 adapter semantics)

Uploader behavior:
- S3: multipart threshold + part size + parallel parts
- GCS: resumable session + chunk size
- bounded concurrency
- exponential backoff with jitter

---

## Schema Evolution Policy

- handle `Relation` messages from WAL
- additive columns: auto-add to target metadata (new snapshot)
- type widening: allowed via mapping rules
- breaking changes: mark table pipeline degraded + require operator action

All schema changes recorded in `schema_history`.

---

## Ops and Observability

Telemetry events:
- `duck_feeder.cdc.message`
- `duck_feeder.batch.flush`
- `duck_feeder.parquet.write`
- `duck_feeder.storage.upload` (with `provider: :s3 | :gcs`)
- `duck_feeder.ducklake.commit`
- `duck_feeder.checkpoint.advance`

Key metrics:
- replication lag (LSN diff)
- batch flush latency
- upload throughput
- commit latency
- retry counts
- stuck batches

---

## Test Matrix (must-have)

1. **Unit**
   - decoder correctness (insert/update/delete/relation/streaming txn)
   - batch state transitions
   - idempotent committer logic

2. **Integration (docker-compose)**
   - Postgres + metadata Postgres + LocalStack S3 profile + GCS-emulator profile
   - snapshot + streaming handoff
   - restart/recovery from checkpoints

3. **Failure Injection**
   - kill process between upload and commit
   - network timeout during multipart upload
   - metadata DB deadlock/timeout and retry

4. **Compatibility**
   - AWS S3
   - GCS (or fake-gcs-server)
   - LocalStack S3 (local testing)
   - at least one alt S3 provider (R2/Wasabi)

---

## Build Checklist (single-track, no phases)

- [ ] Add dependencies: `postgrex`, `rustler`, `req`, `nimble_options`, `telemetry`
- [ ] Implement `duckfeeder_meta` migrations + bootstrap SQL
- [ ] Implement CDC connection + decoder + relation cache
- [ ] Implement designated table router and per-table pipeline workers
- [ ] Implement Rustler Parquet NIF and Elixir wrapper
- [ ] Implement S3 multipart uploader with compatibility flags
- [ ] Implement DuckLake SQL committer with advisory lock + idempotency
- [ ] Implement snapshot export/copy + WAL handoff
- [ ] Implement reconciler and retry policies
- [ ] Add telemetry + health/status API
- [ ] Add full integration and failure tests

---

## Final Note

This design keeps >90% of system logic in Elixir while limiting native code to one narrow, high-value concern (Parquet encoding). It avoids sidecars and DuckDB in the ingest path, but still preserves robust CDC correctness via transactional metadata commits and LSN checkpoints.