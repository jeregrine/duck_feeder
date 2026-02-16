# DuckFeeder Code Review

## Architecture Summary

DuckFeeder is a CDC pipeline that replicates from PostgreSQL via logical replication into DuckLake-format Parquet files on S3/GCS, with Postgres-backed metadata. The pipeline is: `CDC.Connection` → `CDC.Pipeline` → `Ingest` → `TablePipeline` (batching) → `Service` → `BatchProcessor` (write → upload → commit).

---

## 🔴 CRITICAL: Jepsen-Style Race Conditions & Data Integrity

### 1. LSN Acked Before Data Is Committed — Data Loss Window

**File:** `lib/duck_feeder/cdc/connection.ex`, `maybe_ack_event/2`

The CDC connection acks PG's LSN immediately when it decodes a `Commit` event:

```elixir
defp maybe_ack_event(state, %Event.Commit{end_lsn: end_lsn}) do
  lsn = Lsn.parse!(end_lsn)
  state = %{state | flushed_lsn: max(state.flushed_lsn, lsn), applied_lsn: max(state.applied_lsn, lsn)}
  {[standby_status_update(state)], state}
end
```

But downstream, the event has only been buffered in-memory. The actual write→upload→commit pipeline is async and many steps later. The event flows: `Connection` → (send message) → `Service.handle_info` → `Pipeline.push_event` → `TransactionBuffer` → (cast) → `Ingest` → (cast) → `TablePipeline` buffer → (timer/threshold) → (send message) → `Service.handle_info(:duck_feeder_batch)` → `BatchProcessor.process_batch` (write, upload, DB commit).

**If the process crashes anywhere in this pipeline after the LSN ack but before `commit_uploaded_batch`, PostgreSQL considers those WAL bytes consumed.** On restart, `Runtime.start_stream` resolves `start_lsn` from `Meta.fetch_source_start_lsn` (the checkpoint), but the replication slot's `confirmed_flush_lsn` has already advanced past that point. PostgreSQL will NOT resend those events. **Data is silently lost.**

This is the textbook "ack before persist" anti-pattern that Jepsen catches.

**Fix:** Only advance `flushed_lsn`/`applied_lsn` after the batch is committed, or ack lazily based on the meta checkpoint. One approach: the Service sends a `{:ack, lsn}` message back to the Connection after batch commit, and the Connection only updates `applied_lsn` from that callback.

### 2. `:encoded` Batches Are Never Reconciled — Stuck State

**File:** `lib/duck_feeder/reconciler.ex`

The reconciler only processes batches in `[:uploaded, :failed]` states:

```elixir
states = Keyword.get(opts, :states, [:uploaded, :failed])
```

But `BatchProcessor.process_uncommitted_batch` transitions: `pending → encoded → (write) → (upload) → encoded → uploaded → committed`. If the process crashes after the file is uploaded to S3 but before `advance_to(meta, conn, batch_id, :encoded, :uploaded)` succeeds, the batch is stuck in `:encoded` state forever. It has an orphaned S3 object, and will never be retried or cleaned up.

**Fix:** Add `:encoded` to the default reconciler states, or coalesce the state machine so upload+advance happen atomically.

### 3. Non-Atomic Write-Upload-Commit — Orphaned S3 Objects

**File:** `lib/duck_feeder/batch_processor.ex`, `finalize_written_batch/7`

The batch commit sequence is:

1. `Storage.put_file` (upload to S3)
2. `meta.put_batch_file` (record file in Postgres)
3. `advance_to(meta, conn, batch_id, :encoded, :uploaded)` (separate DB query)
4. `committer_module.commit_batch` (commit in yet another DB transaction)

Each step is a separate operation. If the process crashes between steps 1 and 2, there's an orphan in S3 with no metadata. Between 2 and 3, metadata exists but state is wrong. The reconciler only partially addresses this (see issue #2 above).

### 4. `TablePipeline` Buffer Loss on Process Death

**File:** `lib/duck_feeder/table_pipeline.ex`

`TablePipeline` holds rows in an in-memory `BatchBuffer`. If the pipeline process dies (or the parent Ingest/Service dies), all buffered rows are lost. Combined with issue #1 (LSN already acked), these rows are irrecoverably lost.

The `DynamicSupervisor` will restart the pipeline, but with an empty buffer. The CDC connection has already told Postgres those events were consumed.

### 5. `ingest_transaction` Is Cast-Based — No Backpressure, Silent Drop

**File:** `lib/duck_feeder/ingest.ex`

```elixir
def ingest_transaction(server, transaction) when is_map(transaction) do
  GenServer.cast(server, {:ingest_transaction, transaction})
end
```

Casts can be silently dropped if the target process is dead or the mailbox is overwhelmed. There's no `{:error, _}` return path. Combined with the eager LSN ack, a dropped cast means data loss.

### 6. Race Between Reconciler and Normal Processing (Handled Correctly)

If the reconciler runs `commit_uploaded_batch` on a stale batch while the normal pipeline is also trying to complete that same batch (e.g., after a slow upload), both will try to transition `uploaded → committed`. The `FOR UPDATE` lock in `commit_uploaded_batch_tx` serializes them, and `already_committed?` is returned for the second caller. This is handled correctly.

---

## 🟡 MODERATE: Performance Issues

### 1. `BatchBuffer.estimate_row_size` — `term_to_binary` on Every Row

**File:** `lib/duck_feeder/ingest/batch_buffer.ex`

```elixir
defp estimate_row_size(row), do: row |> :erlang.term_to_binary() |> byte_size()
```

Every single `append` call serializes the full row just to estimate its byte size. For 10K rows/batch at high throughput, this is a major CPU and allocation cost. Consider a cheaper heuristic (e.g., count keys × avg value size, or sample every Nth row).

### 2. Parquet NIF — Double JSON Serialization

**File:** `lib/duck_feeder/writer/parquet_nif.ex`

```elixir
rows_json <- JSON.encode!(normalized_rows),
:ok <- run_nif_write(path, rows_json),
```

All rows are JSON-encoded in Elixir, passed as a string to the NIF, then parsed again in Rust via `serde_json::from_str`. For a 10K-row batch, this is two full serialization passes. The NIF should accept Elixir terms directly via Rustler's `Term` type, or use a binary protocol.

### 3. GCS Adapter Reads Entire File Into Memory

**File:** `lib/duck_feeder/storage/gcs.ex`

```elixir
{:ok, body} <- File.read(local_path),
```

For large Parquet files (100MB+), this loads the entire file into a single binary. The S3 adapter correctly uses `File.stream!`. The GCS adapter should stream uploads using GCS resumable uploads.

### 4. DuckLake Commit Generates O(columns) SQL Statements

**File:** `lib/duck_feeder/duck_lake/sql.ex`, `default_commit_statements/2`

For a table with N columns, each batch commit executes ~5N+15 SQL statements (validate + ensure column + ensure mapping + table stats + file stats per column, plus snapshot/table/file/changes overhead). For a 50-column table, that's ~265 individual `Postgrex.query` calls inside one transaction. Consider batching column operations into array-parameter bulk SQL.

### 5. Service GenServer Blocks During Batch Processing

**File:** `lib/duck_feeder/service.ex`, `handle_info({:duck_feeder_batch, ...})`

```elixir
def handle_info({:duck_feeder_batch, table, batch}, %State{} = state) do
  result = BatchProcessor.process_batch(state.context, table, batch)
  ...
end
```

`process_batch` does write → S3 upload → multi-step DB commit synchronously. During this (potentially seconds), the Service can't process new CDC events. Its mailbox grows, and the CDC connection may hit `max_lag_bytes`. Consider spawning batch processing into a Task or pool.

### 6. `extract_column_descriptors` Scans All Rows

**File:** `lib/duck_feeder/duck_lake/sql.ex`

For type inference and stats, every row in the batch is iterated. Column schemas are stable across batches for the same table — consider caching the schema after the first batch.

---

## 🟠 Production Readiness Issues

### 1. No Retry on Storage Uploads

`Storage.put_file` has no retry. `Req` is configured with `retry: false`. A single transient S3/GCS 500 or network timeout fails the batch. Add exponential backoff retries (at least for idempotent PUT operations).

### 2. Atom Table Exhaustion Risk

**Files:** `lib/duck_feeder/config.ex`, `lib/duck_feeder/cdc/connection_options.ex`

```elixir
defp normalize_key(key) when is_binary(key) do
  try do
    String.to_existing_atom(key)
  rescue
    ArgumentError -> String.to_atom(key)
  end
end
```

If config comes from external input (e.g., JSON API), this creates atoms from arbitrary strings. Atoms are never GC'd on the BEAM — this is a DoS vector.

### 3. No Application Supervision Tree

`mix.exs` defines no `mod:` callback. DuckFeeder is a library, which is fine, but there's no built-in mechanism for health monitoring or graceful shutdown at the application level.

### 4. No Timeouts on Batch Processing

If an S3 upload hangs (network black hole), `BatchProcessor.process_batch` blocks indefinitely, taking the Service GenServer with it. Add timeouts to HTTP calls and overall batch processing.

### 5. Temp File Leak

`Writer.ParquetNif` and `Writer.Jsonl` create temp files. Cleanup happens in `Writer.cleanup`, but if the process crashes between `write_batch` and `cleanup`, temp files are orphaned. Consider using a periodic temp dir reaper.

### 6. `postgres_url` Stored in `connection_info` JSONB

**File:** `lib/duck_feeder/bootstrap.ex`

```elixir
connection_info = %{postgres_url: source.postgres_url}
```

The full Postgres connection URL (including password) is stored in the `duckfeeder_meta.sources.connection_info` JSONB column. Consider encrypting at rest or storing only host/port/dbname and retrieving credentials from a vault.

### 7. No Dead-Letter / Poison-Row Handling

If a single row causes `Writer.write_batch` to crash (e.g., un-serializable data), the entire batch fails. There's no mechanism to skip poison rows and continue.

### 8. Missing Telemetry for Key Failure Modes

There's no telemetry event for: stuck `:encoded` batches, orphaned S3 objects, LSN ack-vs-commit lag, pipeline buffer sizes, or temp file counts.

---

## ✅ Things Done Well

- **Deterministic batch IDs** (`BatchId.build`) make insert idempotent — excellent for crash recovery
- **`FOR UPDATE` row locking** in `transition_batch` and `commit_uploaded_batch_tx` — correct serialization
- **`GREATEST` checkpoint advancement** prevents LSN regression on concurrent commits
- **Reconciler pattern** for recovering stuck batches is sound architecture
- **Multipart S3 upload** with abort-on-failure is properly implemented
- **Robust pgoutput decoder** with proper binary protocol handling
- **Transaction boundary enforcement** in `TransactionBuffer` (xid mismatch detection)
- **Telemetry integration** is comprehensive for the happy path
- **NIF scheduled on DirtyIo** — correct scheduler choice for file I/O
- **Clean separation of concerns** (CDC → Events → Router → Ingest → Pipeline → Writer → Storage)

---

## Summary: Top Priorities

| Priority | Issue | Risk |
|----------|-------|------|
| **P0** | LSN acked before batch commit (§1) | Silent data loss on crash |
| **P0** | `:encoded` batches not reconciled (§2) | Data stuck, S3 orphans accumulate |
| **P1** | No backpressure from batch processing to CDC (§5) | Unbounded memory growth, OOM |
| **P1** | Service blocks during batch processing (Perf §5) | Lag buildup, CDC disconnects |
| **P1** | No retry on S3/GCS uploads (Prod §1) | Batch failures on transient errors |
| **P2** | GCS reads full file into memory (Perf §3) | OOM on large files |
| **P2** | `term_to_binary` per row for size estimate (Perf §1) | CPU overhead at scale |
| **P2** | Double JSON serialization in Parquet NIF (Perf §2) | CPU + memory overhead |
| **P2** | Atom table exhaustion from config keys (Prod §2) | DoS vector |
| **P3** | Credentials stored in plaintext JSONB (Prod §6) | Security exposure |
| **P3** | O(columns) SQL per batch commit (Perf §4) | Slow commits for wide tables |
