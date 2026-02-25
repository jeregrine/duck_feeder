# Ingestion Review: DuckFeeder vs Logflare + Broadway

Date: 2026-02-25

## Scope

You asked for a comparison of ingestion/buffering approaches across:

- DuckFeeder (this repo)
- Logflare (`research/vendor/logflare`)
- Broadway (`research/vendor/broadway`)

and a recommendation on whether DuckFeeder is overengineered.

---

## What I reviewed

### DuckFeeder

- `lib/duck_feeder/service.ex`
- `lib/duck_feeder/append_stream.ex`
- `lib/duck_feeder/table_pipeline.ex`
- `lib/duck_feeder/ingest/batch_buffer.ex`
- `lib/duck_feeder/ingest.ex`
- `lib/duck_feeder/cdc/pipeline.ex`
- `lib/duck_feeder/cdc/transaction_buffer.ex`
- `lib/duck_feeder/cdc/connection.ex`
- `lib/duck_feeder/batch_processor.ex`
- `lib/duck_feeder/meta/store.ex`

### Logflare

- `research/vendor/logflare/architecture.md`
- `research/vendor/logflare/lib/logflare/backends.ex`
- `research/vendor/logflare/lib/logflare/backends/ingest_event_queue.ex`
- `research/vendor/logflare/lib/logflare/backends/buffer_producer.ex`
- `research/vendor/logflare/lib/logflare/backends/dynamic_pipeline.ex`
- `research/vendor/logflare/lib/logflare/backends/ingest_event_queue/queue_janitor.ex`
- `research/vendor/logflare/lib/logflare/backends/adaptor/*.ex` (esp. BigQuery/ClickHouse/S3/Postgres)
- `research/vendor/logflare/lib/logflare/backends/adaptor/*/pipeline.ex`

### Broadway

- `research/vendor/broadway/README.md`
- `research/vendor/broadway/lib/broadway.ex`
- `research/vendor/broadway/guides/internals/architecture.md`
- `research/vendor/broadway/guides/examples/custom-producers.md`

---

## TL;DR recommendation

**Do not do a full ingestion rewrite to Broadway for DuckFeeder CDC right now.**

Broadway would help with some plumbing (demand, stage wiring, batching ergonomics), but **it does not remove DuckFeeder’s hardest constraints**:

1. transaction-aware CDC semantics,
2. strict WAL ack-after-durable-commit behavior,
3. source-level correctness across per-table pipelines.

**Where we should simplify:** extract shared queue/executor logic currently duplicated in `Service` and `AppendStream`, and consider a **small Broadway spike on append-only paths first** (where lossy policy is already acceptable).

---

## Comparison: architecture and tradeoffs

## 1) DuckFeeder (current)

### Shape

- CDC connection (`CDC.Connection`) decodes pgoutput and forwards events.
- Transaction buffering (`CDC.TransactionBuffer`) groups changes between BEGIN/COMMIT.
- Ingest routing (`Ingest`) fans into per-table `TablePipeline` processes.
- `TablePipeline` does micro-batching by rows/bytes/time.
- Flushed batches are processed async (`BatchProcessor`: write -> upload -> metadata commit).
- WAL ack is advanced only after commit result returns checkpoint LSN.

### Strengths

- Very explicit about durability boundaries.
- Fail-closed behavior exists for CDC overflow (`Service`).
- Strong observability around queue depth/lag/backpressure.
- Good separation between CDC parsing, table buffering, and commit pipeline.

### Current complexity hotspots

- `Service` and `AppendStream` duplicate most queue/inflight/pending logic.
- Manual bounded queue + task orchestration logic is substantial and subtle.
- Backpressure appears in multiple layers (CDC lag guard + service queue bounds + table flush cadence).

---

## 2) Logflare

### Shape

- API ingestion validates/transforms events, then enqueues to ETS (`IngestEventQueue`).
- `BufferProducer` (GenStage producer) pulls from queue and feeds Broadway pipelines.
- Backend-specific Broadway pipelines process+batch for each sink.
- `DynamicPipeline` scales number of pipelines from queue depth/rate.
- `QueueJanitor` periodically truncates/drop-cleans queues.

### Strengths

- Good throughput-oriented design.
- Broadway gives clean processor/batcher separation.
- Dynamic scaling and queue fanout strategy are practical for high ingest volume.

### Tradeoffs (important)

- Multiple adaptor pipelines have `# TODO: re-queue failed` in ack callbacks.
- Queue janitor intentionally drops data under pressure.
- Durability is sink/throughput-oriented, not strict replay safety oriented.

So Logflare is an excellent model for **high-throughput operational ingestion**, but it targets different guarantees than DuckFeeder CDC.

---

## 3) Broadway fit for DuckFeeder

Broadway would give us:

- built-in GenStage backpressure,
- cleaner stage topology,
- built-in batching and telemetry events,
- optional partitioning semantics.

But Broadway does **not** give us automatically:

- source-level durable checkpoint watermark management,
- retries semantics (producer/sink-specific, must be implemented),
- strict ordering guarantees across failure/retry scenarios,
- CDC transaction semantics.

In short: Broadway can simplify execution plumbing, but cannot replace DuckFeeder’s core correctness logic.

---

## Are we overengineered?

**Conclusion: partly.**

- **Not overengineered** for CDC durability goals. The complexity is mostly justified.
- **Over-duplicated** in local execution machinery (`Service` vs `AppendStream`) and that is where simplification should happen first.

---

## Proposal

## Phase 1 (now): simplify without semantic change

1. Extract shared batch execution queue logic from:
   - `lib/duck_feeder/service.ex`
   - `lib/duck_feeder/append_stream.ex`
2. Keep behavior exactly the same (including fail-closed vs drop-oldest policy split).
3. Add focused tests around overflow/task-crash semantics to lock behavior.

Outcome: less maintenance burden immediately, no architecture risk.

## Phase 2 (targeted spike): Broadway on append-only path

1. Build a small `AppendStream.BroadwayExecutor` prototype:
   - producer fed by `{:duck_feeder_batch, table, batch}` messages,
   - processor executes `BatchProcessor.process_batch/3`,
   - `handle_failed/2` maps to current drop/error notifications.
2. Keep CDC path untouched.
3. Measure:
   - throughput,
   - mailbox pressure,
   - complexity (LOC + bug surface),
   - telemetry parity.

Outcome: real data on whether Broadway meaningfully improves our runtime.

## Phase 3 (only if Phase 2 is clearly better): evaluate CDC adoption

Only proceed if we can prove we can preserve strict ack safety and ordering semantics with no regression.

---

## Decision

**Where we land:**

- Do **not** switch DuckFeeder CDC ingestion wholesale to Broadway now.
- Do simplify internal queue orchestration first.
- Do run a narrow Broadway spike on append streams to validate value before deeper adoption.

This keeps our durability guarantees intact while still addressing legitimate complexity concerns.
