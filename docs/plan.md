# DuckFeeder plan

README and `AGENTS.md` already cover the product and architecture.
This file is only for the next concrete cleanup and refactor targets.

## Current priorities

### 1. Break up `Runtime.start_stream/4`

It still does too much in one function:

- source resolution
- checkpoint/start LSN resolution
- snapshot handoff planning
- bootstrap
- snapshot execution
- snapshot replay
- service startup
- CDC startup
- cleanup/error choreography

Goal:

- split it into explicit phases
- keep phase state small and obvious
- reduce the amount of local error-handling/control-flow nesting

### 2. Keep simplifying `Sink.DuckDB`

It is still the main complexity hotspot.

Current responsibilities still mixed together:

- setup hook memoization
- applied-batch tracking
- append batching SQL generation
- CDC apply logic
- schema evolution checks
- transaction handling

Goal:

- separate internal concerns more clearly
- make append path and CDC path easier to read in isolation
- keep correctness logic obvious

### 3. Isolate snapshot spool / replay code

`Runtime` still owns too much of the snapshot spool lifecycle:

- writing spool rows
- reading spool rows
- replay iteration
- cleanup on failure

Goal:

- move spool-specific logic into a focused module
- leave `Runtime` with orchestration, not file-format details

### 4. Keep removing defensive interior polymorphism

We have already removed a lot of atom/string and nil/missing hedging.
Keep pushing that pattern:

- normalize once at the boundary
- trust normalized internal shapes
- delete helper layers that only re-check already-normalized data

### 5. Revisit telemetry forwarder payload shape

We now make truncation explicit and configurable.
Still decide whether the current marker format is the long-term shape we want:

- `__duck_feeder_truncated__`
- `__duck_feeder_original_count__`
- list wrapper structure

Goal:

- either bless the format and document it
- or replace it with a cleaner stable shape

Redundant tests:

- "builds service options from explicit runtime config" and "builds DuckDB service options from explicit runtime config without storage" in runtime_test.exs — the
  second test name still references "without storage" (legacy concept). Both test service_options/4 with nearly identical setups. The "without storage" distinction no
  longer exists.
- "starts streaming runtime stack" and "starts streaming runtime stack without storage when DuckDB is configured" in runtime_test.exs — same thing. Two tests for a
  distinction that was removed.
- "starts an internal DuckDB connection by default" appears in both service_test.exs and append_stream_test.exs — both just assert is_pid(state.context.duckdb.conn)
  and is_pid(state.context.duckdb.server). They're testing DuckDB.Connection.resolve_opts/1 through two different callers. The connection resolution is already
  tested in duckdb/connection_test.exs.
- "context stores designated table mappings" in service_test.exs and "context stores prefixed checkpoint keys" in append_stream_test.exs — both poke at
  state.context via :sys.get_state to verify the context map shape. These test DesignatedTable.by_target/2 through the init path. That function is already tested in
  designated_table_test.exs.

Bad patterns:

- FakeMeta is defined identically in 3 test files (service_test.exs, append_stream_test.exs, sink/duckdb_test.exs). Same def upsert_checkpoint(\_conn,
  \_checkpoint_key, lsn), do: {:ok, lsn}.
- safe_stop/1, temp_duckdb_path/1, and query_duckdb_file/2 are copy-pasted across service_test.exs, append_stream_test.exs, and sink/duckdb_test.exs. These belong
  in a shared test helper.
- runtime_test.exs is 1236 lines with 29 tests, 12 fake modules, and a setup block that cleans up ETS entries by hardcoded source name strings. The fake modules
  (FakeMeta, FakeService, FakeCDC, FakeConnectionOptions, FakeBootstrap, FakeBootstrapCreatedSlot, FakeSnapshotRunner, FakeSnapshotRunnerThreeRows,
  FakeSnapshotRunnerRaises, FakeBootstrapRaises, FakeCDCFailStart) are 250+ lines of test infrastructure to avoid hitting real dependencies. This is testing
  Runtime.start_stream — a 200-line with chain — by mocking every collaborator. The tests are tightly coupled to the internal wiring sequence rather than testing
  observable behavior.
- The sink/duckdb_test.exs tests that exercise CDC operations ("applies CDC batches as table operations", "applies CDC batches after snapshot-created numeric
  columns", "preserves numeric-looking strings...") each set up their own context and run multi-step batch sequences. The CDC operations test is 50+ lines doing
  insert → update/delete → truncate in one test. These are really integration tests wearing unit test clothes — they'd be clearer as separate focused tests or an
  actual integration test against a real pipeline.
- append_stream_test.exs overflow tests ("fails closed when append batch queue overflows", "can drop oldest pending batch when configured") bypass the public API by
  sending raw {:duck_feeder_batch, ...} messages directly to the GenServer and using a setup_fun that sleeps 250ms to create backpressure. Fragile timing-dependent
  tests.
- async: false on sink/duckdb_test.exs — forced serial execution because of the module-level ETS tables (SetupRegistry, SetupConnRegistry,
  AppliedBatchSetupRegistry). The global mutable state in the sink makes these tests non-parallelizable.

## Secondary work

- sharpen docs after code shape settles
- add more focused tests when logic moves into new modules
- keep deleting dead compatibility shims as refactors land

## Rule of thumb

Prefer:

- smaller orchestration functions
- fewer helper layers
- one normalized internal shape
- fail-closed behavior
- explicit data flow over flexible plumbing
