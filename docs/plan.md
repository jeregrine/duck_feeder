# DuckFeeder plan

`README.md` and `AGENTS.md` describe the product and architecture.
This file is only the next concrete cleanup list.

## Next slices

### 1. Split `Runtime.start_stream/4` into smaller phases

Target files:
- `lib/duck_feeder/runtime.ex`
- `test/duck_feeder/runtime_test.exs`

End state:
- one phase for config + LSN + snapshot planning
- one phase for service startup + snapshot replay + CDC startup
- one cleanup path for partial startup failure
- fewer deeply nested `with` chains

### 2. Move snapshot spool lifecycle fully behind `SnapshotSpool`

Target files:
- `lib/duck_feeder/runtime.ex`
- `lib/duck_feeder/runtime/snapshot_spool.ex`
- `test/duck_feeder/runtime/snapshot_spool_test.exs`

End state:
- `Runtime` asks for spool creation, replay iteration, and cleanup
- spool file format and row IO stay out of `Runtime`
- snapshot replay failure cleanup is tested in the spool module

### 3. Break `Sink.DuckDB` into clearer internal pieces

Target files:
- `lib/duck_feeder/sink/duckdb.ex`
- `test/duck_feeder/sink/duckdb_test.exs`

End state:
- keep `process_batch/3`, transaction handling, and applied-batch dedupe at the top level
- extract append-row SQL/source building into one focused helper/module
- extract CDC row staging and apply logic into one focused helper/module
- make append and CDC paths readable without scrolling through unrelated code

### 4. Shrink `runtime_test.exs`

Target files:
- `test/duck_feeder/runtime_test.exs`
- `test_support/*` if shared fakes/helpers are needed

End state:
- remove duplicate `service_options/4` and `start_stream/4` cases
- move reusable fake modules/helpers out of the test file
- stop hardcoding source-name cleanup entries inline in `setup`
- keep tests focused on observable runtime behavior, not wiring trivia

## Rule

Prefer:
- smaller orchestration functions
- one normalized internal shape
- fail-closed behavior
- focused tests around concrete boundaries
