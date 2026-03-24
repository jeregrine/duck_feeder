# DuckFeeder plan

This document is intentionally short.

README and `AGENTS.md` already cover the product story, architecture, and guiding principles.
This file should stay focused on the next concrete work items for this branch.

## Current branch direction

Keep optimizing for:

- config-first runtime setup
- real DuckDB-managed target tables
- minimal DuckFeeder metadata in Postgres
- strong restart/checkpoint correctness
- simple defaults and copy-pasteable setup

Most important invariant:

- **WAL ACK advances only after DuckDB table writes are committed and the checkpoint is durably persisted.**

## Important terminology

There are two different metadata concerns here and we should keep them separate:

### DuckFeeder metadata

This remains the durable runtime metadata owned by DuckFeeder and stored in Postgres:

- `checkpoints`
- `snapshot_handoffs`
- `migration_versions`

### DuckLake metadata

This is the catalog/storage metadata managed by DuckDB/DuckLake.

The goal is to verify DuckFeeder works cleanly with DuckLake configured the way DuckDB expects, without reintroducing a DuckFeeder-specific storage layer.

## Recently completed on this branch

- moved DuckDB access behind `dux ~> 0.2`
- added env-gated DuckLake integration coverage for append ingestion
  - DuckLake metadata in DuckDB
  - DuckLake metadata in Postgres
- added env-gated embedded runtime integration coverage using real `Ecto.Repo` + `Ecto.Schema`
  - DuckLake metadata in DuckDB
  - DuckLake metadata in Postgres
- added snapshot handoff recovery coverage on the embedded runtime path
- fixed sink issues found by the new integration tests
  - append checkpoint-key fallback
  - idempotent DuckLake attach setup in tests
  - snapshot-created numeric columns followed by CDC string values
- hardened `Sink.DuckDB` SQL construction:
  - `validate_sql_type/1` allowlist for type names interpolated into SQL
  - `escape_sql_string/1` now escapes backslashes and rejects null bytes
  - `fetch_target_columns/3` replaced separate `relation_exists?` + `describe_columns` with single query
  - `infer_columns/2` rewritten as single-pass with target column type overrides
  - `rows_sources/2` chunks large batches instead of building one giant `VALUES` clause
- added DuckDB-side batch dedup tracking (`duck_feeder_internal.applied_batches`) so retries after failed Postgres checkpoint writes are skipped
- added `with_transaction` try/rescue/catch cleanup so DuckDB connection is always rolled back on crash
- removed implicit global DuckDB connection fallback — `duckdb.conn` is now required
- added ETS setup invalidation on connection death via monitor/watcher
- removed `normalize_cdc_value` type coercion — CDC values are now passed through without string-to-integer/boolean guessing
- extracted shared batch dispatch logic into `BatchDispatch` module
- fixed `Runtime.Manager.stop_source/2` to actually stop the source supervisor process
- fixed `Meta.Store.fetch_start_lsn/3` to fall back to default when any checkpoint key is missing
- fixed `Meta.Store.lsn_param/1` to return error tuples instead of raising
- fixed `Config` to accept map-shaped designated tables with string keys
- fixed `Runtime.Shared.fetch_duckdb!/1` to raise a helpful error message
- fixed `Runtime.Supervisor` to avoid injecting `duckdb: nil` when neither key was provided
- fixed `StreamSupport.designated_table_config_mapping/1` to normalize tables before extracting target relations
- added `DesignatedTable.normalize/1` for consistent atom-key handling of string-keyed maps
- added `priv` to hex package `files`
- removed `jason` optional dependency (uses Elixir 1.19+ built-in `JSON`)
- added CI matrix testing against OTP 27 and OTP 28
- widened `compatible_type?/2` to handle integer/float promotion paths

## Highest-priority next steps

### 1. Refresh docs around the tested DuckLake path

The biggest gap now is docs accuracy.

Update README and `docs/runtime.md` to match what we actually verified:

- explain DuckFeeder metadata vs DuckLake metadata clearly
- show the tested DuckDB-backed DuckLake setup
- show the tested Postgres-backed DuckLake setup
- document the exact `duckdb` config shape we now use (`setup_sql` + `setup_fun`)
- explain when `ducklake_flush_inlined_data()` matters for local filesystem inspection
- show how to inspect/query resulting tables and metadata

README should show the recommended default, not a speculative one.

### 2. Define the recommended local default explicitly

We now have enough tested coverage to pick a real default.

Questions to answer explicitly:

- should the golden path for local development be DuckLake metadata in DuckDB?
- should Postgres-backed DuckLake metadata be documented as the multi-client / shared-catalog alternate?
- what exact config snippet should users copy first?
- what should we call the supported local development setup?

The likely shape should follow DuckLake's own guidance closely.

### 3. Close the remaining integration-matrix gaps

The matrix is much better now, but not complete yet.

Still add coverage for:

- snapshot handoff recovery on the DuckDB-backed DuckLake metadata path
- a more operational append-only path (telemetry/logger-backed flow, not just direct append calls)
- broader restart/resume assertions after live CDC on both metadata backends
- more explicit filesystem assertions around DuckLake data files after flush/materialization

### 4. Broaden schema evolution coverage carefully

We now cover additive-column behavior on the embedded runtime path.

Next steps here:

- make sure additive-column coverage exists on both DuckLake metadata backends
- add relation/update sequencing assertions around schema changes
- document which schema changes are supported automatically vs fail closed

### 5. Keep improving sink scalability and correctness

After docs + matrix cleanup:

- consider parameterized query paths for DuckDB writes instead of string interpolation
- keep auditing dynamic SQL construction/validation
- add more large-batch correctness coverage
- consider whether the `applied_batches` dedup table needs periodic cleanup/compaction

## Nice-to-have follow-ons

After the items above:

- sharpen append-stream restart semantics/docs
- improve startup validation and error messages
- add copy-pasteable examples for the tested DuckLake setups
- consider a small helper/story for the tested DuckLake attach defaults if the config still feels too manual

## Relevant files for the next session

Start here:

- `README.md`
- `AGENTS.md`
- `docs/plan.md`
- `docs/runtime.md`
- `lib/duck_feeder/runtime.ex`
- `lib/duck_feeder/runtime/embedded.ex`
- `lib/duck_feeder/sink/duckdb.ex`
- `lib/duck_feeder/duckdb/connection.ex`
- `test_support/integration_helpers.ex`
- `test/duck_feeder/ducklake/append_integration_test.exs`
- `test/duck_feeder/runtime/embedded_ducklake_integration_test.exs`
- `test/duck_feeder/runtime/embedded_ducklake_duckdb_metadata_integration_test.exs`
- `test/duck_feeder/sink/duckdb_test.exs`
- `test/duck_feeder/cdc/connection_integration_test.exs`
- `test/duck_feeder/meta/store_integration_test.exs`

## Decision rules

When in doubt, prefer:

- simpler runtime shape
- config-first setup
- minimal DuckFeeder metadata
- DuckDB/DuckLake-native defaults
- real end-to-end verification
- loud failures over silent fallback
- beautiful DevUX
