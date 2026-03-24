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

The immediate goal is to verify DuckFeeder works cleanly with DuckLake configured the way DuckDB expects, without reintroducing a DuckFeeder-specific storage layer.

## Highest-priority next steps

### 1. Move DuckDB access back behind Dux

Before expanding the integration matrix further, switch DuckFeeder from using raw `adbc` directly to using `dux ~> 0.2` as the primary DuckDB access layer.

Goals:

- follow the intended DuckDB access abstraction for this project
- stop scattering raw ADBC concerns through the runtime and tests
- keep setup and connection handling closer to the library we actually want to expose/support
- make the upcoming DuckLake integration work reflect the intended stack

This should be treated as the top priority before the broader DuckLake integration push.

### 2. Build a real DuckLake integration matrix

This is the biggest gap after the Dux cleanup.

We need env-gated end-to-end integration tests that exercise DuckFeeder with DuckLake storing actual data locally on the filesystem. These tests should remain opt-in for normal local development runs.

Target configurations:

1. **DuckLake metadata in DuckDB**
   - DuckLake catalog/metadata managed from DuckDB
   - DuckLake data files stored locally on disk
2. **DuckLake metadata in Postgres**
   - DuckLake catalog/metadata managed from Postgres
   - DuckLake data files stored locally on disk

For each configuration, verify at minimum:

- runtime boot/setup succeeds
- snapshot loads initial rows correctly
- CDC inserts work
- CDC updates work
- CDC deletes work
- truncates behave correctly
- additive columns are handled correctly
- schema-change behavior is explicitly covered, starting with additive columns and the relation/update flow around them
- append streams can write into the same database/catalog setup
- checkpoints persist correctly
- restart/resume from checkpoints works
- snapshot handoff recovery works
- resulting tables are queryable directly from DuckDB

Also add a third integration path focused on **append-only telemetry ingestion without CDC**.
A logger-backed or telemetry-backed append stream is a good starting point. The goal is to verify that a real append-only operational data flow works cleanly with the same local DuckLake setups.

These tests should use real temporary directories and real database state, not just mocks.
Prefer one explicit env gate for the suite such as `RUN_INTEGRATION_TESTS=1`.
There is already basic opt-in integration coverage for metadata roundtrips and CDC connection streaming; extend that foundation rather than creating a separate test style.

### 3. Define the recommended DuckLake default

We should lean heavily on how DuckDB recommends using DuckLake.

That means:

- prefer DuckDB/DuckLake-native setup patterns over DuckFeeder-specific abstractions
- keep DuckFeeder's `duckdb` config small
- use `setup_sql` / `setup_fun` for catalog/storage initialization where possible
- avoid inventing extra storage concepts unless DuckDB itself requires them

Questions to answer explicitly:

- Which local DuckLake setup should be the **golden path**?
- Should the default docs prefer DuckLake metadata in DuckDB or Postgres?
- What exact `setup_sql` should we recommend for local filesystem-backed DuckLake use?
- What setup should we call the supported default for local development?

### 4. Refresh docs around the tested DuckLake path

Once the integration matrix is real, update docs to match what we actually verified.

Docs should clearly explain:

- the difference between DuckFeeder metadata and DuckLake metadata
- the recommended local filesystem-backed DuckLake setup
- the alternate Postgres-backed DuckLake metadata setup
- how to inspect/query the resulting local DuckDB/DuckLake state
- how restart/checkpoint behavior works in those setups

README should show the recommended default, not a speculative one.

### 5. Keep improving sink scalability and correctness

After the integration matrix is in place, continue on the known sink work:

- replace giant `VALUES` SQL generation with a better bulk-write path
- reduce `infer_columns/1` overhead
- keep auditing dynamic SQL construction/validation
- add more large-batch correctness coverage

## Nice-to-have follow-ons

After the items above:

- sharpen append-stream restart semantics/docs
- improve schema evolution coverage matrix
- keep polishing startup validation and error messages
- add copy-pasteable examples for the tested DuckLake setups

## Relevant files for the next session

Start here:

- `README.md`
- `AGENTS.md`
- `docs/plan.md`
- `docs/runtime.md`
- `lib/duck_feeder/runtime.ex`
- `lib/duck_feeder/runtime/embedded.ex`
- `lib/duck_feeder/bootstrap.ex`
- `lib/duck_feeder/config.ex`
- `lib/duck_feeder/sink/duckdb.ex`
- `lib/duck_feeder/duckdb/connection.ex`
- `lib/duck_feeder/meta/store.ex`
- `test/test_helper.exs`
- `test/duck_feeder/runtime_test.exs`
- `test/duck_feeder/service_test.exs`
- `test/duck_feeder/append_stream_test.exs`
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
