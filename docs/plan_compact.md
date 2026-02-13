# DuckFeeder Compact Plan

## Goal
Postgres CDC -> Parquet -> Object Storage (S3 or GCS) -> DuckLake metadata in Postgres.

## Core choices
- Elixir owns CDC, batching, retries, checkpoints, and DuckLake commits.
- Minimal Rustler NIF only for Parquet encoding/stats.
- Storage via semi-generic adapter:
  - `DuckFeeder.Storage.S3`
  - `DuckFeeder.Storage.GCS`

## Interface
- `DuckFeeder.put_file/4`
- `DuckFeeder.head_object/2`
- `DuckFeeder.delete_object/2`

Config uses `provider: :s3 | :gcs`, `bucket`, optional `prefix`, and provider-specific auth/options.

## Exactly-once boundary
- Upload parquet files
- Commit DuckLake metadata in one Postgres tx
- Advance checkpoint LSN only after commit succeeds

## Build order
1. CDC connection + decoder + txn boundaries
2. Table pipelines + batch buffer
3. Parquet NIF wrapper
4. DuckLake SQL committer + checkpoints
5. Recovery/reconciler + integration tests
