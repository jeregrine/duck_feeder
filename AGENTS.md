# AGENTS.md

## Project Overview
DuckFeeder is an Elixir runtime for:

- **Postgres logical replication (WAL/CDC)**
- **Parquet file writing** (Rust NIF, precompiled binaries)
- **Object storage upload** (S3/GCS via Req)
- **DuckLake metadata commits** (Postgres-backed)
- Query target is DuckDB via DuckLake metadata + Parquet files.

Core durability rule: **WAL ACK advances only after durable checkpoint/metadata commit**.

## Current Runtime/Product Decisions
- Elixir-first architecture, OTP-supervised runtime.
- HTTP/storage stack is **Req-only**.
- CDC path is fail-closed under sustained overload; append stream can optionally use lossy overflow policy.
- Telemetry helper shipped: `DuckFeeder.TelemetryForwarder`.
- Pure BEAM parquet writer spike exists on branch `parquet-elixir-spike` and is **not merged**.

## VCS / Workflow Conventions
- Use **jj** for local commit/bookmark flow.
- Main branch is authoritative for release workflow config.
- Prefer version bumps for releases; avoid retagging old versions unless explicitly needed.

## Rustler / Precompiled NIF Critical Notes
The most common failure mode is mismatch between:
1) RustlerPrecompiled `targets`/`nif_versions` in Elixir,
2) workflow build matrix,
3) release assets,
4) checksum file.

### Current expected NIF setup
- NIF version: **2.17**
- Rust crate: `native/duck_feeder_parquet`
- Module: `DuckFeeder.Writer.ParquetNif`
- Uses `RustlerPrecompiled` with explicit `targets` (including FreeBSD, excluding musl).

### Important
`RustlerPrecompiled` defaults include musl targets. If `targets:` is not explicit, it will try to download musl artifacts and fail.

## CI / Release Workflow
Workflow: `.github/workflows/build_precompiled_nifs.yml`

- Builds mainstream targets with `philss/rustler-precompiled-action`.
- Builds FreeBSD via `vmactions/freebsd-vm` (native build in VM).
- Publishes assets on `v*` tag pushes.
- Tag/version consistency check is enabled (`vX.Y.Z` must match `mix.exs` version).

## Asset Naming Expectations
Release files must match RustlerPrecompiled naming, e.g.:
- `libduck_feeder_parquet-v<version>-nif-2.17-<target>.so.tar.gz`
- `duck_feeder_parquet-v<version>-nif-2.17-<windows-target>.dll.tar.gz`

## Checksum File
Required for precompiled downloads:
- `checksum-Elixir.DuckFeeder.Writer.ParquetNif.exs`

Regenerate/update after assets change:

```bash
RUSTLER_PRECOMPILED_FORCE_BUILD_ALL=true mix rustler_precompiled.download DuckFeeder.Writer.ParquetNif --all --print
```

This both validates downloadable artifacts and updates checksum entries.

## Hex Publish Notes
Before publish:
- Ensure `mix.exs` package `files` all exist (license file included).
- Ensure checksum file matches release assets.
- Ensure README badges/links/docs metadata are sane.

Publish commands:
```bash
mix hex.publish
mix hex.publish docs
```
Non-interactive (CI/headless) requires `HEX_API_KEY`.

## Test Notes
- Integration tests are env-gated; default test run should stay stable.
- Keep credentials out of committed config.

## Operational Notes
- Queue/lag telemetry exists and is important for alerting/backpressure tuning.
- Remaining workstreams: provider failure/reconcile matrix expansion + alert policy tuning.
