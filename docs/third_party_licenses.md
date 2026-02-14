# Third-Party License Notes

## ElectricSQL LSN Postgrex extension reference (Apache-2.0)

DuckFeeder's Postgrex `pg_lsn` extension implementation was developed with reference to
ElectricSQL's Apache-2.0 licensed implementation.

- Upstream project: https://github.com/electric-sql/electric
- Upstream file used as reference:
  - `packages/sync-service/lib/pg_interop/postgrex/extensions/pg_lsn.ex`
- Local implementation:
  - `lib/duck_feeder/postgrex/extensions/pg_lsn.ex`

### Apache-2.0 requirements applied here

For copied/adapted source under Apache-2.0, distribution should:

1. Preserve license and attribution notices in source headers/comments.
2. Include a copy of the Apache-2.0 license text.
3. Clearly mark local modifications from upstream.
4. Include upstream NOTICE content if present (ElectricSQL does not currently ship a top-level NOTICE file).

### Local compliance actions

- Added source attribution + modification note in `lib/duck_feeder/postgrex/extensions/pg_lsn.ex`.
- Vendored Apache-2.0 text at `third_party/electric/LICENSE`.
