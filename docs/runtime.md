# Runtime setup

DuckFeeder has two ways to start a mirror runtime:

1. the recommended app-facing wrapper built from `repo`, `schemas`, and `duckdb`
2. the lower-level explicit engine config passed to validation/bootstrap APIs

The recommended path is the wrapper.

## Recommended wrapper: `use DuckFeeder.Runtime`

```elixir
config :my_app, MyApp.DuckFeeder,
  enabled: true,
  repo: MyApp.Repo,
  schemas: [
    MyApp.Users,
    MyApp.Orders,
    {MyApp.Invoices, target_schema: "raw", target_table: "invoice_events"}
  ],
  duckdb: %{
    path: "/var/lib/my_app/analytics.duckdb"
  }
```

```elixir
defmodule MyApp.DuckFeeder do
  use DuckFeeder.Runtime, otp_app: :my_app
end
```

## What DuckFeeder infers from Ecto schemas

For each schema entry, DuckFeeder resolves:

- source table name from `__schema__(:source)`
- source schema from `__schema__(:prefix)` or `"public"`
- primary keys from `__schema__(:primary_key)`
- default target schema as `"raw"`
- default target table as the source table name

Per-schema overrides are supported:

```elixir
config :my_app, MyApp.DuckFeeder,
  repo: MyApp.Repo,
  schemas: [
    MyApp.Users,
    {MyApp.Orders, target_schema: "analytics", target_table: "orders"},
    {MyApp.LegacyAudit, enabled?: false}
  ],
  duckdb: %{path: "/var/lib/my_app/analytics.duckdb"}
```

## Wrapper defaults

The wrapper is opinionated toward the intended golden path.

By default it:

- derives slot/publication names from `source_name`
- creates the `duckfeeder_meta` schema if needed
- runs an initial snapshot before replication on first start
- resumes an incomplete snapshot handoff on restart
- does **not** rerun a full snapshot after a healthy start unless you opt in

If you need to customize runtime startup behavior, pass `runtime_opts` in config:

```elixir
config :my_app, MyApp.DuckFeeder,
  repo: MyApp.Repo,
  schemas: [MyApp.Users],
  duckdb: %{path: "/var/lib/my_app/analytics.duckdb"},
  runtime_opts: [
    snapshot_on_restart?: true,
    reconnect_backoff: 1_000,
    max_lag_bytes: 50_000_000
  ]
```

## Separate metadata Postgres

By default, the metadata store uses the same repo config as the source database.

If you want metadata in a separate Postgres database, use `metadata_repo`, `metadata_postgres_url`, or `source_postgres_url` / `metadata_postgres_url` explicitly.

```elixir
config :my_app, MyApp.DuckFeeder,
  repo: MyApp.Repo,
  metadata_repo: MyApp.DuckFeederMetaRepo,
  schemas: [MyApp.Users],
  duckdb: %{path: "/var/lib/my_app/analytics.duckdb"}
```

## Explicit engine config

If you do not want the wrapper, DuckFeeder can validate an explicit config shape:

```elixir
config = %{
  source: %{
    postgres_url: "postgres://...",
    slot_name: "duck_feeder_default_slot",
    publication_name: "duck_feeder_default_pub",
    designated_tables: [
      %{
        source_schema: "public",
        source_table: "users",
        target_schema: "raw",
        target_table: "users",
        primary_keys: ["id"]
      }
    ]
  },
  duckdb: %{
    path: "/var/lib/my_app/analytics.duckdb"
  },
  metadata: %{
    postgres_url: "postgres://..."
  }
}

{:ok, validated} = DuckFeeder.validate_config(config)
```

This path keeps startup decisions explicit.

Notably, `DuckFeeder.start_stream/4` does not assume you want an initial snapshot unless you pass runtime options like `snapshot_before_stream?: true`.

## What lives in Postgres metadata

DuckFeeder keeps only the durable state needed for restart correctness in Postgres.

Tables created under `duckfeeder_meta`:

- `checkpoints`
- `snapshot_handoffs`
- `migration_versions`

That means:

- table selection does **not** live in Postgres metadata
- target naming does **not** live in Postgres metadata
- primary keys do **not** live in Postgres metadata

Those belong in app config and code.

## Inspecting runtime state

Useful Postgres queries:

```sql
SELECT checkpoint_key, last_committed_lsn, updated_at
FROM duckfeeder_meta.checkpoints
ORDER BY checkpoint_key;

SELECT source_name, state, boundary_lsn, started_at, completed_at, updated_at
FROM duckfeeder_meta.snapshot_handoffs
ORDER BY source_name;
```

Useful DuckDB queries:

```bash
duckdb /var/lib/my_app/analytics.duckdb
```

```sql
SHOW SCHEMAS;
SHOW TABLES;
SELECT * FROM raw.users LIMIT 10;
```

## Durability model

The runtime is built around one invariant:

- **WAL ACK advances only after DuckDB writes are committed and the checkpoint is durably persisted.**

That is the reason checkpoints live in Postgres and why snapshot handoff state is persisted there too.

If a Postgres checkpoint write fails after a successful DuckDB commit, the batch is tracked inside DuckDB (in `duck_feeder_internal.applied_batches`) so it will be detected and skipped on the next attempt instead of being written twice.

## Schema evolution

Current behavior:

- additive columns are added to the DuckDB target table automatically
- existing target column types are respected during CDC merges (incoming values are cast to the target type rather than re-inferred)
- destructive or ambiguous type changes fail closed
- missing primary keys cause loud failures for update/delete semantics
- type compatibility is checked with widening rules (e.g. `INTEGER` → `BIGINT` is safe, `VARCHAR` accepts anything)
- type names are validated against a strict allowlist before being interpolated into SQL

When a change cannot be applied safely, DuckFeeder should stop loudly instead of guessing.
