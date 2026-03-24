# Troubleshooting

## Missing or invalid wrapper config

Typical startup errors:

- `{:missing_or_invalid_option, :repo, ...}`
- `{:missing_or_invalid_option, :schemas, ...}`
- `{:missing_or_invalid_option, :duckdb, ...}`

Check that your runtime module config includes:

```elixir
config :my_app, MyApp.DuckFeeder,
  repo: MyApp.Repo,
  schemas: [MyApp.Users],
  duckdb: %{path: "/var/lib/my_app/analytics.duckdb"}
```

## Missing primary keys

Update and delete handling requires primary keys.

If DuckFeeder cannot infer them correctly from your Ecto schema, override them explicitly in the schema entry.

```elixir
config :my_app, MyApp.DuckFeeder,
  repo: MyApp.Repo,
  schemas: [
    {MyApp.Users, primary_keys: ["id"]}
  ],
  duckdb: %{path: "/var/lib/my_app/analytics.duckdb"}
```

## Snapshot handoff is pending

A crash or failed startup during initial snapshot can leave a row in `duckfeeder_meta.snapshot_handoffs` with state `pending`.

Inspect it with:

```sql
SELECT *
FROM duckfeeder_meta.snapshot_handoffs
ORDER BY source_name;
```

The wrapper runtime defaults to resuming incomplete snapshot handoffs.

If you are using lower-level startup APIs directly, pass `resume_incomplete_snapshot?: true` together with `snapshot_before_stream?: true` if you want DuckFeeder to retry safely.

## Checkpoint looks stale

Inspect checkpoint state:

```sql
SELECT checkpoint_key, last_committed_lsn, updated_at
FROM duckfeeder_meta.checkpoints
ORDER BY checkpoint_key;
```

If the checkpoint is not advancing:

- check whether the service is failing on a DuckDB write
- check whether schema evolution failed closed
- check whether the metadata Postgres connection is healthy
- check whether backpressure or overload caused CDC disconnects

Remember: WAL ACK will not advance before DuckDB commit plus checkpoint persistence.

Note: if a checkpoint write to Postgres fails after the DuckDB write succeeded, the batch is tracked inside DuckDB (`duck_feeder_internal.applied_batches`) so it will be skipped on the next retry. You can inspect this with:

```sql
SELECT * FROM duck_feeder_internal.applied_batches;
```

## Incompatible schema change

DuckFeeder handles additive columns automatically, but destructive or ambiguous changes fail closed.

During CDC merges, DuckFeeder respects existing target column types — incoming CDC values are cast to the target type rather than being re-inferred from the raw WAL values. Type compatibility follows widening rules (e.g. `INTEGER` column accepts `SMALLINT` values; `VARCHAR` accepts anything). Incompatible narrowing changes will produce an error.

If a source column changes type incompatibly, expect startup or batch processing to stop loudly instead of guessing.

Recommended response:

1. inspect the source schema change
2. inspect the current DuckDB target table
3. decide whether to migrate/rebuild the target table
4. restart DuckFeeder after the schema is safe again

## Unknown append target table

For append streams, this error usually means the target table was not declared in `designated_tables`.

Example valid setup:

```elixir
{:ok, stream} =
  DuckFeeder.start_append_stream(
    designated_tables: [
      %{target_schema: "raw", target_table: "app_events"}
    ],
    meta_conn: meta_conn,
    duckdb: %{path: "/var/lib/my_app/analytics.duckdb"}
  )
```

Then append using `"app_events"` or `{"raw", "app_events"}`.

## Inspecting the DuckDB file locally

```bash
duckdb /var/lib/my_app/analytics.duckdb
```

Useful queries:

```sql
SHOW SCHEMAS;
SHOW TABLES;
SELECT * FROM raw.users LIMIT 10;
SELECT * FROM raw.app_events ORDER BY at DESC LIMIT 20;
```
