# Running DuckFeeder inside an existing Elixir application's supervision tree

This runbook shows recommended patterns for embedding DuckFeeder into an existing app.

## Preferred topology

Use one top-level DuckFeeder runtime supervisor per source:

- `DuckFeeder.Runtime.Supervisor`
  - `DuckFeeder.Runtime.StreamWorker` (service + replication stream)
  - optional `DuckFeeder.Reconciler.Worker`

This gives predictable restart behavior and clean ownership boundaries.

## Example `Application.start/2`

```elixir
def start(_type, _args) do
  children = [
    # your app deps (Repo, telemetry, etc)
    {Postgrex, [name: MyApp.DuckFeederMetaConn, url: System.fetch_env!("DUCK_FEEDER_META_DATABASE_URL")]},

    {DuckFeeder.Runtime.Supervisor,
     [
       name: MyApp.DuckFeederRuntime,
       meta_conn: MyApp.DuckFeederMetaConn,
       source_name: "primary",
       storage_config: %{provider: :s3, bucket: "ducklake-data", access_key_id: "...", secret_access_key: "..."},
       runtime_opts: [
         writer: %{format: :jsonl},
         # committer_module: DuckFeeder.DuckLake.Committer.Postgres,
         # committer_opts: [...]
         bootstrap_replication?: true
       ],
       start_reconciler?: true,
       reconcile_opts: [verify_uploaded_objects?: true, cleanup_failed_uploads?: true]
     ]}
  ]

  Supervisor.start_link(children, strategy: :one_for_one, name: MyApp.Supervisor)
end
```

## Startup sequence checklist

1. Start metadata connection (`Postgrex` or your existing DB wrapper).
2. Ensure `duckfeeder_meta` schema is bootstrapped (`DuckFeeder.Meta.bootstrap/1` or migration flow).
3. Ensure source + designated table rows exist (`DuckFeeder.seed_meta/3` or pre-seeded).
4. Start `DuckFeeder.Runtime.Supervisor`.

## Shutdown strategy

- Keep `DuckFeeder.Runtime.Supervisor` as a normal worker child under your app supervisor.
- Allow OTP to stop children in reverse order.
- `DuckFeeder.Runtime.StreamWorker` handles stream/service child termination in `terminate/2`.

## Multi-source pattern

Two options:

1. Static source set: one `DuckFeeder.Runtime.Supervisor` child per source name.
2. Dynamic source set: run `DuckFeeder.Runtime.Manager` and start/stop sources at runtime.

Static example:

```elixir
{DuckFeeder.Runtime.Supervisor, name: MyApp.DuckFeeder.SourceA, source_name: "source_a", ...}
{DuckFeeder.Runtime.Supervisor, name: MyApp.DuckFeeder.SourceB, source_name: "source_b", ...}
```

Dynamic example:

```elixir
{DuckFeeder.Runtime.Manager,
 [
   name: MyApp.DuckFeeder.Manager,
   meta_conn: MyApp.DuckFeederMetaConn,
   storage_config: storage_config,
   base_opts: [start_reconciler?: true]
 ]}

# later
DuckFeeder.start_source_runtime(MyApp.DuckFeeder.Manager, "source_a")
DuckFeeder.stop_source_runtime(MyApp.DuckFeeder.Manager, "source_a")
```

## Common pitfalls

- Missing metadata source rows (`source_name` not found).
- Source connection info not populated (`connection_info` lacks DSN/URL/host+database).
- Starting runtime before meta bootstrap/registration.
- Not providing storage config required for reconciler verification/cleanup flows.
