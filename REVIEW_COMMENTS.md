# Review Comments

- `DuckDB.Client.execute/2` now returns `{:ok, result}` tuples and validates conn, fixing the original swallowed-return and nil-conn issues. However, the `rescue` still only catches `ArgumentError` — other Dux exceptions (e.g. `ErlangError`, `RuntimeError` from NIF crashes) will propagate as unhandled raises rather than `{:error, ...}` tuples.

- `DuckDB.Client.query_map/2` now validates conn and handles nil return from `Dux.Backend.query/2`. Fixed.

- `Sink.DuckDB` now fetches target columns via `fetch_target_columns/3` (a single `information_schema.columns` query) and threads the result through, eliminating the separate `relation_exists?` + `describe_columns` two-query pattern. Fixed.

- `Sink.DuckDB.ensure_additive_columns/5` now calls `validate_sql_type/1` on each type before interpolating into `ALTER TABLE` SQL. The regex `@sql_type_pattern` (`~r/\A[A-Z][A-Z0-9_ ]*(\([0-9 ,]+\))?\z/`) is a reasonable allowlist. Fixed.

- `Sink.DuckDB.sql_literal/2` now validates the type via `validate_sql_type!/1` before interpolating into `CAST(... AS ...)`. Fixed.

- `Sink.DuckDB.fetch_target_columns/3` (was `relation_exists?`) now uses `quote_string/1` (which goes through `escape_sql_string`) for schema/table/catalog values in `WHERE` clauses instead of bare `escape_sql_string` inside single quotes. `escape_sql_string/1` now also escapes backslashes and rejects null bytes. Fixed.

- `Sink.DuckDB` no longer has a separate `describe_columns` that silently returns `[]` on error. The `fetch_target_columns/3` function returns `{:ok, nil}` for non-existent tables and `{:ok, columns}` for existing ones, and errors propagate. Fixed.

- `Sink.DuckDB.normalize_cdc_value/1` (the function that coerced string `"true"`/`"false"` to booleans and parsed numeric-looking strings) has been removed. `normalize_cdc_row_map` now only stringifies keys without altering values. Fixed.

- `Sink.DuckDB.base_sql_literal/1` now uses `encode_json!/1` which wraps `JSON.encode!/1` with a rescue for `UndefinedFunctionError` that gives a clear error message about requiring Elixir 1.19+. The `jason` optional dep has been removed from `mix.exs`. Fixed.

- `Sink.DuckDB.rows_source/1` now chunks rows via `rows_sources/2` using `@rows_source_chunk_size` (500), producing multiple source statements that are iterated with `append_sources/4` or `Enum.reduce_while`. Fixed.

- `Sink.DuckDB.infer_columns/2` has been rewritten to a single-pass `Enum.reduce` over rows, accumulating column kinds in a map. Also accepts `type_overrides` from existing target columns to preserve types. Fixed.

- `Sink.DuckDB.with_transaction/2` now wraps the function body in `try/rescue/catch` that rolls back on any exception or throw before reraising. Fixed.

- `Sink.DuckDB.process_batch/3` now records an `applied_batches` entry inside the DuckDB transaction (via `ensure_applied_batch_table`, `record_applied_batch`, `batch_already_applied?`). On restart, if the Postgres checkpoint was not persisted but DuckDB was committed, the batch is detected as already applied and skipped (deduped). This addresses the durability rule violation. Fixed.

- `Sink.DuckDB.ensure_setup/2` now monitors the connection pid via `ensure_setup_conn_monitor/1`. When the conn process dies, a watcher process clears the stale ETS entries via `clear_setup_entries/1`. Fixed.

- `Sink.DuckDB.duckdb_conn/1` no longer falls back to a global `DuckDBConnection.get_conn()` — it returns `{:error, :missing_duckdb_conn}` when no `:conn` is provided. Fixed.

- `StreamSupport.maybe_start_duckdb_connection/2` still calls `DuckDBConnection.start_link` without linking the started `Dux.Connection` GenServer to a supervisor. The process will be orphaned if the caller crashes. The test `test "resolve_duckdb links started duckdb connections to the caller process"` expects the conn to die when the caller is killed, but `DuckDBConnection.start_link` uses `GenServer.start_link` which links to the calling process — so the test passes because of the implicit link from `start_link`, not because an explicit supervisor manages it. This is fragile: if the calling process is a GenServer that traps exits, the connection won't be cleaned up. **Not fully fixed.**

- `StreamSupport.designated_table_config_mapping/1` now calls `DesignatedTable.normalize/1` on each table and uses `DesignatedTable.target_relation/1` instead of `Map.fetch!`. `DesignatedTable.normalize/1` converts known string keys to atom keys. Fixed.

- `Runtime.build_runtime_source/2` now preserves all source map keys by starting with `source |> Map.put(:name, ...)` instead of building a new map from scratch. The test `build_runtime_source_test.exs` verifies that extra keys like `:designated_tables` and `:custom` survive. Fixed.

- `Runtime` snapshot handoff functions now use `snapshot_handoff_source_key/2` which does `Map.get(source, :id)` and falls through case-by-case (string, integer, or fallback to `source_name`). This is clearer than the old `Map.get(source, :id, source_name)` default-arg pattern, but the config-first model still never sets `:id` on source maps, so the string/integer branches are still dead code in practice. **Improved but still dead code.**

- `Runtime.Shared.fetch_duckdb!/1` now pattern matches both keys in one case expression and raises an `ArgumentError` with a helpful message listing available keys. Fixed.

- `Runtime.Supervisor` now uses `maybe_put_duckdb_opt/3` which only passes `:duckdb` to children if the parent opts actually contained `:duckdb` or `:duckdb_config`. This avoids injecting `duckdb: nil` when neither was provided. Fixed.

- `Runtime.Manager.stop_source/2` now calls `GenServer.stop(pid)` on the source supervisor before calling `drop_source/2`. The test now asserts `refute Process.alive?(source_a_pid)`. Fixed.

- `Config` now uses `designated_tables: [type: {:list, :any}, default: []]` instead of `{:list, :keyword_list}`, allowing map-shaped entries. The `validate_designated_tables` step still converts each entry via `to_keyword` which handles both maps and keyword lists. A new test verifies string-key maps pass validation. Fixed.

- `Meta.Store.fetch_start_lsn/3` now returns `matched_count` alongside `min_lsn` and compares against expected count. If any checkpoint key is missing, it falls back to `default_lsn`. A new test `"fetch_start_lsn falls back when any checkpoint is missing"` verifies this. Fixed.

- `Meta.Store.lsn_param/1` now uses `Lsn.parse/1` (the non-bang version) and returns `{:error, ...}` tuples instead of raising. A new test verifies invalid LSN values return error tuples. Fixed.

- `AppendStream` and `Service` batch queue handling has been extracted into `BatchDispatch` module. Both `Service` and `AppendStream` now delegate to `BatchDispatch.handle_incoming_batch/4`, `handle_batch_result/4`, and `handle_batch_down/4` with callbacks. Fixed.

- `mix.exs` `package.files` now includes `"priv"`. Fixed.

- CI workflow now uses a matrix strategy testing `{1.19.0, OTP 27.3}` and `{1.19.0, OTP 28.0}`. Fixed.

- `DuckDB.Client` now validates conn is a pid and alive before calling Dux. Fixed.

- `Sink.DuckDB.compatible_type?/2` now handles wider integer promotions (`BIGINT` accepts `INTEGER`/`SMALLINT`/`TINYINT`, etc.) and float family promotions (`DOUBLE`/`REAL`/`FLOAT` accept integer types). Fixed.
