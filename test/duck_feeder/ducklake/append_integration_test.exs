defmodule DuckFeeder.DuckLake.AppendIntegrationTest do
  use ExUnit.Case, async: false

  import DuckFeeder.TestSupport.IntegrationHelpers

  alias DuckFeeder.Meta

  @moduletag :integration

  setup_all do
    meta_conn =
      meta_database_url!()
      |> start_postgres_conn!()

    assert :ok = Meta.bootstrap(meta_conn)

    on_exit(fn ->
      safe_stop(meta_conn)
    end)

    {:ok, meta_conn: meta_conn}
  end

  test "append stream writes into DuckLake with DuckDB metadata", %{meta_conn: meta_conn} do
    root = temp_dir!("ducklake_duckdb_append")
    duckdb = ducklake_duckdb_config(root)
    object_prefix = unique_name("ducklake_duckdb_append")
    checkpoint_key = "#{object_prefix}:raw.events"

    on_exit(fn ->
      File.rm_rf(root)
    end)

    {:ok, stream} =
      DuckFeeder.start_append_stream(
        designated_tables: [%{target_schema: "raw", target_table: "events"}],
        meta_conn: meta_conn,
        duckdb: duckdb,
        object_prefix: object_prefix,
        pipeline_opts: %{max_rows: 100, max_bytes: 10_000, flush_interval_ms: 60_000},
        observer_pid: self()
      )

    on_exit(fn ->
      safe_stop(stream)
    end)

    assert :ok = DuckFeeder.append_event(stream, "events", %{"id" => 1, "kind" => "page_view"})
    assert :ok = DuckFeeder.append_event(stream, "events", %{"id" => 2, "kind" => "signup"})

    assert {:ok, batch} = DuckFeeder.flush_append_table(stream, "events")
    assert batch.row_count == 2

    assert_receive {:duck_feeder_append_batch_processed, {"raw", "events"}, {:ok, result}, _},
                   5_000

    assert result.checkpoint_key == checkpoint_key
    assert result.checkpoint_lsn == "0/2"
    assert {:ok, "0/2"} = Meta.fetch_checkpoint(meta_conn, checkpoint_key)

    assert %{"type" => ["ducklake"]} =
             query_duckdb!(
               duckdb,
               "SELECT type FROM duckdb_databases() WHERE database_name = 'lake'"
             )

    assert %{"id" => [1, 2], "kind" => ["page_view", "signup"]} =
             query_duckdb!(duckdb, "SELECT id, kind FROM lake.raw.events ORDER BY id")

    snapshot_count = query_duckdb!(duckdb, "SELECT count(*) AS n FROM lake.snapshots()")
    assert hd(snapshot_count["n"]) >= 2

    safe_stop(stream)
    _ = flush_ducklake_inlined_data!(duckdb)

    assert duckdb.ducklake_metadata_path |> File.exists?()
    assert parquet_file_count(duckdb) > 0
  end

  test "append stream writes into DuckLake with Postgres metadata", %{meta_conn: meta_conn} do
    root = temp_dir!("ducklake_postgres_append")
    catalog_database = unique_name("ducklake_catalog")
    base_postgres_url = meta_database_url!()
    catalog_postgres_url = create_postgres_database!(base_postgres_url, catalog_database)
    duckdb = ducklake_postgres_config(root, catalog_postgres_url)
    object_prefix = unique_name("ducklake_postgres_append")
    checkpoint_key = "#{object_prefix}:raw.events"

    on_exit(fn ->
      _ = drop_postgres_database!(base_postgres_url, catalog_database)
      File.rm_rf(root)
    end)

    {:ok, stream} =
      DuckFeeder.start_append_stream(
        designated_tables: [%{target_schema: "raw", target_table: "events"}],
        meta_conn: meta_conn,
        duckdb: duckdb,
        object_prefix: object_prefix,
        pipeline_opts: %{max_rows: 100, max_bytes: 10_000, flush_interval_ms: 60_000},
        observer_pid: self()
      )

    on_exit(fn ->
      safe_stop(stream)
    end)

    assert :ok = DuckFeeder.append_event(stream, "events", %{"id" => 1, "kind" => "telemetry"})
    assert :ok = DuckFeeder.append_event(stream, "events", %{"id" => 2, "kind" => "audit"})

    assert {:ok, batch} = DuckFeeder.flush_append_table(stream, "events")
    assert batch.row_count == 2

    assert_receive {:duck_feeder_append_batch_processed, {"raw", "events"}, {:ok, result}, _},
                   5_000

    assert result.checkpoint_key == checkpoint_key
    assert result.checkpoint_lsn == "0/2"
    assert {:ok, "0/2"} = Meta.fetch_checkpoint(meta_conn, checkpoint_key)

    assert %{"id" => [1, 2], "kind" => ["telemetry", "audit"]} =
             query_duckdb!(duckdb, "SELECT id, kind FROM lake.raw.events ORDER BY id")

    safe_stop(stream)
    _ = flush_ducklake_inlined_data!(duckdb)

    catalog_conn = start_postgres_conn!(catalog_postgres_url)

    try do
      assert {:ok, %Postgrex.Result{rows: [[count]]}} =
               Postgrex.query(
                 catalog_conn,
                 "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE 'ducklake_%'",
                 []
               )

      assert count > 0
    after
      safe_stop(catalog_conn)
    end

    assert parquet_file_count(duckdb) > 0
  end

  defp parquet_file_count(duckdb) do
    data_path = String.replace(duckdb.ducklake_data_path, "'", "''")

    duckdb
    |> query_duckdb!("SELECT count(*) AS n FROM glob('#{data_path}/**/*.parquet')")
    |> Map.fetch!("n")
    |> hd()
  end
end
