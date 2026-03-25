defmodule DuckFeeder.ServiceTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias DuckFeeder.CDC.Event
  alias DuckFeeder.Service
  alias DuckFeeder.TestSupport.DuckDBHelpers
  alias DuckFeeder.TestSupport.FakeMeta
  alias DuckFeeder.TestSupport.ProcessHelpers

  test "runs end-to-end from CDC event to committed DuckDB batch" do
    path = DuckDBHelpers.temp_duckdb_path("service_end_to_end")

    designated_tables = [
      %{
        id: 1,
        source_schema: "public",
        source_table: "users",
        target_schema: "raw",
        target_table: "users"
      }
    ]

    {:ok, service} =
      Service.start_link(
        designated_tables: designated_tables,
        meta_conn: self(),
        meta_module: FakeMeta,
        duckdb: %{path: path},
        pipeline_opts: %{max_rows: 1, max_bytes: 10_000, flush_interval_ms: 60_000},
        observer_pid: self()
      )

    on_exit(fn ->
      ProcessHelpers.safe_stop(service)
      _ = File.rm(path)
    end)

    assert :ok = Service.attach_cdc(service, self())

    assert :buffering =
             Service.push_event(service, %Event.Relation{id: 1, schema: "public", table: "users"})

    assert :buffering =
             Service.push_event(service, %Event.Begin{xid: 700, final_lsn: "0/100"})

    assert :buffering =
             Service.push_event(service, %Event.Insert{
               relation_id: 1,
               record: %{"id" => 1, "name" => "duck"}
             })

    assert {:committed, %{xid: 700}} =
             Service.push_event(service, %Event.Commit{xid: 700, end_lsn: "0/120"})

    assert_receive {:duck_feeder_batch_processed, {"raw", "users"}, {:ok, result}, batch},
                   1_000

    assert_receive {:duck_feeder_ack_lsn, "0/120"}, 1_000

    assert result.status == :committed
    assert result.checkpoint_key == "raw.users"
    assert result.checkpoint_lsn == "0/120"
    assert batch.row_count == 1

    assert %{"id" => [1], "name" => ["duck"]} =
             DuckDBHelpers.query_duckdb_file(path, "SELECT id, name FROM raw.users ORDER BY id")

    refute Service.in_transaction?(service)
  end

  test "runs DuckDB setup during startup" do
    path = DuckDBHelpers.temp_duckdb_path("service_startup_setup")
    caller = self()

    {:ok, service} =
      Service.start_link(
        designated_tables: [
          %{
            id: 1,
            source_schema: "public",
            source_table: "users",
            target_schema: "raw",
            target_table: "users"
          }
        ],
        meta_conn: self(),
        meta_module: FakeMeta,
        duckdb: %{
          path: path,
          setup_sql: ["CREATE SCHEMA IF NOT EXISTS raw"],
          setup_fun: fn _conn ->
            send(caller, :service_duckdb_setup_ran)
            :ok
          end
        },
        observer_pid: self()
      )

    on_exit(fn ->
      ProcessHelpers.safe_stop(service)
      _ = File.rm(path)
    end)

    assert_receive :service_duckdb_setup_ran, 1_000
  end

  test "fails startup when DuckDB setup fails" do
    path = DuckDBHelpers.temp_duckdb_path("service_startup_setup_fail")

    assert {:error, :service_setup_failed} =
             GenServer.start(
               Service,
               designated_tables: [
                 %{
                   id: 1,
                   source_schema: "public",
                   source_table: "users",
                   target_schema: "raw",
                   target_table: "users"
                 }
               ],
               meta_conn: self(),
               meta_module: FakeMeta,
               duckdb: %{
                 path: path,
                 setup_fun: fn _conn -> {:error, :service_setup_failed} end
               },
               observer_pid: self()
             )

    _ = File.rm(path)
  end

  test "attach_cdc emits latest checkpoint ack after prior commits" do
    path = DuckDBHelpers.temp_duckdb_path("service_attach_cdc")

    designated_tables = [
      %{
        id: 1,
        source_schema: "public",
        source_table: "users",
        target_schema: "raw",
        target_table: "users"
      }
    ]

    {:ok, service} =
      Service.start_link(
        designated_tables: designated_tables,
        meta_conn: self(),
        meta_module: FakeMeta,
        duckdb: %{path: path},
        pipeline_opts: %{max_rows: 1, max_bytes: 10_000, flush_interval_ms: 60_000},
        observer_pid: self()
      )

    on_exit(fn ->
      ProcessHelpers.safe_stop(service)
      _ = File.rm(path)
    end)

    assert :buffering =
             Service.push_event(service, %Event.Relation{id: 1, schema: "public", table: "users"})

    assert :buffering =
             Service.push_event(service, %Event.Begin{xid: 701, final_lsn: "0/100"})

    assert :buffering =
             Service.push_event(service, %Event.Insert{relation_id: 1, record: %{"id" => 2}})

    assert {:committed, %{xid: 701}} =
             Service.push_event(service, %Event.Commit{xid: 701, end_lsn: "0/120"})

    assert_receive {:duck_feeder_batch_processed, {"raw", "users"}, {:ok, _result}, _batch},
                   1_000

    assert :ok = Service.attach_cdc(service, self())
    assert_receive {:duck_feeder_ack_lsn, "0/120"}, 1_000
  end

  test "ingests pre-tagged snapshot rows without re-wrapping metadata into _record" do
    path = DuckDBHelpers.temp_duckdb_path("service_snapshot_tagged")

    designated_table = %{
      id: 1,
      source_schema: "public",
      source_table: "users",
      target_schema: "raw",
      target_table: "users"
    }

    {:ok, service} =
      Service.start_link(
        designated_tables: [designated_table],
        meta_conn: self(),
        meta_module: FakeMeta,
        duckdb: %{path: path},
        pipeline_opts: %{max_rows: 1, max_bytes: 10_000, flush_interval_ms: 60_000},
        observer_pid: self()
      )

    on_exit(fn ->
      ProcessHelpers.safe_stop(service)
      _ = File.rm(path)
    end)

    tagged_row = %{
      "id" => 1,
      "name" => "snapshot-duck",
      _op: "R",
      _commit_lsn: "0/10",
      _xid: 0,
      _source_ts: nil,
      _ingest_ts: DateTime.utc_now(),
      _relation_schema: "public",
      _relation_table: "users"
    }

    assert :ok = Service.ingest_snapshot_row(service, designated_table, tagged_row)

    assert_receive {:duck_feeder_batch_processed, {"raw", "users"}, {:ok, _result}, batch},
                   1_000

    [row] = batch.rows
    assert row._op == "R"
    assert row._record["id"] == 1
    refute Map.has_key?(row._record, "_op")
    refute Map.has_key?(row._record, "_commit_lsn")
  end

  test "ingests snapshot rows when designated table uses string keys" do
    path = DuckDBHelpers.temp_duckdb_path("service_snapshot_string_keys")

    start_table = %{
      source_schema: "public",
      source_table: "users",
      target_schema: "raw",
      target_table: "users"
    }

    snapshot_table = %{
      "source_schema" => "public",
      "source_table" => "users",
      "target_schema" => "raw",
      "target_table" => "users"
    }

    {:ok, service} =
      Service.start_link(
        designated_tables: [start_table],
        meta_conn: self(),
        meta_module: FakeMeta,
        duckdb: %{path: path},
        pipeline_opts: %{max_rows: 1, max_bytes: 10_000, flush_interval_ms: 60_000},
        observer_pid: self()
      )

    on_exit(fn ->
      ProcessHelpers.safe_stop(service)
      _ = File.rm(path)
    end)

    assert :ok = Service.ingest_snapshot_row(service, snapshot_table, %{"id" => 1})

    assert_receive {:duck_feeder_batch_processed, {"raw", "users"}, {:ok, _result}, batch},
                   1_000

    assert batch.row_count == 1
  end

  test "rejects invalid duckdb option shapes" do
    designated_tables = [
      %{
        id: 1,
        source_schema: "public",
        source_table: "users",
        target_schema: "raw",
        target_table: "users"
      }
    ]

    capture_log(fn ->
      assert {:error, {:invalid_option, :duckdb, [:not, :a, :keyword]}} =
               GenServer.start(
                 Service,
                 designated_tables: designated_tables,
                 meta_conn: self(),
                 duckdb: [:not, :a, :keyword]
               )
    end)
  end

  test "returns CDC validation errors" do
    {:ok, service} =
      Service.start_link(
        designated_tables: [],
        meta_conn: self(),
        observer_pid: self()
      )

    on_exit(fn ->
      ProcessHelpers.safe_stop(service)
    end)

    assert :buffering =
             Service.push_event(service, %Event.Relation{id: 1, schema: "public", table: "users"})

    assert {:error, :change_outside_transaction} =
             Service.push_event(service, %Event.Insert{relation_id: 1, record: %{}})
  end
end
