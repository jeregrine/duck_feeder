defmodule DuckFeeder.AppendStreamTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias DuckFeeder.AppendStream
  alias DuckFeeder.TestSupport.DuckDBHelpers
  alias DuckFeeder.TestSupport.FakeMeta

  test "appends event rows and processes batch" do
    path = DuckDBHelpers.temp_duckdb_path("append_stream")

    stream =
      start_supervised!(
        {AppendStream,
         designated_tables: [%{id: 1, target_schema: "raw", target_table: "events"}],
         meta_module: FakeMeta,
         meta_conn: self(),
         duckdb: %{path: path},
         pipeline_opts: %{max_rows: 1, max_bytes: 10_000, flush_interval_ms: 60_000},
         observer_pid: self()}
      )

    on_exit(fn ->
      _ = File.rm(path)
    end)

    assert :ok = AppendStream.append(stream, "events", %{"id" => 1, "kind" => "telemetry"})

    assert_receive {:duck_feeder_append_batch_processed, {"raw", "events"}, {:ok, result}, batch},
                   1_000

    assert result.status == :committed
    assert result.checkpoint_key == "duck_feeder_append:raw.events"
    assert result.checkpoint_lsn == "0/1"
    assert batch.row_count == 1

    assert %{"id" => [1], "kind" => ["telemetry"]} =
             DuckDBHelpers.query_duckdb_file(path, "SELECT id, kind FROM raw.events ORDER BY id")
  end

  test "returns error for unknown target table" do
    stream =
      start_supervised!(
        {AppendStream,
         designated_tables: [%{id: 1, target_schema: "raw", target_table: "events"}],
         meta_module: FakeMeta,
         meta_conn: self()}
      )

    assert {:error, {:unknown_target_table, {"raw", "missing"}}} =
             AppendStream.append(stream, "missing", %{"kind" => "log"})
  end

  test "returns error for invalid explicit lsn without crashing stream" do
    stream =
      start_supervised!(
        {AppendStream,
         designated_tables: [%{id: 1, target_schema: "raw", target_table: "events"}],
         meta_module: FakeMeta,
         meta_conn: self()}
      )

    assert {:error, {:invalid_lsn, {:invalid_lsn, "bad"}}} =
             AppendStream.append(stream, "events", %{"kind" => "log"}, lsn: "bad")

    assert Process.alive?(stream)
  end

  test "runs DuckDB setup during startup" do
    path = DuckDBHelpers.temp_duckdb_path("append_stream_startup_setup")
    caller = self()

    _stream =
      start_supervised!(
        {AppendStream,
         designated_tables: [%{id: 1, target_schema: "raw", target_table: "events"}],
         meta_module: FakeMeta,
         meta_conn: self(),
         duckdb: %{
           path: path,
           setup_sql: ["CREATE SCHEMA IF NOT EXISTS raw"],
           setup_fun: fn _conn ->
             send(caller, :append_stream_duckdb_setup_ran)
             :ok
           end
         },
         observer_pid: self()}
      )

    on_exit(fn ->
      _ = File.rm(path)
    end)

    assert_receive :append_stream_duckdb_setup_ran, 1_000
  end

  test "fails startup when DuckDB setup fails" do
    path = DuckDBHelpers.temp_duckdb_path("append_stream_startup_setup_fail")

    assert {:error, :append_stream_setup_failed} =
             GenServer.start(
               AppendStream,
               designated_tables: [%{id: 1, target_schema: "raw", target_table: "events"}],
               meta_module: FakeMeta,
               meta_conn: self(),
               duckdb: %{
                 path: path,
                 setup_fun: fn _conn -> {:error, :append_stream_setup_failed} end
               },
               observer_pid: self()
             )

    _ = File.rm(path)
  end

  test "supports explicit flush for append stream table" do
    path = DuckDBHelpers.temp_duckdb_path("append_stream_flush")

    stream =
      start_supervised!(
        {AppendStream,
         designated_tables: [%{id: 1, target_schema: "raw", target_table: "events"}],
         meta_module: FakeMeta,
         meta_conn: self(),
         duckdb: %{path: path},
         pipeline_opts: %{max_rows: 100, max_bytes: 10_000, flush_interval_ms: 60_000},
         observer_pid: self()}
      )

    on_exit(fn ->
      _ = File.rm(path)
    end)

    assert :ok = AppendStream.append(stream, "events", %{"kind" => "error", "message" => "boom"})
    assert {:ok, batch} = AppendStream.flush_table(stream, "events")
    assert batch.row_count == 1

    assert_receive {:duck_feeder_append_batch_processed, {"raw", "events"}, {:ok, _result},
                    _batch},
                   1_000
  end

  test "fails closed when append batch queue overflows" do
    previous = Process.flag(:trap_exit, true)

    on_exit(fn ->
      Process.flag(:trap_exit, previous)
    end)

    stream =
      start_supervised!(
        {AppendStream,
         designated_tables: [%{id: 1, target_schema: "raw", target_table: "events"}],
         meta_module: FakeMeta,
         meta_conn: self(),
         observer_pid: self(),
         max_inflight_batches: 1,
         max_pending_batches: 1,
         batch_processor: fn _context, _table, _batch ->
           Process.sleep(250)

           {:ok,
            %{
              status: :committed,
              checkpoint_key: "duck_feeder_append:raw.events",
              checkpoint_lsn: "0/1"
            }}
         end}
      )

    batch = %{
      rows: [%{"kind" => "telemetry"}],
      row_count: 1,
      lsn_start: "0/1",
      lsn_end: "0/1"
    }

    capture_log(fn ->
      send(stream, {:duck_feeder_batch, {"raw", "events"}, batch})

      send(
        stream,
        {:duck_feeder_batch, {"raw", "events"}, %{batch | lsn_start: "0/2", lsn_end: "0/2"}}
      )

      send(
        stream,
        {:duck_feeder_batch, {"raw", "events"}, %{batch | lsn_start: "0/3", lsn_end: "0/3"}}
      )

      assert_receive {:duck_feeder_append_batch_queue_overflow, {"raw", "events"}, _batch,
                      {:batch_queue_overflow, 1}},
                     1_000

      ref = Process.monitor(stream)
      assert_receive {:DOWN, ^ref, :process, ^stream, {:batch_queue_overflow, 1}}, 1_000
    end)
  end

  test "can drop oldest pending batch when configured" do
    previous = Process.flag(:trap_exit, true)

    on_exit(fn ->
      Process.flag(:trap_exit, previous)
    end)

    stream =
      start_supervised!(
        {AppendStream,
         designated_tables: [%{id: 1, target_schema: "raw", target_table: "events"}],
         meta_module: FakeMeta,
         meta_conn: self(),
         observer_pid: self(),
         max_inflight_batches: 1,
         max_pending_batches: 1,
         overflow_strategy: :drop_oldest,
         batch_processor: fn _context, _table, _batch ->
           Process.sleep(250)

           {:ok,
            %{
              status: :committed,
              checkpoint_key: "duck_feeder_append:raw.events",
              checkpoint_lsn: "0/1"
            }}
         end}
      )

    batch = %{rows: [%{"kind" => "telemetry"}], row_count: 1, lsn_start: "0/1", lsn_end: "0/1"}

    send(stream, {:duck_feeder_batch, {"raw", "events"}, batch})

    send(
      stream,
      {:duck_feeder_batch, {"raw", "events"}, %{batch | lsn_start: "0/2", lsn_end: "0/2"}}
    )

    send(
      stream,
      {:duck_feeder_batch, {"raw", "events"}, %{batch | lsn_start: "0/3", lsn_end: "0/3"}}
    )

    assert_receive {:duck_feeder_append_batch_dropped, {"raw", "events"}, dropped_batch,
                    :drop_oldest},
                   1_000

    assert dropped_batch.lsn_end == "0/2"

    assert_receive {:duck_feeder_append_batch_processed, {"raw", "events"}, {:ok, _},
                    first_batch},
                   1_000

    assert first_batch.lsn_end == "0/1"

    assert_receive {:duck_feeder_append_batch_processed, {"raw", "events"}, {:ok, _}, last_batch},
                   1_000

    assert last_batch.lsn_end == "0/3"
    refute_received {:EXIT, ^stream, _}
  end

  test "validates append stream queue options" do
    assert {:error, {:invalid_option, :max_inflight_batches, 0}} =
             GenServer.start(
               AppendStream,
               designated_tables: [%{id: 1, target_schema: "raw", target_table: "events"}],
               meta_module: FakeMeta,
               meta_conn: self(),
               max_inflight_batches: 0
             )

    assert {:error, {:invalid_option, :max_pending_batches, -1}} =
             GenServer.start(
               AppendStream,
               designated_tables: [%{id: 1, target_schema: "raw", target_table: "events"}],
               meta_module: FakeMeta,
               meta_conn: self(),
               max_pending_batches: -1
             )

    assert {:error, {:invalid_option, :overflow_strategy, :drop_latest}} =
             GenServer.start(
               AppendStream,
               designated_tables: [%{id: 1, target_schema: "raw", target_table: "events"}],
               meta_module: FakeMeta,
               meta_conn: self(),
               overflow_strategy: :drop_latest
             )
  end
end
