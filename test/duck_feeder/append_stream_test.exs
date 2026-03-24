defmodule DuckFeeder.AppendStreamTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias DuckFeeder.AppendStream
  alias DuckFeeder.DuckDB.Client, as: DuckDBClient
  alias DuckFeeder.DuckDB.Connection, as: DuckDBConnection

  defmodule FakeMeta do
    def upsert_checkpoint(_conn, _checkpoint_key, lsn), do: {:ok, lsn}
  end

  defmodule SlowSink do
    @behaviour DuckFeeder.Sink

    @impl true
    def process_batch(context, table, batch) do
      Process.sleep(250)

      {:ok,
       %{
         status: :committed,
         batch_id: "append-slow-sink-batch",
         checkpoint_key: Map.fetch!(context.designated_table_by_target, table),
         checkpoint_lsn: batch.lsn_end
       }}
    end
  end

  defmodule FakeSink do
    @behaviour DuckFeeder.Sink

    @impl true
    def process_batch(context, table, batch) do
      if is_pid(context.meta_conn) do
        send(context.meta_conn, {:fake_sink_batch, context, table, batch})
      end

      {:ok,
       %{
         status: :committed,
         batch_id: "append-sink-batch",
         checkpoint_key: Map.fetch!(context.designated_table_by_target, table),
         checkpoint_lsn: batch.lsn_end
       }}
    end
  end

  test "appends event rows and processes batch" do
    designated_tables = [
      %{id: 1, target_schema: "raw", target_table: "events"}
    ]

    {:ok, stream} =
      AppendStream.start_link(
        designated_tables: designated_tables,
        meta_conn: self(),
        sink_module: FakeSink,
        pipeline_opts: %{max_rows: 1, max_bytes: 10_000, flush_interval_ms: 60_000},
        observer_pid: self()
      )

    assert :ok = AppendStream.append(stream, "events", %{"kind" => "telemetry", "value" => 1})

    assert_receive {:fake_sink_batch, _context, {"raw", "events"}, batch}, 1_000

    assert_receive {:duck_feeder_append_batch_processed, {"raw", "events"}, {:ok, result},
                    ^batch},
                   1_000

    assert result.status == :committed
    assert batch.row_count == 1
  end

  test "supports custom sink modules without default DuckDB config" do
    designated_tables = [
      %{id: 1, target_schema: "raw", target_table: "events"}
    ]

    {:ok, stream} =
      AppendStream.start_link(
        designated_tables: designated_tables,
        meta_conn: self(),
        sink_module: FakeSink,
        pipeline_opts: %{max_rows: 1, max_bytes: 10_000, flush_interval_ms: 60_000},
        observer_pid: self()
      )

    assert :ok = AppendStream.append(stream, "events", %{"kind" => "telemetry", "value" => 2})

    assert_receive {:fake_sink_batch, context, {"raw", "events"}, batch}, 1_000
    refute Map.has_key?(context, :storage)
    assert context.sink_module == FakeSink
    assert batch.row_count == 1

    assert_receive {:duck_feeder_append_batch_processed, {"raw", "events"}, {:ok, result}, _},
                   1_000

    assert result.status == :committed
    assert result.batch_id == "append-sink-batch"
  end

  test "auto-selects DuckDB sink and starts an internal DuckDB connection" do
    path =
      Path.join(
        System.tmp_dir!(),
        "duck_feeder_append_#{System.unique_integer([:positive])}.duckdb"
      )

    on_exit(fn ->
      _ = File.rm(path)
    end)

    {:ok, stream} =
      AppendStream.start_link(
        designated_tables: [%{id: 1, target_schema: "raw", target_table: "events"}],
        meta_module: FakeMeta,
        meta_conn: :fake_conn,
        duckdb: %{path: path},
        pipeline_opts: %{max_rows: 1, max_bytes: 10_000, flush_interval_ms: 60_000},
        observer_pid: self()
      )

    assert :ok = AppendStream.append(stream, "events", %{"id" => 1, "kind" => "telemetry"})

    assert_receive {:duck_feeder_append_batch_processed, {"raw", "events"}, {:ok, result}, _},
                   1_000

    assert result.status == :committed
    assert result.checkpoint_lsn == "0/1"

    assert %{"id" => [1], "kind" => ["telemetry"]} =
             query_duckdb_file(path, "SELECT id, kind FROM raw.events ORDER BY id")
  end

  test "returns error for unknown target table" do
    {:ok, stream} =
      AppendStream.start_link(
        designated_tables: [%{id: 1, target_schema: "raw", target_table: "events"}],
        meta_conn: self(),
        sink_module: FakeSink
      )

    assert {:error, {:unknown_target_table, {"raw", "missing"}}} =
             AppendStream.append(stream, "missing", %{"kind" => "log"})
  end

  test "returns error for invalid explicit lsn without crashing stream" do
    {:ok, stream} =
      AppendStream.start_link(
        designated_tables: [%{id: 1, target_schema: "raw", target_table: "events"}],
        meta_conn: self(),
        sink_module: FakeSink
      )

    assert {:error, {:invalid_lsn, {:invalid_lsn, "bad"}}} =
             AppendStream.append(stream, "events", %{"kind" => "log"}, lsn: "bad")

    assert Process.alive?(stream)
  end

  test "supports explicit flush for append stream table" do
    {:ok, stream} =
      AppendStream.start_link(
        designated_tables: [%{id: 1, target_schema: "raw", target_table: "events"}],
        meta_conn: self(),
        sink_module: FakeSink,
        pipeline_opts: %{max_rows: 100, max_bytes: 10_000, flush_interval_ms: 60_000},
        observer_pid: self()
      )

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

    designated_tables = [
      %{id: 1, target_schema: "raw", target_table: "events"}
    ]

    {:ok, stream} =
      AppendStream.start_link(
        designated_tables: designated_tables,
        meta_conn: self(),
        sink_module: SlowSink,
        observer_pid: self(),
        max_inflight_batches: 1,
        max_pending_batches: 1
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

      assert_receive {:EXIT, ^stream, {:batch_queue_overflow, 1}}, 1_000
    end)
  end

  test "can drop oldest pending batch when configured" do
    previous = Process.flag(:trap_exit, true)

    on_exit(fn ->
      Process.flag(:trap_exit, previous)
    end)

    designated_tables = [
      %{id: 1, target_schema: "raw", target_table: "events"}
    ]

    {:ok, stream} =
      AppendStream.start_link(
        designated_tables: designated_tables,
        meta_conn: self(),
        sink_module: SlowSink,
        observer_pid: self(),
        max_inflight_batches: 1,
        max_pending_batches: 1,
        overflow_strategy: :drop_oldest
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
               meta_conn: self(),
               sink_module: FakeSink,
               max_inflight_batches: 0
             )

    assert {:error, {:invalid_option, :max_pending_batches, -1}} =
             GenServer.start(
               AppendStream,
               designated_tables: [%{id: 1, target_schema: "raw", target_table: "events"}],
               meta_conn: self(),
               sink_module: FakeSink,
               max_pending_batches: -1
             )

    assert {:error, {:invalid_option, :overflow_strategy, :drop_latest}} =
             GenServer.start(
               AppendStream,
               designated_tables: [%{id: 1, target_schema: "raw", target_table: "events"}],
               meta_conn: self(),
               sink_module: FakeSink,
               overflow_strategy: :drop_latest
             )
  end

  defp query_duckdb_file(path, sql) do
    {:ok, server} = DuckDBConnection.start_link(name: nil, path: path)
    conn = DuckDBConnection.get_conn(server)

    try do
      {:ok, result} = DuckDBClient.query_map(conn, sql)
      result
    after
      safe_stop(server)
    end
  end

  defp safe_stop(pid) when is_pid(pid) do
    _ = GenServer.stop(pid)
    :ok
  catch
    :exit, _reason -> :ok
  end
end
