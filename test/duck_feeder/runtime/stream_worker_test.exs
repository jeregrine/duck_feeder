defmodule DuckFeeder.Runtime.StreamWorkerTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias DuckFeeder.Runtime.StreamWorker

  defmodule FakeRuntime do
    def start_stream(_meta_conn, _source_name, _duckdb, opts) do
      if opts[:fail] do
        {:error, :failed_to_start_stream}
      else
        test_pid = opts[:test_pid]
        service_pid = spawn(fn -> Process.sleep(:infinity) end)
        cdc_pid = spawn(fn -> Process.sleep(:infinity) end)

        if is_pid(test_pid) do
          send(test_pid, {:fake_runtime_started, service_pid, cdc_pid})
        end

        {:ok,
         %{
           service_pid: service_pid,
           cdc_pid: cdc_pid,
           start_lsn: "0/20",
           source: %{name: "source-a"}
         }}
      end
    end
  end

  setup do
    Process.flag(:trap_exit, true)
    :ok
  end

  test "starts stream worker and reports stream info" do
    {:ok, worker} =
      StreamWorker.start_link(
        meta_conn: :meta,
        source_name: "source-a",
        duckdb: %{path: "/tmp/source-a.duckdb"},
        runtime_module: FakeRuntime,
        runtime_opts: [test_pid: self()]
      )

    assert_receive {:fake_runtime_started, service_pid, cdc_pid}

    assert {:ok, info} = StreamWorker.stream_info(worker)
    assert info.service_pid == service_pid
    assert info.cdc_pid == cdc_pid
    assert info.start_lsn == "0/20"

    GenServer.stop(worker)
    refute Process.alive?(service_pid)
    refute Process.alive?(cdc_pid)
  end

  test "stops when child stream process dies" do
    {:ok, worker} =
      StreamWorker.start_link(
        meta_conn: :meta,
        source_name: "source-a",
        duckdb: %{path: "/tmp/source-a.duckdb"},
        runtime_module: FakeRuntime,
        runtime_opts: [test_pid: self()]
      )

    assert_receive {:fake_runtime_started, _service_pid, cdc_pid}

    monitor_ref = Process.monitor(worker)

    capture_log(fn ->
      Process.exit(cdc_pid, :kill)

      assert_receive {:DOWN, ^monitor_ref, :process, ^worker,
                      {:stream_child_down, ^cdc_pid, :killed}},
                     2_000
    end)
  end

  test "fails to start when runtime stream startup fails" do
    assert {:error, :failed_to_start_stream} =
             StreamWorker.start_link(
               meta_conn: :meta,
               source_name: "source-a",
               duckdb: %{path: "/tmp/source-a.duckdb"},
               runtime_module: FakeRuntime,
               runtime_opts: [fail: true]
             )
  end
end
