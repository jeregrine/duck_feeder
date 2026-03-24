defmodule DuckFeeder.Runtime.SupervisorTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias DuckFeeder.Runtime.Supervisor

  defmodule FakeStreamWorker do
    use GenServer

    def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

    @impl true
    def init(opts) do
      if pid = opts[:observer_pid], do: send(pid, {:fake_stream_worker_start, opts})
      {:ok, %{opts: opts}}
    end
  end

  defmodule FailStreamWorker do
    use GenServer

    def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

    @impl true
    def init(_opts), do: {:stop, :stream_start_failed}
  end

  defmodule FakeRuntime do
    def start_stream(_meta_conn, _source_name, _duckdb_config, opts) do
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

  test "starts runtime supervisor with stream worker" do
    assert {:ok, sup} =
             Supervisor.start_link(
               meta_conn: :meta,
               source_name: "source-a",
               duckdb_config: %{path: "/tmp/source-a.duckdb"},
               stream_worker_module: FakeStreamWorker,
               observer_pid: self(),
               runtime_opts: [observer_pid: self()]
             )

    assert_receive {:fake_stream_worker_start, stream_opts}
    assert stream_opts[:meta_conn] == :meta
    assert stream_opts[:source_name] == "source-a"
    assert stream_opts[:duckdb_config][:path] == "/tmp/source-a.duckdb"

    children = :supervisor.which_children(sup)
    assert length(children) == 1

    GenServer.stop(sup)
  end

  test "restarts stream worker when managed cdc child dies" do
    assert {:ok, sup} =
             Supervisor.start_link(
               meta_conn: :meta,
               source_name: "source-a",
               duckdb_config: %{path: "/tmp/source-a.duckdb"},
               runtime_module: FakeRuntime,
               runtime_opts: [test_pid: self()],
               observer_pid: self()
             )

    assert_receive {:fake_runtime_started, _service_pid_1, cdc_pid_1}

    test_pid = self()

    capture_log(fn ->
      Process.exit(cdc_pid_1, :kill)
      assert_receive {:fake_runtime_started, _service_pid_2, cdc_pid_2}, 1_000
      send(test_pid, {:restarted_cdc_pid, cdc_pid_2})
    end)

    assert_receive {:restarted_cdc_pid, cdc_pid_2}
    refute cdc_pid_1 == cdc_pid_2

    GenServer.stop(sup)
  end

  test "returns error when stream worker fails to start" do
    Process.flag(:trap_exit, true)

    assert {:error, {:shutdown, {:failed_to_start_child, FailStreamWorker, :stream_start_failed}}} =
             Supervisor.start_link(
               meta_conn: :meta,
               source_name: "source-a",
               duckdb_config: %{path: "/tmp/source-a.duckdb"},
               stream_worker_module: FailStreamWorker
             )
  end
end
