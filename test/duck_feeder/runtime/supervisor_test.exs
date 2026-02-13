defmodule DuckFeeder.Runtime.SupervisorTest do
  use ExUnit.Case, async: true

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

  defmodule FakeReconcilerWorker do
    use GenServer

    def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

    @impl true
    def init(opts) do
      if pid = opts[:observer_pid], do: send(pid, {:fake_reconciler_worker_start, opts})
      {:ok, %{opts: opts}}
    end
  end

  defmodule FailStreamWorker do
    use GenServer

    def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

    @impl true
    def init(_opts), do: {:stop, :stream_start_failed}
  end

  test "starts runtime supervisor with stream worker" do
    assert {:ok, sup} =
             Supervisor.start_link(
               meta_conn: :meta,
               source_name: "source-a",
               storage_config: %{provider: :s3, bucket: "bucket"},
               stream_worker_module: FakeStreamWorker,
               observer_pid: self(),
               runtime_opts: [observer_pid: self()]
             )

    assert_receive {:fake_stream_worker_start, stream_opts}
    assert stream_opts[:meta_conn] == :meta
    assert stream_opts[:source_name] == "source-a"

    children = :supervisor.which_children(sup)
    assert length(children) == 1

    GenServer.stop(sup)
  end

  test "starts runtime supervisor with reconciler worker" do
    assert {:ok, sup} =
             Supervisor.start_link(
               meta_conn: :meta,
               source_name: "source-a",
               storage_config: %{provider: :s3, bucket: "bucket"},
               stream_worker_module: FakeStreamWorker,
               reconciler_worker_module: FakeReconcilerWorker,
               start_reconciler?: true,
               observer_pid: self(),
               reconcile_opts: [cleanup_failed_uploads?: true],
               runtime_opts: [observer_pid: self()]
             )

    assert_receive {:fake_stream_worker_start, _}
    assert_receive {:fake_reconciler_worker_start, rec_opts}
    assert rec_opts[:context][:meta_conn] == :meta
    assert rec_opts[:context][:storage][:bucket] == "bucket"
    assert rec_opts[:reconcile_opts] == [cleanup_failed_uploads?: true]

    children = :supervisor.which_children(sup)
    assert length(children) == 2

    GenServer.stop(sup)
  end

  test "returns error when stream worker fails to start" do
    Process.flag(:trap_exit, true)

    assert {:error, {:shutdown, {:failed_to_start_child, FailStreamWorker, :stream_start_failed}}} =
             Supervisor.start_link(
               meta_conn: :meta,
               source_name: "source-a",
               storage_config: %{provider: :s3, bucket: "bucket"},
               stream_worker_module: FailStreamWorker
             )
  end
end
