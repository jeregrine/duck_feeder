defmodule DuckFeeder.Runtime.ManagerTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.Runtime.Manager

  defmodule FakeRuntimeSupervisor do
    use GenServer

    def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

    @impl true
    def init(opts) do
      if pid = opts[:observer_pid], do: send(pid, {:fake_runtime_supervisor_start, opts})
      {:ok, opts}
    end
  end

  defmodule FailRuntimeSupervisor do
    def start_link(_opts), do: {:error, :boom}
  end

  test "starts/stops source runtimes and lists active sources" do
    {:ok, manager} =
      Manager.start_link(
        meta_conn: :meta,
        duckdb: %{path: "/tmp/shared.duckdb"},
        runtime_supervisor_module: FakeRuntimeSupervisor,
        base_opts: [observer_pid: self()]
      )

    assert {:ok, source_a_pid} = Manager.start_source(manager, "source_a")
    assert_receive {:fake_runtime_supervisor_start, start_opts_a}
    assert start_opts_a[:source_name] == "source_a"
    assert start_opts_a[:meta_conn] == :meta
    assert start_opts_a[:duckdb][:path] == "/tmp/shared.duckdb"

    assert {:error, :already_started} = Manager.start_source(manager, "source_a")

    assert {:ok, source_b_pid} = Manager.start_source(manager, "source_b")
    assert_receive {:fake_runtime_supervisor_start, start_opts_b}
    assert start_opts_b[:source_name] == "source_b"

    sources = Manager.list_sources(manager)
    assert sources["source_a"] == source_a_pid
    assert sources["source_b"] == source_b_pid

    assert :ok = Manager.stop_source(manager, "source_a")
    refute Process.alive?(source_a_pid)
    assert {:error, :not_found} = Manager.stop_source(manager, "missing")

    sources = Manager.list_sources(manager)
    refute Map.has_key?(sources, "source_a")
    assert sources["source_b"] == source_b_pid
  end

  test "accepts legacy duckdb_config option" do
    {:ok, manager} =
      Manager.start_link(
        meta_conn: :meta,
        duckdb_config: %{path: "/tmp/shared.duckdb"},
        runtime_supervisor_module: FakeRuntimeSupervisor,
        base_opts: [observer_pid: self()]
      )

    assert {:ok, _source_pid} = Manager.start_source(manager, "source_a")
    assert_receive {:fake_runtime_supervisor_start, start_opts}
    assert start_opts[:duckdb][:path] == "/tmp/shared.duckdb"
  end

  test "drops source from list when runtime process exits" do
    {:ok, manager} =
      Manager.start_link(
        meta_conn: :meta,
        duckdb: %{path: "/tmp/shared.duckdb"},
        runtime_supervisor_module: FakeRuntimeSupervisor,
        base_opts: [observer_pid: self()]
      )

    assert {:ok, source_pid} = Manager.start_source(manager, "source_a")
    assert_receive {:fake_runtime_supervisor_start, _}

    Process.exit(source_pid, :kill)
    Process.sleep(50)

    refute Map.has_key?(Manager.list_sources(manager), "source_a")
  end

  test "can start source again after runtime process exits" do
    {:ok, manager} =
      Manager.start_link(
        meta_conn: :meta,
        duckdb: %{path: "/tmp/shared.duckdb"},
        runtime_supervisor_module: FakeRuntimeSupervisor,
        base_opts: [observer_pid: self()]
      )

    assert {:ok, source_pid_1} = Manager.start_source(manager, "source_a")
    assert_receive {:fake_runtime_supervisor_start, _}

    Process.exit(source_pid_1, :kill)
    Process.sleep(50)

    assert {:ok, source_pid_2} = Manager.start_source(manager, "source_a")
    assert_receive {:fake_runtime_supervisor_start, _}

    refute source_pid_1 == source_pid_2
    assert Manager.list_sources(manager)["source_a"] == source_pid_2
  end

  test "propagates runtime startup errors" do
    {:ok, manager} =
      Manager.start_link(
        meta_conn: :meta,
        duckdb: %{path: "/tmp/shared.duckdb"},
        runtime_supervisor_module: FailRuntimeSupervisor
      )

    assert {:error, :boom} = Manager.start_source(manager, "source_a")
  end
end
