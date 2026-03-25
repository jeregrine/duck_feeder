defmodule DuckFeeder.RuntimeTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.Runtime

  defmodule FakeMeta do
    @state_table :duck_feeder_runtime_test_state

    def fetch_start_lsn(_conn, checkpoint_keys, default_lsn) do
      case Enum.sort(checkpoint_keys) do
        ["source-a:raw.users"] -> {:ok, "0/20"}
        ["source-resume:raw.users"] -> {:ok, "0/40"}
        ["source-partial:raw.users"] -> {:ok, "0/34"}
        _ -> {:ok, default_lsn}
      end
    end

    def fetch_snapshot_handoff(_conn, source_name) do
      ensure_state_table()

      case :ets.lookup(@state_table, {self(), source_name}) do
        [{{_pid, ^source_name}, handoff}] -> {:ok, handoff}
        [] -> {:ok, nil}
      end
    end

    def fail_mark_snapshot_handoff_pending(source_name, attempts)
        when is_binary(source_name) and source_name != "" and is_integer(attempts) and
               attempts >= 0 do
      ensure_state_table()
      true = :ets.insert(@state_table, {{self(), {:fail_pending, source_name}}, attempts})
      :ok
    end

    def fail_mark_snapshot_handoff_complete(source_name, attempts)
        when is_binary(source_name) and source_name != "" and is_integer(attempts) and
               attempts >= 0 do
      ensure_state_table()
      true = :ets.insert(@state_table, {{self(), {:fail_complete, source_name}}, attempts})
      :ok
    end

    def mark_snapshot_handoff_pending(_conn, source_name, boundary_lsn) do
      ensure_state_table()

      case consume_fail_attempt({:fail_pending, source_name}) do
        true ->
          {:error, :forced_mark_pending_failure}

        false ->
          handoff = %{source_name: source_name, state: :pending, boundary_lsn: boundary_lsn}
          true = :ets.insert(@state_table, {{self(), source_name}, handoff})
          {:ok, boundary_lsn}
      end
    end

    def mark_snapshot_handoff_complete(_conn, source_name, boundary_lsn) do
      ensure_state_table()

      case consume_fail_attempt({:fail_complete, source_name}) do
        true ->
          {:error, :forced_mark_complete_failure}

        false ->
          handoff = %{source_name: source_name, state: :complete, boundary_lsn: boundary_lsn}
          true = :ets.insert(@state_table, {{self(), source_name}, handoff})
          {:ok, boundary_lsn}
      end
    end

    def clear_snapshot_handoff(_conn, source_name) do
      ensure_state_table()
      _ = :ets.delete(@state_table, {self(), source_name})
      _ = :ets.delete(@state_table, {self(), {:fail_pending, source_name}})
      _ = :ets.delete(@state_table, {self(), {:fail_complete, source_name}})
      :ok
    end

    defp consume_fail_attempt(key) do
      case :ets.lookup(@state_table, {self(), key}) do
        [{{_pid, ^key}, attempts}] when attempts > 0 ->
          true = :ets.insert(@state_table, {{self(), key}, attempts - 1})
          true

        _ ->
          false
      end
    end

    defp ensure_state_table do
      case :ets.whereis(@state_table) do
        :undefined ->
          try do
            _ = :ets.new(@state_table, [:named_table, :public, :set])
            :ok
          rescue
            ArgumentError -> :ok
          end

        _ ->
          :ok
      end
    end
  end

  defmodule FakeService do
    use GenServer

    def start_link(opts) do
      GenServer.start_link(__MODULE__, opts)
    end

    def push_event(server, event) do
      GenServer.call(server, {:push_event, event})
    end

    def ingest_snapshot_row(server, designated_table, row) do
      GenServer.call(server, {:ingest_snapshot_row, designated_table, row})
    end

    @impl true
    def init(opts) do
      observer_pid = opts[:observer_pid]
      if is_pid(observer_pid), do: send(observer_pid, {:fake_service_start, opts})
      {:ok, %{observer_pid: observer_pid}}
    end

    @impl true
    def handle_call({:push_event, event}, _from, state) do
      if is_pid(state.observer_pid), do: send(state.observer_pid, {:fake_service_event, event})
      {:reply, :buffering, state}
    end

    def handle_call({:ingest_snapshot_row, designated_table, row}, _from, state) do
      if is_pid(state.observer_pid),
        do: send(state.observer_pid, {:fake_service_snapshot_row, designated_table, row})

      {:reply, :ok, state}
    end

    @impl true
    def handle_info({:duck_feeder_cdc_event, event}, state) do
      if is_pid(state.observer_pid), do: send(state.observer_pid, {:fake_service_event, event})
      {:noreply, state}
    end
  end

  defmodule FakeCDC do
    alias DuckFeeder.CDC.Event

    def start_link(opts) do
      send(self(), {:fake_cdc_start, opts})

      event = %Event.Relation{id: 1, schema: "public", table: "users"}

      case opts[:event_sink] do
        pid when is_pid(pid) -> send(pid, {:duck_feeder_cdc_event, event})
        sink when is_function(sink, 1) -> :ok = sink.(event)
      end

      pid = spawn(fn -> Process.sleep(:infinity) end)
      {:ok, pid}
    end
  end

  defmodule FakeConnectionOptions do
    def resolve(_source, _opts) do
      {:ok, [hostname: "localhost", port: 5432, database: "db", username: "postgres"]}
    end
  end

  defmodule FakeCDCFailStart do
    def start_link(_opts), do: {:error, :failed_to_start_cdc}
  end

  defmodule FakeBootstrap do
    def bootstrap(:query_conn, attrs) do
      send(self(), {:fake_bootstrap, attrs})

      {:ok,
       %{
         publication: :exists,
         slot: :exists,
         start_lsn: "0/30",
         current_lsn: "0/40",
         start_replication_sql: "START_REPLICATION ..."
       }}
    end
  end

  defmodule FakeBootstrapCreatedSlot do
    def bootstrap(:query_conn, attrs) do
      send(self(), {:fake_bootstrap_created_slot, attrs})

      {:ok,
       %{
         publication: :created,
         slot: {:created, %{slot_name: "slot-a", lsn: "0/30"}},
         start_lsn: "0/30",
         current_lsn: "0/40",
         start_replication_sql: "START_REPLICATION ..."
       }}
    end
  end

  defmodule FakeSnapshotRunner do
    def run(:query_conn, designated_tables, opts) do
      send(self(), {:fake_snapshot_runner, designated_tables})

      :ok = opts[:row_handler].(hd(designated_tables), %{"id" => 1})

      {:ok,
       %{
         snapshot_id: "snap-1",
         boundary_lsn: "0/35",
         table_counts: %{{"public", "users"} => 1}
       }}
    end
  end

  defmodule FakeSnapshotRunnerThreeRows do
    def run(:query_conn, designated_tables, opts) do
      send(self(), {:fake_snapshot_runner_three_rows, designated_tables})

      :ok = opts[:row_handler].(hd(designated_tables), %{"id" => 1})
      :ok = opts[:row_handler].(hd(designated_tables), %{"id" => 2})
      :ok = opts[:row_handler].(hd(designated_tables), %{"id" => 3})

      {:ok,
       %{
         snapshot_id: "snap-3",
         boundary_lsn: "0/35",
         table_counts: %{{"public", "users"} => 3}
       }}
    end
  end

  defmodule FakeSnapshotRunnerRaises do
    def run(:query_conn, _designated_tables, _opts) do
      raise "snapshot runner boom"
    end
  end

  defmodule FakeBootstrapRaises do
    def bootstrap(:query_conn, _attrs) do
      raise "bootstrap boom"
    end
  end

  defmodule FakeSnapshotIngestCrashService do
    use GenServer

    def start_link(opts), do: GenServer.start(__MODULE__, opts)
    def push_event(_server, _event), do: :buffering

    def ingest_snapshot_row(server, designated_table, row) do
      GenServer.call(server, {:ingest_snapshot_row, designated_table, row})
    end

    @impl true
    def init(opts) do
      observer_pid = opts[:observer_pid]
      if is_pid(observer_pid), do: send(observer_pid, {:fake_service_start, opts})
      {:ok, %{observer_pid: observer_pid}}
    end

    @impl true
    def handle_call({:ingest_snapshot_row, _designated_table, _row}, _from, state) do
      Process.exit(self(), :snapshot_ingest_crash)
      {:reply, :ok, state}
    end
  end

  setup do
    table = :duck_feeder_runtime_test_state

    case :ets.whereis(table) do
      :undefined ->
        try do
          _ = :ets.new(table, [:named_table, :public, :set])
          :ok
        rescue
          ArgumentError -> :ok
        end

      _tid ->
        :ok
    end

    _ = :ets.delete(table, {self(), "source-a"})
    _ = :ets.delete(table, {self(), "source-missing-slot"})
    _ = :ets.delete(table, {self(), "source-resume"})
    _ = :ets.delete(table, {self(), "source-partial"})
    _ = :ets.delete(table, {self(), {:fail_pending, "source-a"}})
    _ = :ets.delete(table, {self(), {:fail_pending, "source-missing-slot"}})
    _ = :ets.delete(table, {self(), {:fail_pending, "source-resume"}})
    _ = :ets.delete(table, {self(), {:fail_pending, "source-partial"}})
    _ = :ets.delete(table, {self(), {:fail_complete, "source-a"}})
    _ = :ets.delete(table, {self(), {:fail_complete, "source-missing-slot"}})
    _ = :ets.delete(table, {self(), {:fail_complete, "source-resume"}})
    _ = :ets.delete(table, {self(), {:fail_complete, "source-partial"}})

    :ok
  end

  test "builds service options from explicit runtime config" do
    duckdb = %{
      path:
        Path.join(
          System.tmp_dir!(),
          "duck_feeder_runtime_#{System.unique_integer([:positive])}.duckdb"
        )
    }

    assert {:ok, opts} =
             Runtime.service_options(:meta_conn, "source-a", duckdb,
               meta_module: FakeMeta,
               source: runtime_source("source-a"),
               designated_tables: runtime_tables("source-a"),
               observer_pid: self(),
               snapshot_lsn_start: "0/10",
               max_inflight_batches: 3,
               max_pending_batches: 7
             )

    assert opts[:designated_tables] != []
    refute Keyword.has_key?(opts, :storage)
    assert opts[:duckdb] == duckdb
    assert opts[:meta_module] == FakeMeta
    assert opts[:snapshot_lsn_start] == "0/10"
    assert opts[:max_inflight_batches] == 3
    assert opts[:max_pending_batches] == 7
  end

  test "starts service from explicit runtime config" do
    duckdb = %{}

    assert {:ok, pid} =
             Runtime.start_service(:meta_conn, "source-a", duckdb,
               meta_module: FakeMeta,
               source: runtime_source("source-a"),
               designated_tables: runtime_tables("source-a"),
               name: nil,
               observer_pid: self()
             )

    assert is_pid(pid)
    assert Process.alive?(pid)

    GenServer.stop(pid)
  end

  test "starts streaming runtime stack" do
    duckdb = %{
      path:
        Path.join(
          System.tmp_dir!(),
          "duck_feeder_runtime_#{System.unique_integer([:positive])}.duckdb"
        )
    }

    assert {:ok, %{service_pid: service_pid, cdc_pid: cdc_pid, start_lsn: "0/20"}} =
             Runtime.start_stream(:meta_conn, "source-a", duckdb,
               meta_module: FakeMeta,
               source: runtime_source("source-a"),
               designated_tables: runtime_tables("source-a"),
               service_module: FakeService,
               cdc_module: FakeCDC,
               connection_options_module: FakeConnectionOptions,
               bootstrap_replication?: false,
               reconnect_backoff: 1_500,
               max_lag_bytes: 4_096,
               observer_pid: self(),
               service_name: nil,
               cdc_name: nil
             )

    assert is_pid(service_pid)
    assert is_pid(cdc_pid)

    assert_receive {:fake_service_start, service_opts}
    assert service_opts[:designated_tables] != []
    refute Keyword.has_key?(service_opts, :storage)
    assert service_opts[:duckdb] == duckdb

    assert_receive {:fake_cdc_start, cdc_opts}
    assert cdc_opts[:slot_name] == "slot-a"
    assert cdc_opts[:publication_name] == "pub-a"
    assert cdc_opts[:start_lsn] == "0/20"
    assert cdc_opts[:connection_opts][:hostname] == "localhost"
    assert cdc_opts[:reconnect_backoff] == 1_500
    assert cdc_opts[:max_lag_bytes] == 4_096
    assert is_pid(cdc_opts[:event_sink])

    assert_receive {:fake_service_event, %DuckFeeder.CDC.Event.Relation{id: 1}}

    GenServer.stop(service_pid)
    Process.exit(cdc_pid, :normal)
  end

  test "start_stream applies default reconnect backoff" do
    duckdb = %{}

    assert {:ok, %{service_pid: service_pid, cdc_pid: cdc_pid, start_lsn: "0/20"}} =
             Runtime.start_stream(:meta_conn, "source-a", duckdb,
               meta_module: FakeMeta,
               source: runtime_source("source-a"),
               designated_tables: runtime_tables("source-a"),
               service_module: FakeService,
               cdc_module: FakeCDC,
               connection_options_module: FakeConnectionOptions,
               bootstrap_replication?: false,
               observer_pid: self(),
               service_name: nil,
               cdc_name: nil
             )

    assert_receive {:fake_cdc_start, cdc_opts}
    assert cdc_opts[:reconnect_backoff] == 1_000

    GenServer.stop(service_pid)
    Process.exit(cdc_pid, :normal)
  end

  test "start_stream supports reconnect backoff bounds and jitter" do
    duckdb = %{}

    jitter_fun = fn base_ms, jitter_ms ->
      send(self(), {:fake_reconnect_jitter, base_ms, jitter_ms})
      -200
    end

    assert {:ok, %{service_pid: service_pid, cdc_pid: cdc_pid, start_lsn: "0/20"}} =
             Runtime.start_stream(:meta_conn, "source-a", duckdb,
               meta_module: FakeMeta,
               source: runtime_source("source-a"),
               designated_tables: runtime_tables("source-a"),
               service_module: FakeService,
               cdc_module: FakeCDC,
               connection_options_module: FakeConnectionOptions,
               bootstrap_replication?: false,
               reconnect_backoff: 1_000,
               reconnect_backoff_min_ms: 900,
               reconnect_backoff_max_ms: 1_100,
               reconnect_backoff_jitter_ms: 150,
               reconnect_backoff_jitter_fun: jitter_fun,
               backpressure_lag_bytes: 2_048,
               observer_pid: self(),
               service_name: nil,
               cdc_name: nil
             )

    assert_receive {:fake_reconnect_jitter, 1_000, 150}
    assert_receive {:fake_cdc_start, cdc_opts}
    assert cdc_opts[:reconnect_backoff] == 900
    assert cdc_opts[:backpressure_lag_bytes] == 2_048

    GenServer.stop(service_pid)
    Process.exit(cdc_pid, :normal)
  end

  test "start_stream keeps checkpoint start_lsn when bootstrap reports existing slot" do
    duckdb = %{}

    query_connect_fun = fn connection_opts ->
      send(self(), {:fake_query_connect, connection_opts})
      {:ok, :query_conn}
    end

    query_disconnect_fun = fn :query_conn ->
      send(self(), {:fake_query_disconnect, :query_conn})
      :ok
    end

    assert {:ok, %{service_pid: service_pid, cdc_pid: cdc_pid, start_lsn: "0/20"}} =
             Runtime.start_stream(:meta_conn, "source-a", duckdb,
               meta_module: FakeMeta,
               source: runtime_source("source-a"),
               designated_tables: runtime_tables("source-a"),
               service_module: FakeService,
               cdc_module: FakeCDC,
               connection_options_module: FakeConnectionOptions,
               bootstrap_replication?: true,
               bootstrap_module: FakeBootstrap,
               query_connect_fun: query_connect_fun,
               query_disconnect_fun: query_disconnect_fun,
               observer_pid: self(),
               service_name: nil,
               cdc_name: nil
             )

    assert_receive {:fake_query_connect,
                    [hostname: "localhost", port: 5432, database: "db", username: "postgres"]}

    assert_receive {:fake_bootstrap, %{slot_name: "slot-a", publication_name: "pub-a"}}
    assert_receive {:fake_query_disconnect, :query_conn}

    assert_receive {:fake_cdc_start, cdc_opts}
    assert cdc_opts[:start_lsn] == "0/20"

    GenServer.stop(service_pid)
    Process.exit(cdc_pid, :normal)
  end

  test "start_stream uses bootstrap start_lsn when slot is created" do
    duckdb = %{}

    query_connect_fun = fn connection_opts ->
      send(self(), {:fake_query_connect_created_slot, connection_opts})
      {:ok, :query_conn}
    end

    query_disconnect_fun = fn :query_conn ->
      send(self(), {:fake_query_disconnect_created_slot, :query_conn})
      :ok
    end

    assert {:ok, %{service_pid: service_pid, cdc_pid: cdc_pid, start_lsn: "0/30"}} =
             Runtime.start_stream(:meta_conn, "source-a", duckdb,
               meta_module: FakeMeta,
               source: runtime_source("source-a"),
               designated_tables: runtime_tables("source-a"),
               service_module: FakeService,
               cdc_module: FakeCDC,
               connection_options_module: FakeConnectionOptions,
               bootstrap_replication?: true,
               bootstrap_module: FakeBootstrapCreatedSlot,
               query_connect_fun: query_connect_fun,
               query_disconnect_fun: query_disconnect_fun,
               observer_pid: self(),
               service_name: nil,
               cdc_name: nil
             )

    assert_receive {:fake_query_connect_created_slot,
                    [hostname: "localhost", port: 5432, database: "db", username: "postgres"]}

    assert_receive {:fake_bootstrap_created_slot,
                    %{slot_name: "slot-a", publication_name: "pub-a"}}

    assert_receive {:fake_query_disconnect_created_slot, :query_conn}

    assert_receive {:fake_cdc_start, cdc_opts}
    assert cdc_opts[:start_lsn] == "0/30"

    GenServer.stop(service_pid)
    Process.exit(cdc_pid, :normal)
  end

  test "start_stream returns bootstrap exception and still disconnects query conn" do
    duckdb = %{}

    query_connect_fun = fn _connection_opts ->
      {:ok, :query_conn}
    end

    query_disconnect_fun = fn :query_conn ->
      send(self(), {:fake_query_disconnect_bootstrap, :query_conn})
      :ok
    end

    assert {:error, {:bootstrap_exception, %RuntimeError{message: "bootstrap boom"}}} =
             Runtime.start_stream(:meta_conn, "source-a", duckdb,
               meta_module: FakeMeta,
               source: runtime_source("source-a"),
               designated_tables: runtime_tables("source-a"),
               service_module: FakeService,
               cdc_module: FakeCDC,
               connection_options_module: FakeConnectionOptions,
               bootstrap_replication?: true,
               bootstrap_module: FakeBootstrapRaises,
               query_connect_fun: query_connect_fun,
               query_disconnect_fun: query_disconnect_fun,
               observer_pid: self(),
               service_name: nil,
               cdc_name: nil
             )

    assert_receive {:fake_query_disconnect_bootstrap, :query_conn}
  end

  test "start_stream can run initial snapshot before replication stream" do
    duckdb = %{}

    query_connect_fun = fn connection_opts ->
      send(self(), {:fake_query_connect_snapshot, connection_opts})
      {:ok, :query_conn}
    end

    query_disconnect_fun = fn :query_conn ->
      send(self(), {:fake_query_disconnect_snapshot, :query_conn})
      :ok
    end

    row_handler = fn designated_table, row ->
      send(self(), {:snapshot_row, designated_table, row})
      :ok
    end

    assert {:ok, %{service_pid: service_pid, cdc_pid: cdc_pid, start_lsn: "0/35"}} =
             Runtime.start_stream(:meta_conn, "source-a", duckdb,
               meta_module: FakeMeta,
               source: runtime_source("source-a"),
               designated_tables: runtime_tables("source-a"),
               service_module: FakeService,
               cdc_module: FakeCDC,
               connection_options_module: FakeConnectionOptions,
               bootstrap_replication?: false,
               snapshot_before_stream?: true,
               snapshot_on_restart?: true,
               snapshot_runner_module: FakeSnapshotRunner,
               snapshot_row_handler: row_handler,
               query_connect_fun: query_connect_fun,
               query_disconnect_fun: query_disconnect_fun,
               observer_pid: self(),
               service_name: nil,
               cdc_name: nil
             )

    assert_receive {:fake_query_connect_snapshot, _}
    assert_receive {:fake_snapshot_runner, [%{source_table: "users"}]}
    assert_receive {:snapshot_row, %{source_table: "users"}, %{"id" => 1}}
    assert_receive {:fake_query_disconnect_snapshot, :query_conn}

    assert_receive {:fake_cdc_start, cdc_opts}
    assert cdc_opts[:start_lsn] == "0/35"

    GenServer.stop(service_pid)
    Process.exit(cdc_pid, :normal)
  end

  test "snapshot mode defaults to replaying rows into service when row handler is not provided" do
    duckdb = %{}

    assert {:ok, %{service_pid: service_pid, cdc_pid: cdc_pid, start_lsn: "0/35"}} =
             Runtime.start_stream(:meta_conn, "source-a", duckdb,
               meta_module: FakeMeta,
               source: runtime_source("source-a"),
               designated_tables: runtime_tables("source-a"),
               service_module: FakeService,
               cdc_module: FakeCDC,
               connection_options_module: FakeConnectionOptions,
               bootstrap_replication?: false,
               snapshot_before_stream?: true,
               snapshot_on_restart?: true,
               snapshot_runner_module: FakeSnapshotRunner,
               query_connect_fun: fn _ -> {:ok, :query_conn} end,
               query_disconnect_fun: fn _ -> :ok end,
               observer_pid: self(),
               service_name: nil,
               cdc_name: nil
             )

    assert_receive {:fake_service_start, service_opts}
    assert service_opts[:snapshot_lsn_start] == "0/34"
    assert_receive {:fake_service_snapshot_row, %{source_table: "users"}, %{"id" => 1}}

    GenServer.stop(service_pid)
    Process.exit(cdc_pid, :normal)
  end

  test "partial snapshot replay resumes from checkpoint progress within synthetic lsn window" do
    duckdb = %{}

    assert {:ok, %{service_pid: service_pid, cdc_pid: cdc_pid, start_lsn: "0/35"}} =
             Runtime.start_stream(:meta_conn, "source-partial", duckdb,
               meta_module: FakeMeta,
               source: runtime_source("source-partial"),
               designated_tables: runtime_tables("source-partial"),
               service_module: FakeService,
               cdc_module: FakeCDC,
               connection_options_module: FakeConnectionOptions,
               bootstrap_replication?: false,
               snapshot_before_stream?: true,
               snapshot_on_restart?: true,
               snapshot_runner_module: FakeSnapshotRunnerThreeRows,
               query_connect_fun: fn _ -> {:ok, :query_conn} end,
               query_disconnect_fun: fn _ -> :ok end,
               observer_pid: self(),
               service_name: nil,
               cdc_name: nil
             )

    assert_receive {:fake_service_start, service_opts}
    assert service_opts[:snapshot_lsn_start] == "0/34"

    assert_receive {:fake_service_snapshot_row, %{source_table: "users"}, %{"id" => 3}}
    refute_receive {:fake_service_snapshot_row, %{source_table: "users"}, %{"id" => 1}}, 50
    refute_receive {:fake_service_snapshot_row, %{source_table: "users"}, %{"id" => 2}}, 50

    GenServer.stop(service_pid)
    Process.exit(cdc_pid, :normal)
  end

  test "returns snapshot runner exception and still disconnects query conn" do
    duckdb = %{}

    query_connect_fun = fn _ -> {:ok, :query_conn} end

    query_disconnect_fun = fn :query_conn ->
      send(self(), {:fake_query_disconnect_snapshot_error, :query_conn})
      :ok
    end

    assert {:error, {:initial_snapshot_failed, {:snapshot_runner_exception, %RuntimeError{}}}} =
             Runtime.start_stream(:meta_conn, "source-a", duckdb,
               meta_module: FakeMeta,
               source: runtime_source("source-a"),
               designated_tables: runtime_tables("source-a"),
               service_module: FakeService,
               cdc_module: FakeCDC,
               connection_options_module: FakeConnectionOptions,
               bootstrap_replication?: false,
               snapshot_before_stream?: true,
               snapshot_on_restart?: true,
               snapshot_runner_module: FakeSnapshotRunnerRaises,
               query_connect_fun: query_connect_fun,
               query_disconnect_fun: query_disconnect_fun,
               observer_pid: self(),
               service_name: nil,
               cdc_name: nil
             )

    assert_receive {:fake_query_disconnect_snapshot_error, :query_conn}
  end

  test "returns snapshot replay failure when snapshot ingest crashes" do
    duckdb = %{}

    assert {:error, {:snapshot_replay_failed, {:snapshot_ingest_exit, _reason}}} =
             Runtime.start_stream(:meta_conn, "source-a", duckdb,
               meta_module: FakeMeta,
               source: runtime_source("source-a"),
               designated_tables: runtime_tables("source-a"),
               service_module: FakeSnapshotIngestCrashService,
               cdc_module: FakeCDC,
               connection_options_module: FakeConnectionOptions,
               bootstrap_replication?: false,
               snapshot_before_stream?: true,
               snapshot_on_restart?: true,
               snapshot_runner_module: FakeSnapshotRunner,
               query_connect_fun: fn _ -> {:ok, :query_conn} end,
               query_disconnect_fun: fn _ -> :ok end,
               observer_pid: self(),
               service_name: nil,
               cdc_name: nil
             )
  end

  test "marks snapshot handoff pending when cdc start fails after snapshot replay" do
    duckdb = %{}

    assert {:error, :failed_to_start_cdc} =
             Runtime.start_stream(:meta_conn, "source-a", duckdb,
               meta_module: FakeMeta,
               source: runtime_source("source-a"),
               designated_tables: runtime_tables("source-a"),
               service_module: FakeService,
               cdc_module: FakeCDCFailStart,
               connection_options_module: FakeConnectionOptions,
               bootstrap_replication?: false,
               snapshot_before_stream?: true,
               snapshot_on_restart?: true,
               snapshot_runner_module: FakeSnapshotRunner,
               query_connect_fun: fn _ -> {:ok, :query_conn} end,
               query_disconnect_fun: fn _ -> :ok end,
               observer_pid: self(),
               service_name: nil,
               cdc_name: nil
             )

    assert {:ok, %{state: :pending, boundary_lsn: "0/35"}} =
             FakeMeta.fetch_snapshot_handoff(:meta_conn, "source-a")

    assert {:error, {:snapshot_handoff_incomplete, %{source_name: "source-a", state: :pending}}} =
             Runtime.start_stream(:meta_conn, "source-a", duckdb,
               meta_module: FakeMeta,
               source: runtime_source("source-a"),
               designated_tables: runtime_tables("source-a"),
               service_module: FakeService,
               cdc_module: FakeCDC,
               connection_options_module: FakeConnectionOptions,
               bootstrap_replication?: false,
               snapshot_before_stream?: true,
               observer_pid: self(),
               service_name: nil,
               cdc_name: nil
             )

    assert {:ok, %{service_pid: service_pid, cdc_pid: cdc_pid}} =
             Runtime.start_stream(:meta_conn, "source-a", duckdb,
               meta_module: FakeMeta,
               source: runtime_source("source-a"),
               designated_tables: runtime_tables("source-a"),
               service_module: FakeService,
               cdc_module: FakeCDC,
               connection_options_module: FakeConnectionOptions,
               bootstrap_replication?: false,
               snapshot_before_stream?: true,
               resume_incomplete_snapshot?: true,
               snapshot_runner_module: FakeSnapshotRunner,
               query_connect_fun: fn _ -> {:ok, :query_conn} end,
               query_disconnect_fun: fn _ -> :ok end,
               observer_pid: self(),
               service_name: nil,
               cdc_name: nil
             )

    assert {:ok, %{state: :complete, boundary_lsn: "0/35"}} =
             FakeMeta.fetch_snapshot_handoff(:meta_conn, "source-a")

    GenServer.stop(service_pid)
    Process.exit(cdc_pid, :normal)
  end

  test "marks snapshot handoff pending when snapshot replay fails" do
    duckdb = %{}

    assert {:error, {:snapshot_replay_failed, {:snapshot_ingest_exit, _reason}}} =
             Runtime.start_stream(:meta_conn, "source-a", duckdb,
               meta_module: FakeMeta,
               source: runtime_source("source-a"),
               designated_tables: runtime_tables("source-a"),
               service_module: FakeSnapshotIngestCrashService,
               cdc_module: FakeCDC,
               connection_options_module: FakeConnectionOptions,
               bootstrap_replication?: false,
               snapshot_before_stream?: true,
               snapshot_on_restart?: true,
               snapshot_runner_module: FakeSnapshotRunner,
               query_connect_fun: fn _ -> {:ok, :query_conn} end,
               query_disconnect_fun: fn _ -> :ok end,
               observer_pid: self(),
               service_name: nil,
               cdc_name: nil
             )

    assert {:ok, %{state: :pending, boundary_lsn: "0/35"}} =
             FakeMeta.fetch_snapshot_handoff(:meta_conn, "source-a")
  end

  test "pending handoff resume requires snapshot_before_stream when checkpoint is behind boundary" do
    duckdb = %{}

    assert {:ok, "0/35"} = FakeMeta.mark_snapshot_handoff_pending(:meta_conn, "source-a", "0/35")

    assert {:error,
            {:snapshot_resume_requires_snapshot_before_stream,
             %{source_name: "source-a", state: :pending}}} =
             Runtime.start_stream(:meta_conn, "source-a", duckdb,
               meta_module: FakeMeta,
               source: runtime_source("source-a"),
               designated_tables: runtime_tables("source-a"),
               service_module: FakeService,
               cdc_module: FakeCDC,
               connection_options_module: FakeConnectionOptions,
               bootstrap_replication?: false,
               resume_incomplete_snapshot?: true,
               observer_pid: self(),
               service_name: nil,
               cdc_name: nil
             )
  end

  test "retries mark_snapshot_handoff_pending before failing startup" do
    duckdb = %{}
    :ok = FakeMeta.fail_mark_snapshot_handoff_pending("source-a", 1)

    assert {:error, :failed_to_start_cdc} =
             Runtime.start_stream(:meta_conn, "source-a", duckdb,
               meta_module: FakeMeta,
               source: runtime_source("source-a"),
               designated_tables: runtime_tables("source-a"),
               service_module: FakeService,
               cdc_module: FakeCDCFailStart,
               connection_options_module: FakeConnectionOptions,
               bootstrap_replication?: false,
               snapshot_before_stream?: true,
               snapshot_on_restart?: true,
               snapshot_runner_module: FakeSnapshotRunner,
               snapshot_handoff_mark_retries: 1,
               query_connect_fun: fn _ -> {:ok, :query_conn} end,
               query_disconnect_fun: fn _ -> :ok end,
               observer_pid: self(),
               service_name: nil,
               cdc_name: nil
             )

    assert {:ok, %{state: :pending}} = FakeMeta.fetch_snapshot_handoff(:meta_conn, "source-a")
  end

  test "returns error when mark_snapshot_handoff_pending retries are exhausted" do
    duckdb = %{}
    :ok = FakeMeta.fail_mark_snapshot_handoff_pending("source-a", 3)

    assert {:error, :forced_mark_pending_failure} =
             Runtime.start_stream(:meta_conn, "source-a", duckdb,
               meta_module: FakeMeta,
               source: runtime_source("source-a"),
               designated_tables: runtime_tables("source-a"),
               service_module: FakeService,
               cdc_module: FakeCDC,
               connection_options_module: FakeConnectionOptions,
               bootstrap_replication?: false,
               snapshot_before_stream?: true,
               snapshot_on_restart?: true,
               snapshot_runner_module: FakeSnapshotRunner,
               snapshot_handoff_mark_retries: 1,
               query_connect_fun: fn _ -> {:ok, :query_conn} end,
               query_disconnect_fun: fn _ -> :ok end,
               observer_pid: self(),
               service_name: nil,
               cdc_name: nil
             )
  end

  test "retries mark_snapshot_handoff_complete before succeeding startup" do
    duckdb = %{}
    :ok = FakeMeta.fail_mark_snapshot_handoff_complete("source-a", 1)

    assert {:ok, %{service_pid: service_pid, cdc_pid: cdc_pid}} =
             Runtime.start_stream(:meta_conn, "source-a", duckdb,
               meta_module: FakeMeta,
               source: runtime_source("source-a"),
               designated_tables: runtime_tables("source-a"),
               service_module: FakeService,
               cdc_module: FakeCDC,
               connection_options_module: FakeConnectionOptions,
               bootstrap_replication?: false,
               snapshot_before_stream?: true,
               snapshot_on_restart?: true,
               snapshot_runner_module: FakeSnapshotRunner,
               snapshot_handoff_mark_retries: 1,
               query_connect_fun: fn _ -> {:ok, :query_conn} end,
               query_disconnect_fun: fn _ -> :ok end,
               observer_pid: self(),
               service_name: nil,
               cdc_name: nil
             )

    assert {:ok, %{state: :complete}} = FakeMeta.fetch_snapshot_handoff(:meta_conn, "source-a")

    GenServer.stop(service_pid)
    Process.exit(cdc_pid, :normal)
  end

  test "returns error when mark_snapshot_handoff_complete retries are exhausted" do
    duckdb = %{}
    :ok = FakeMeta.fail_mark_snapshot_handoff_complete("source-a", 3)

    assert {:error, {:snapshot_handoff_mark_complete_failed, :forced_mark_complete_failure}} =
             Runtime.start_stream(:meta_conn, "source-a", duckdb,
               meta_module: FakeMeta,
               source: runtime_source("source-a"),
               designated_tables: runtime_tables("source-a"),
               service_module: FakeService,
               cdc_module: FakeCDC,
               connection_options_module: FakeConnectionOptions,
               bootstrap_replication?: false,
               snapshot_before_stream?: true,
               snapshot_on_restart?: true,
               snapshot_runner_module: FakeSnapshotRunner,
               snapshot_handoff_mark_retries: 1,
               query_connect_fun: fn _ -> {:ok, :query_conn} end,
               query_disconnect_fun: fn _ -> :ok end,
               observer_pid: self(),
               service_name: nil,
               cdc_name: nil
             )
  end

  test "returns error when snapshot handoff is pending and resume is disabled" do
    duckdb = %{}

    assert {:ok, "0/35"} =
             FakeMeta.mark_snapshot_handoff_pending(:meta_conn, "source-resume", "0/35")

    assert {:error,
            {:snapshot_handoff_incomplete, %{source_name: "source-resume", state: :pending}}} =
             Runtime.start_stream(:meta_conn, "source-resume", duckdb,
               meta_module: FakeMeta,
               source: runtime_source("source-resume"),
               designated_tables: runtime_tables("source-resume"),
               service_module: FakeService,
               cdc_module: FakeCDC,
               connection_options_module: FakeConnectionOptions,
               bootstrap_replication?: false,
               snapshot_before_stream?: true,
               observer_pid: self(),
               service_name: nil,
               cdc_name: nil
             )
  end

  test "pending handoff can complete without rerunning snapshot when checkpoint is at/after boundary" do
    duckdb = %{}

    assert {:ok, "0/35"} =
             FakeMeta.mark_snapshot_handoff_pending(:meta_conn, "source-resume", "0/35")

    assert {:ok, %{service_pid: service_pid, cdc_pid: cdc_pid, start_lsn: "0/40"}} =
             Runtime.start_stream(:meta_conn, "source-resume", duckdb,
               meta_module: FakeMeta,
               source: runtime_source("source-resume"),
               designated_tables: runtime_tables("source-resume"),
               service_module: FakeService,
               cdc_module: FakeCDC,
               connection_options_module: FakeConnectionOptions,
               bootstrap_replication?: false,
               snapshot_before_stream?: true,
               resume_incomplete_snapshot?: true,
               snapshot_runner_module: FakeSnapshotRunner,
               query_connect_fun: fn _ -> {:ok, :query_conn} end,
               query_disconnect_fun: fn _ -> :ok end,
               observer_pid: self(),
               service_name: nil,
               cdc_name: nil
             )

    refute_receive {:fake_snapshot_runner, _}, 100

    assert {:ok, %{state: :complete}} =
             FakeMeta.fetch_snapshot_handoff(:meta_conn, "source-resume")

    GenServer.stop(service_pid)
    Process.exit(cdc_pid, :normal)
  end

  test "snapshot replay is skipped when checkpoint lsn is already at or past boundary" do
    duckdb = %{}

    assert {:ok, %{service_pid: service_pid, cdc_pid: cdc_pid, start_lsn: "0/40"}} =
             Runtime.start_stream(:meta_conn, "source-resume", duckdb,
               meta_module: FakeMeta,
               source: runtime_source("source-resume"),
               designated_tables: runtime_tables("source-resume"),
               service_module: FakeService,
               cdc_module: FakeCDC,
               connection_options_module: FakeConnectionOptions,
               bootstrap_replication?: false,
               snapshot_before_stream?: true,
               snapshot_runner_module: FakeSnapshotRunner,
               query_connect_fun: fn _ -> {:ok, :query_conn} end,
               query_disconnect_fun: fn _ -> :ok end,
               observer_pid: self(),
               service_name: nil,
               cdc_name: nil
             )

    assert_receive {:fake_service_start, service_opts}
    refute Keyword.has_key?(service_opts, :snapshot_lsn_start)
    refute_receive {:fake_service_snapshot_row, _, _}, 100

    GenServer.stop(service_pid)
    Process.exit(cdc_pid, :normal)
  end

  test "returns error when snapshot ingest is disabled and row handler missing" do
    duckdb = %{}

    assert {:error, :missing_snapshot_row_handler} =
             Runtime.start_stream(:meta_conn, "source-a", duckdb,
               meta_module: FakeMeta,
               source: runtime_source("source-a"),
               designated_tables: runtime_tables("source-a"),
               service_module: FakeService,
               cdc_module: FakeCDC,
               connection_options_module: FakeConnectionOptions,
               bootstrap_replication?: false,
               snapshot_before_stream?: true,
               snapshot_on_restart?: true,
               snapshot_ingest?: false,
               query_connect_fun: fn _ -> {:ok, :query_conn} end,
               query_disconnect_fun: fn _ -> :ok end,
               observer_pid: self()
             )
  end

  test "returns error when source is missing" do
    duckdb = %{}

    assert {:error, {:missing_runtime_source, "missing"}} =
             Runtime.service_options(:meta_conn, "missing", duckdb, meta_module: FakeMeta)
  end

  test "returns error when designated tables are missing" do
    duckdb = %{}

    assert {:error, {:missing_designated_tables, "source-a"}} =
             Runtime.service_options(:meta_conn, "source-a", duckdb,
               meta_module: FakeMeta,
               source: runtime_source("source-a")
             )
  end

  test "returns error when source fields are missing for stream startup" do
    duckdb = %{}

    assert {:error, {:missing_source_field, :slot_name}} =
             Runtime.start_stream(:meta_conn, "source-missing-slot", duckdb,
               meta_module: FakeMeta,
               source: runtime_source("source-missing-slot"),
               designated_tables: runtime_tables("source-missing-slot"),
               service_module: FakeService,
               cdc_module: FakeCDC,
               connection_options_module: FakeConnectionOptions,
               observer_pid: self()
             )
  end

  defp runtime_source("source-a") do
    %{
      connection_info: %{"dsn" => "postgres://user:pass@localhost:5432/source_a"},
      slot_name: "slot-a",
      publication_name: "pub-a"
    }
  end

  defp runtime_source("source-missing-slot") do
    %{
      connection_info: %{"dsn" => "postgres://user:pass@localhost:5432/source_b"},
      slot_name: nil,
      publication_name: "pub-b"
    }
  end

  defp runtime_source("source-resume") do
    %{
      connection_info: %{"dsn" => "postgres://user:pass@localhost:5432/source_c"},
      slot_name: "slot-resume",
      publication_name: "pub-resume"
    }
  end

  defp runtime_source("source-partial") do
    %{
      connection_info: %{"dsn" => "postgres://user:pass@localhost:5432/source_d"},
      slot_name: "slot-partial",
      publication_name: "pub-partial"
    }
  end

  defp runtime_tables("source-a") do
    [
      %{
        id: 1,
        source_id: 10,
        source_schema: "public",
        source_table: "users",
        target_schema: "raw",
        target_table: "users",
        mode: "cdc_changelog",
        primary_keys: ["id"],
        partition_config: %{}
      }
    ]
  end

  defp runtime_tables("source-missing-slot"), do: []

  defp runtime_tables("source-resume") do
    [
      %{
        id: 2,
        source_id: 12,
        source_schema: "public",
        source_table: "users",
        target_schema: "raw",
        target_table: "users",
        mode: "cdc_changelog",
        primary_keys: ["id"],
        partition_config: %{}
      }
    ]
  end

  defp runtime_tables("source-partial") do
    [
      %{
        id: 3,
        source_id: 13,
        source_schema: "public",
        source_table: "users",
        target_schema: "raw",
        target_table: "users",
        mode: "cdc_changelog",
        primary_keys: ["id"],
        partition_config: %{}
      }
    ]
  end
end
