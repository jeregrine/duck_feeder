defmodule DuckFeeder.RuntimeTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.Runtime

  defmodule FakeMeta do
    @state_table :duck_feeder_runtime_test_state
    def get_source(_conn, "source-a") do
      {:ok,
       %{
         id: 10,
         name: "source-a",
         connection_info: %{"dsn" => "postgres://user:pass@localhost:5432/source_a"},
         slot_name: "slot-a",
         publication_name: "pub-a"
       }}
    end

    def get_source(_conn, "source-missing-slot") do
      {:ok,
       %{
         id: 11,
         name: "source-missing-slot",
         connection_info: %{"dsn" => "postgres://user:pass@localhost:5432/source_b"},
         slot_name: nil,
         publication_name: "pub-b"
       }}
    end

    def get_source(_conn, "source-resume") do
      {:ok,
       %{
         id: 12,
         name: "source-resume",
         connection_info: %{"dsn" => "postgres://user:pass@localhost:5432/source_c"},
         slot_name: "slot-resume",
         publication_name: "pub-resume"
       }}
    end

    def get_source(_conn, "source-partial") do
      {:ok,
       %{
         id: 13,
         name: "source-partial",
         connection_info: %{"dsn" => "postgres://user:pass@localhost:5432/source_d"},
         slot_name: "slot-partial",
         publication_name: "pub-partial"
       }}
    end

    def get_source(_conn, other), do: {:error, {:source_not_found, other}}

    def list_designated_tables(_conn, source_id: 10) do
      {:ok,
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
       ]}
    end

    def list_designated_tables(_conn, source_id: 11), do: {:ok, []}

    def list_designated_tables(_conn, source_id: 12) do
      {:ok,
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
       ]}
    end

    def list_designated_tables(_conn, source_id: 13) do
      {:ok,
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
       ]}
    end

    def list_designated_tables(_conn, _opts), do: {:ok, []}

    def fetch_source_start_lsn(_conn, 10, _default), do: {:ok, "0/20"}
    def fetch_source_start_lsn(_conn, 11, default), do: {:ok, default}
    def fetch_source_start_lsn(_conn, 12, _default), do: {:ok, "0/40"}
    def fetch_source_start_lsn(_conn, 13, _default), do: {:ok, "0/34"}

    def fetch_snapshot_handoff(_conn, source_id) do
      case :ets.lookup(@state_table, {self(), source_id}) do
        [{{_pid, ^source_id}, handoff}] -> {:ok, handoff}
        [] -> {:ok, nil}
      end
    end

    def fail_mark_snapshot_handoff_pending(source_id, attempts)
        when is_integer(source_id) and source_id > 0 and is_integer(attempts) and attempts >= 0 do
      true = :ets.insert(@state_table, {{self(), {:fail_pending, source_id}}, attempts})
      :ok
    end

    def fail_mark_snapshot_handoff_complete(source_id, attempts)
        when is_integer(source_id) and source_id > 0 and is_integer(attempts) and attempts >= 0 do
      true = :ets.insert(@state_table, {{self(), {:fail_complete, source_id}}, attempts})
      :ok
    end

    def mark_snapshot_handoff_pending(_conn, source_id, boundary_lsn) do
      case consume_fail_attempt({:fail_pending, source_id}) do
        true ->
          {:error, :forced_mark_pending_failure}

        false ->
          handoff = %{source_id: source_id, state: :pending, boundary_lsn: boundary_lsn}
          true = :ets.insert(@state_table, {{self(), source_id}, handoff})
          {:ok, boundary_lsn}
      end
    end

    def mark_snapshot_handoff_complete(_conn, source_id, boundary_lsn) do
      case consume_fail_attempt({:fail_complete, source_id}) do
        true ->
          {:error, :forced_mark_complete_failure}

        false ->
          handoff = %{source_id: source_id, state: :complete, boundary_lsn: boundary_lsn}
          true = :ets.insert(@state_table, {{self(), source_id}, handoff})
          {:ok, boundary_lsn}
      end
    end

    def clear_snapshot_handoff(_conn, source_id) do
      _ = :ets.delete(@state_table, {self(), source_id})
      _ = :ets.delete(@state_table, {self(), {:fail_pending, source_id}})
      _ = :ets.delete(@state_table, {self(), {:fail_complete, source_id}})
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

    _ = :ets.delete(table, {self(), 10})
    _ = :ets.delete(table, {self(), 11})
    _ = :ets.delete(table, {self(), 12})
    _ = :ets.delete(table, {self(), 13})
    _ = :ets.delete(table, {self(), {:fail_pending, 10}})
    _ = :ets.delete(table, {self(), {:fail_pending, 11}})
    _ = :ets.delete(table, {self(), {:fail_pending, 12}})
    _ = :ets.delete(table, {self(), {:fail_pending, 13}})
    _ = :ets.delete(table, {self(), {:fail_complete, 10}})
    _ = :ets.delete(table, {self(), {:fail_complete, 11}})
    _ = :ets.delete(table, {self(), {:fail_complete, 12}})
    _ = :ets.delete(table, {self(), {:fail_complete, 13}})

    :ok
  end

  test "builds service options from metadata" do
    storage = %{provider: :s3, bucket: "bucket", adapter: DuckFeeder.Storage.S3}

    assert {:ok, opts} =
             Runtime.service_options(:meta_conn, "source-a", storage,
               meta_module: FakeMeta,
               observer_pid: self(),
               object_prefix: "prefix",
               committer_module: DuckFeeder.DuckLake.Committer.Noop,
               committer_opts: [ducklake_sql: ["SELECT 1"]]
             )

    assert opts[:designated_tables] != []
    assert opts[:storage] == storage
    assert opts[:object_prefix] == "prefix"
    assert opts[:meta_module] == FakeMeta
    assert opts[:committer_module] == DuckFeeder.DuckLake.Committer.Noop
    assert opts[:committer_opts] == [ducklake_sql: ["SELECT 1"]]
  end

  test "starts service from metadata" do
    storage = %{provider: :s3, bucket: "bucket", adapter: DuckFeeder.Storage.S3}

    assert {:ok, pid} =
             Runtime.start_service(:meta_conn, "source-a", storage,
               meta_module: FakeMeta,
               name: nil,
               observer_pid: self()
             )

    assert is_pid(pid)
    assert Process.alive?(pid)

    GenServer.stop(pid)
  end

  test "starts streaming runtime stack" do
    storage = %{provider: :s3, bucket: "bucket", adapter: DuckFeeder.Storage.S3}

    assert {:ok, %{service_pid: service_pid, cdc_pid: cdc_pid, start_lsn: "0/20"}} =
             Runtime.start_stream(:meta_conn, "source-a", storage,
               meta_module: FakeMeta,
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

  test "start_stream supports call-mode event sink" do
    storage = %{provider: :s3, bucket: "bucket", adapter: DuckFeeder.Storage.S3}

    assert {:ok, %{service_pid: service_pid, cdc_pid: cdc_pid, start_lsn: "0/20"}} =
             Runtime.start_stream(:meta_conn, "source-a", storage,
               meta_module: FakeMeta,
               service_module: FakeService,
               cdc_module: FakeCDC,
               connection_options_module: FakeConnectionOptions,
               bootstrap_replication?: false,
               event_sink_mode: :call,
               observer_pid: self(),
               service_name: nil,
               cdc_name: nil
             )

    assert_receive {:fake_cdc_start, cdc_opts}
    assert is_function(cdc_opts[:event_sink], 1)

    GenServer.stop(service_pid)
    Process.exit(cdc_pid, :normal)
  end

  test "start_stream applies default reconnect backoff" do
    storage = %{provider: :s3, bucket: "bucket", adapter: DuckFeeder.Storage.S3}

    assert {:ok, %{service_pid: service_pid, cdc_pid: cdc_pid, start_lsn: "0/20"}} =
             Runtime.start_stream(:meta_conn, "source-a", storage,
               meta_module: FakeMeta,
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

  test "start_stream can bootstrap publication/slot and adjust start lsn" do
    storage = %{provider: :s3, bucket: "bucket", adapter: DuckFeeder.Storage.S3}

    query_connect_fun = fn connection_opts ->
      send(self(), {:fake_query_connect, connection_opts})
      {:ok, :query_conn}
    end

    query_disconnect_fun = fn :query_conn ->
      send(self(), {:fake_query_disconnect, :query_conn})
      :ok
    end

    assert {:ok, %{service_pid: service_pid, cdc_pid: cdc_pid, start_lsn: "0/30"}} =
             Runtime.start_stream(:meta_conn, "source-a", storage,
               meta_module: FakeMeta,
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
    assert cdc_opts[:start_lsn] == "0/30"

    GenServer.stop(service_pid)
    Process.exit(cdc_pid, :normal)
  end

  test "start_stream returns bootstrap exception and still disconnects query conn" do
    storage = %{provider: :s3, bucket: "bucket", adapter: DuckFeeder.Storage.S3}

    query_connect_fun = fn _connection_opts ->
      {:ok, :query_conn}
    end

    query_disconnect_fun = fn :query_conn ->
      send(self(), {:fake_query_disconnect_bootstrap, :query_conn})
      :ok
    end

    assert {:error, {:bootstrap_exception, %RuntimeError{message: "bootstrap boom"}}} =
             Runtime.start_stream(:meta_conn, "source-a", storage,
               meta_module: FakeMeta,
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
    storage = %{provider: :s3, bucket: "bucket", adapter: DuckFeeder.Storage.S3}

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
             Runtime.start_stream(:meta_conn, "source-a", storage,
               meta_module: FakeMeta,
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
    storage = %{provider: :s3, bucket: "bucket", adapter: DuckFeeder.Storage.S3}

    assert {:ok, %{service_pid: service_pid, cdc_pid: cdc_pid, start_lsn: "0/35"}} =
             Runtime.start_stream(:meta_conn, "source-a", storage,
               meta_module: FakeMeta,
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
    storage = %{provider: :s3, bucket: "bucket", adapter: DuckFeeder.Storage.S3}

    assert {:ok, %{service_pid: service_pid, cdc_pid: cdc_pid, start_lsn: "0/35"}} =
             Runtime.start_stream(:meta_conn, "source-partial", storage,
               meta_module: FakeMeta,
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
    storage = %{provider: :s3, bucket: "bucket", adapter: DuckFeeder.Storage.S3}

    query_connect_fun = fn _ -> {:ok, :query_conn} end

    query_disconnect_fun = fn :query_conn ->
      send(self(), {:fake_query_disconnect_snapshot_error, :query_conn})
      :ok
    end

    assert {:error, {:initial_snapshot_failed, {:snapshot_runner_exception, %RuntimeError{}}}} =
             Runtime.start_stream(:meta_conn, "source-a", storage,
               meta_module: FakeMeta,
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
    storage = %{provider: :s3, bucket: "bucket", adapter: DuckFeeder.Storage.S3}

    assert {:error, {:snapshot_replay_failed, {:snapshot_ingest_exit, _reason}}} =
             Runtime.start_stream(:meta_conn, "source-a", storage,
               meta_module: FakeMeta,
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
    storage = %{provider: :s3, bucket: "bucket", adapter: DuckFeeder.Storage.S3}

    assert {:error, :failed_to_start_cdc} =
             Runtime.start_stream(:meta_conn, "source-a", storage,
               meta_module: FakeMeta,
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
             FakeMeta.fetch_snapshot_handoff(:meta_conn, 10)

    assert {:error, {:snapshot_handoff_incomplete, %{source_id: 10, state: :pending}}} =
             Runtime.start_stream(:meta_conn, "source-a", storage,
               meta_module: FakeMeta,
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
             Runtime.start_stream(:meta_conn, "source-a", storage,
               meta_module: FakeMeta,
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
             FakeMeta.fetch_snapshot_handoff(:meta_conn, 10)

    GenServer.stop(service_pid)
    Process.exit(cdc_pid, :normal)
  end

  test "marks snapshot handoff pending when snapshot replay fails" do
    storage = %{provider: :s3, bucket: "bucket", adapter: DuckFeeder.Storage.S3}

    assert {:error, {:snapshot_replay_failed, {:snapshot_ingest_exit, _reason}}} =
             Runtime.start_stream(:meta_conn, "source-a", storage,
               meta_module: FakeMeta,
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
             FakeMeta.fetch_snapshot_handoff(:meta_conn, 10)
  end

  test "pending handoff resume requires snapshot_before_stream when checkpoint is behind boundary" do
    storage = %{provider: :s3, bucket: "bucket", adapter: DuckFeeder.Storage.S3}

    assert {:ok, "0/35"} = FakeMeta.mark_snapshot_handoff_pending(:meta_conn, 10, "0/35")

    assert {:error,
            {:snapshot_resume_requires_snapshot_before_stream, %{source_id: 10, state: :pending}}} =
             Runtime.start_stream(:meta_conn, "source-a", storage,
               meta_module: FakeMeta,
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
    storage = %{provider: :s3, bucket: "bucket", adapter: DuckFeeder.Storage.S3}
    :ok = FakeMeta.fail_mark_snapshot_handoff_pending(10, 1)

    assert {:error, :failed_to_start_cdc} =
             Runtime.start_stream(:meta_conn, "source-a", storage,
               meta_module: FakeMeta,
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

    assert {:ok, %{state: :pending}} = FakeMeta.fetch_snapshot_handoff(:meta_conn, 10)
  end

  test "returns error when mark_snapshot_handoff_pending retries are exhausted" do
    storage = %{provider: :s3, bucket: "bucket", adapter: DuckFeeder.Storage.S3}
    :ok = FakeMeta.fail_mark_snapshot_handoff_pending(10, 3)

    assert {:error, :forced_mark_pending_failure} =
             Runtime.start_stream(:meta_conn, "source-a", storage,
               meta_module: FakeMeta,
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
    storage = %{provider: :s3, bucket: "bucket", adapter: DuckFeeder.Storage.S3}
    :ok = FakeMeta.fail_mark_snapshot_handoff_complete(10, 1)

    assert {:ok, %{service_pid: service_pid, cdc_pid: cdc_pid}} =
             Runtime.start_stream(:meta_conn, "source-a", storage,
               meta_module: FakeMeta,
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

    assert {:ok, %{state: :complete}} = FakeMeta.fetch_snapshot_handoff(:meta_conn, 10)

    GenServer.stop(service_pid)
    Process.exit(cdc_pid, :normal)
  end

  test "returns error when mark_snapshot_handoff_complete retries are exhausted" do
    storage = %{provider: :s3, bucket: "bucket", adapter: DuckFeeder.Storage.S3}
    :ok = FakeMeta.fail_mark_snapshot_handoff_complete(10, 3)

    assert {:error, {:snapshot_handoff_mark_complete_failed, :forced_mark_complete_failure}} =
             Runtime.start_stream(:meta_conn, "source-a", storage,
               meta_module: FakeMeta,
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
    storage = %{provider: :s3, bucket: "bucket", adapter: DuckFeeder.Storage.S3}

    assert {:ok, "0/35"} = FakeMeta.mark_snapshot_handoff_pending(:meta_conn, 12, "0/35")

    assert {:error, {:snapshot_handoff_incomplete, %{source_id: 12, state: :pending}}} =
             Runtime.start_stream(:meta_conn, "source-resume", storage,
               meta_module: FakeMeta,
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
    storage = %{provider: :s3, bucket: "bucket", adapter: DuckFeeder.Storage.S3}

    assert {:ok, "0/35"} = FakeMeta.mark_snapshot_handoff_pending(:meta_conn, 12, "0/35")

    assert {:ok, %{service_pid: service_pid, cdc_pid: cdc_pid, start_lsn: "0/40"}} =
             Runtime.start_stream(:meta_conn, "source-resume", storage,
               meta_module: FakeMeta,
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
    assert {:ok, %{state: :complete}} = FakeMeta.fetch_snapshot_handoff(:meta_conn, 12)

    GenServer.stop(service_pid)
    Process.exit(cdc_pid, :normal)
  end

  test "snapshot replay is skipped when checkpoint lsn is already at or past boundary" do
    storage = %{provider: :s3, bucket: "bucket", adapter: DuckFeeder.Storage.S3}

    assert {:ok, %{service_pid: service_pid, cdc_pid: cdc_pid, start_lsn: "0/40"}} =
             Runtime.start_stream(:meta_conn, "source-resume", storage,
               meta_module: FakeMeta,
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
    storage = %{provider: :s3, bucket: "bucket", adapter: DuckFeeder.Storage.S3}

    assert {:error, :missing_snapshot_row_handler} =
             Runtime.start_stream(:meta_conn, "source-a", storage,
               meta_module: FakeMeta,
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
    storage = %{provider: :s3, bucket: "bucket", adapter: DuckFeeder.Storage.S3}

    assert {:error, {:source_not_found, "missing"}} =
             Runtime.service_options(:meta_conn, "missing", storage, meta_module: FakeMeta)
  end

  test "returns error when source fields are missing for stream startup" do
    storage = %{provider: :s3, bucket: "bucket", adapter: DuckFeeder.Storage.S3}

    assert {:error, {:missing_source_field, :slot_name}} =
             Runtime.start_stream(:meta_conn, "source-missing-slot", storage,
               meta_module: FakeMeta,
               service_module: FakeService,
               cdc_module: FakeCDC,
               connection_options_module: FakeConnectionOptions,
               observer_pid: self()
             )
  end
end
