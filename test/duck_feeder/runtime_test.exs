defmodule DuckFeeder.RuntimeTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.Runtime

  defmodule FakeMeta do
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
    def list_designated_tables(_conn, _opts), do: {:ok, []}

    def fetch_source_start_lsn(_conn, 10, _default), do: {:ok, "0/20"}
    def fetch_source_start_lsn(_conn, 11, default), do: {:ok, default}
  end

  defmodule FakeService do
    use GenServer

    def start_link(opts) do
      GenServer.start_link(__MODULE__, opts)
    end

    def push_event(server, event) do
      GenServer.call(server, {:push_event, event})
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

    @impl true
    def handle_info({:duck_feeder_cdc_event, event}, state) do
      if is_pid(state.observer_pid), do: send(state.observer_pid, {:fake_service_event, event})
      {:noreply, state}
    end
  end

  defmodule FakeCDC do
    alias DuckFeeder.CDC.Event

    def start_link(opts) do
      if pid = Process.get(:test_pid), do: send(pid, {:fake_cdc_start, opts})

      event = %Event.Relation{id: 1, schema: "public", table: "users"}

      case opts[:event_sink] do
        pid when is_pid(pid) -> send(pid, {:duck_feeder_cdc_event, event})
        sink when is_function(sink, 1) -> :ok = sink.(event)
      end

      pid = spawn_link(fn -> Process.sleep(:infinity) end)
      {:ok, pid}
    end
  end

  defmodule FakeConnectionOptions do
    def resolve(_source, _opts) do
      {:ok, [hostname: "localhost", port: 5432, database: "db", username: "postgres"]}
    end
  end

  defmodule FakeBootstrap do
    def bootstrap(:query_conn, attrs) do
      if pid = Process.get(:test_pid), do: send(pid, {:fake_bootstrap, attrs})

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
      if pid = Process.get(:test_pid), do: send(pid, {:fake_snapshot_runner, designated_tables})

      :ok = opts[:row_handler].(hd(designated_tables), %{"id" => 1})

      {:ok,
       %{
         snapshot_id: "snap-1",
         boundary_lsn: "0/35",
         table_counts: %{{"public", "users"} => 1}
       }}
    end
  end

  setup do
    Process.put(:test_pid, self())
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

  test "returns error when snapshot mode is enabled without row handler" do
    storage = %{provider: :s3, bucket: "bucket", adapter: DuckFeeder.Storage.S3}

    assert {:error, :missing_snapshot_row_handler} =
             Runtime.start_stream(:meta_conn, "source-a", storage,
               meta_module: FakeMeta,
               service_module: FakeService,
               cdc_module: FakeCDC,
               connection_options_module: FakeConnectionOptions,
               bootstrap_replication?: false,
               snapshot_before_stream?: true,
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
