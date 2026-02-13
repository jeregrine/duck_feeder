defmodule DuckFeeder.RuntimeStreamIntegrationTest do
  use ExUnit.Case, async: false

  alias DuckFeeder.{Meta, Runtime}
  alias DuckFeeder.CDC.Setup

  @meta_url System.get_env("DUCK_FEEDER_META_DATABASE_URL")
  @source_url System.get_env("DUCK_FEEDER_SOURCE_DATABASE_URL")

  @moduletag :integration

  @moduletag skip:
               if(is_nil(@meta_url) or is_nil(@source_url),
                 do: "set DUCK_FEEDER_META_DATABASE_URL and DUCK_FEEDER_SOURCE_DATABASE_URL",
                 else: false
               )

  defmodule FakeStorage do
    @behaviour DuckFeeder.Storage.Adapter

    @impl true
    def put_file(_config, _local_path, _object_ref, _opts),
      do: {:ok, %{etag: "itest-etag", version_id: nil, size: 1}}

    @impl true
    def head_object(_config, _object_ref), do: {:ok, %{}}

    @impl true
    def delete_object(_config, _object_ref), do: :ok
  end

  setup_all do
    {:ok, meta_conn} = Postgrex.start_link(url: @meta_url)
    assert :ok = Meta.bootstrap(meta_conn)

    {:ok, source_conn} = Postgrex.start_link(url: @source_url)

    on_exit(fn ->
      GenServer.stop(source_conn)
      GenServer.stop(meta_conn)
    end)

    {:ok, meta_conn: meta_conn, source_conn: source_conn}
  end

  setup %{meta_conn: meta_conn, source_conn: source_conn} do
    unique = System.unique_integer([:positive, :monotonic])
    source_name = "runtime_source_#{unique}"
    source_table = "runtime_users_#{unique}"
    target_table = source_table
    slot_name = "runtime_slot_#{unique}"
    publication_name = "runtime_pub_#{unique}"

    assert {:ok, _} =
             Postgrex.query(
               source_conn,
               "CREATE TABLE public.\"#{source_table}\" (id integer PRIMARY KEY, name text)",
               []
             )

    assert {:ok, _} =
             Postgrex.query(
               source_conn,
               "ALTER TABLE public.\"#{source_table}\" REPLICA IDENTITY FULL",
               []
             )

    assert {:ok, source_id} =
             Meta.register_source(meta_conn, %{
               name: source_name,
               connection_info: %{"dsn" => @source_url},
               slot_name: slot_name,
               publication_name: publication_name,
               status: "active"
             })

    assert {:ok, designated_table_id} =
             Meta.register_designated_table(meta_conn, %{
               source_id: source_id,
               source_schema: "public",
               source_table: source_table,
               target_schema: "raw",
               target_table: target_table,
               mode: "cdc_changelog"
             })

    on_exit(fn ->
      _ = Setup.drop_slot(source_conn, slot_name)
      _ = Postgrex.query(source_conn, "DROP PUBLICATION IF EXISTS \"#{publication_name}\"", [])
      _ = Postgrex.query(source_conn, "DROP TABLE IF EXISTS public.\"#{source_table}\"", [])
    end)

    {:ok,
     source_name: source_name,
     source_table: source_table,
     target_table: target_table,
     designated_table_id: designated_table_id}
  end

  test "start_stream processes source inserts end-to-end", %{
    meta_conn: meta_conn,
    source_conn: source_conn,
    source_name: source_name,
    source_table: source_table,
    target_table: target_table,
    designated_table_id: designated_table_id
  } do
    storage = %{provider: :s3, bucket: "bucket", adapter: FakeStorage}

    assert {:ok, %{service_pid: service_pid, cdc_pid: cdc_pid}} =
             Runtime.start_stream(meta_conn, source_name, storage,
               observer_pid: self(),
               pipeline_opts: %{max_rows: 1, max_bytes: 10_000, flush_interval_ms: 60_000},
               bootstrap_replication?: true,
               auto_reconnect: false,
               sync_connect: true
             )

    Process.sleep(150)

    assert {:ok, _} =
             Postgrex.query(
               source_conn,
               "INSERT INTO public.\"#{source_table}\" (id, name) VALUES (1, 'duck')",
               []
             )

    assert_receive {:duck_feeder_batch_processed, {"raw", ^target_table}, {:ok, result}, batch},
                   10_000

    assert result.status in [:committed, :already_committed]
    assert batch.row_count == 1

    assert {:ok, checkpoint} = Meta.fetch_checkpoint(meta_conn, designated_table_id)
    assert checkpoint != "0/0"

    GenServer.stop(cdc_pid)
    GenServer.stop(service_pid)
  end

  test "wal cdc to parquet upload and ducklake metadata commit", %{
    meta_conn: meta_conn,
    source_conn: source_conn,
    source_name: source_name,
    source_table: source_table,
    target_table: target_table,
    designated_table_id: designated_table_id
  } do
    storage = %{provider: :s3, bucket: "bucket", adapter: FakeStorage}

    assert {:ok, %{service_pid: service_pid, cdc_pid: cdc_pid}} =
             Runtime.start_stream(meta_conn, source_name, storage,
               observer_pid: self(),
               writer: %{format: :parquet},
               committer_module: DuckFeeder.DuckLake.Committer.Postgres,
               pipeline_opts: %{max_rows: 1, max_bytes: 10_000, flush_interval_ms: 60_000},
               bootstrap_replication?: true,
               auto_reconnect: false,
               sync_connect: true
             )

    Process.sleep(150)

    assert {:ok, _} =
             Postgrex.query(
               source_conn,
               "INSERT INTO public.\"#{source_table}\" (id, name) VALUES (2, 'goose')",
               []
             )

    assert_receive {:duck_feeder_batch_processed, {"raw", ^target_table}, {:ok, result}, _batch},
                   10_000

    assert result.status in [:committed, :already_committed]
    assert String.ends_with?(result.object_key, ".parquet")

    assert {:ok, batch_files} = Meta.list_batch_files(meta_conn, result.batch_id)
    assert [%{object_key: object_key}] = batch_files
    assert String.ends_with?(object_key, ".parquet")

    assert {:ok, %{rows: [[snapshot_count]]}} =
             Postgrex.query(
               meta_conn,
               "SELECT count(*) FROM ducklake_metadata.ducklake_snapshot WHERE table_id = $1",
               [designated_table_id]
             )

    assert snapshot_count >= 1

    assert {:ok, %{rows: [[commit_count]]}} =
             Postgrex.query(
               meta_conn,
               "SELECT count(*) FROM duckfeeder_meta.ducklake_commits WHERE designated_table_id = $1",
               [designated_table_id]
             )

    assert commit_count >= 1

    GenServer.stop(cdc_pid)
    GenServer.stop(service_pid)
  end
end
