defmodule DuckFeeder.AppendStreamIntegrationTest do
  use ExUnit.Case, async: false

  alias DuckFeeder.{AppendStream, Meta}
  alias DuckFeeder.CDC.ConnectionOptions

  @moduletag :integration

  defmodule LocalFilesystemStorage do
    @behaviour DuckFeeder.Storage.Adapter

    @impl true
    def put_file(config, local_path, %{key: key}, _opts) do
      root_dir = Map.fetch!(config, :root_dir)
      destination = Path.join(root_dir, key)

      File.mkdir_p!(Path.dirname(destination))
      File.cp!(local_path, destination)

      {:ok, %{etag: "itest-local-etag", version_id: nil, size: File.stat!(destination).size}}
    end

    @impl true
    def head_object(config, %{key: key}) do
      root_dir = Map.fetch!(config, :root_dir)
      path = Path.join(root_dir, key)

      case File.stat(path) do
        {:ok, stat} -> {:ok, %{size: stat.size}}
        {:error, reason} -> {:error, reason}
      end
    end

    @impl true
    def delete_object(config, %{key: key}) do
      root_dir = Map.fetch!(config, :root_dir)
      path = Path.join(root_dir, key)

      case File.rm(path) do
        :ok -> :ok
        {:error, :enoent} -> :ok
        {:error, reason} -> {:error, reason}
      end
    end
  end

  setup_all do
    integration_config = Application.get_env(:duck_feeder, :integration, [])
    meta_url = Keyword.get(integration_config, :meta_database_url)

    assert is_binary(meta_url) and meta_url != "",
           "set :duck_feeder, :integration, meta_database_url in config/test.exs"

    {:ok, meta_conn_opts} = ConnectionOptions.parse_url(meta_url)
    {:ok, meta_conn} = Postgrex.start_link(meta_conn_opts ++ [types: DuckFeeder.Postgrex.Types])

    assert {:ok, _} =
             Postgrex.query(meta_conn, "DROP SCHEMA IF EXISTS ducklake_metadata CASCADE", [])

    assert {:ok, _} =
             Postgrex.query(meta_conn, "DROP SCHEMA IF EXISTS duckfeeder_meta CASCADE", [])

    assert :ok = Meta.bootstrap(meta_conn)

    on_exit(fn ->
      GenServer.stop(meta_conn)
    end)

    {:ok, meta_conn: meta_conn, meta_url: meta_url}
  end

  test "append stream batches rows and commits DuckLake metadata", %{meta_conn: meta_conn} do
    unique =
      "#{System.system_time(:microsecond)}_#{System.unique_integer([:positive, :monotonic])}"

    source_name = "append_source_#{unique}"
    target_table = "append_events_#{unique}"

    local_data_root =
      Path.join(
        System.tmp_dir!(),
        "duck_feeder_append_trace_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(local_data_root)

    storage = %{
      provider: :s3,
      bucket: "bucket",
      adapter: LocalFilesystemStorage,
      root_dir: local_data_root
    }

    assert {:ok, source_id} =
             Meta.register_source(meta_conn, %{
               name: source_name,
               connection_info: %{"dsn" => "append://local"},
               slot_name: "append_slot_#{unique}",
               publication_name: "append_pub_#{unique}",
               status: "active"
             })

    assert {:ok, designated_table_id} =
             Meta.register_designated_table(meta_conn, %{
               source_id: source_id,
               source_schema: "app",
               source_table: "events",
               target_schema: "raw",
               target_table: target_table,
               mode: "cdc_changelog"
             })

    assert {:ok, designated_tables} = Meta.list_designated_tables(meta_conn, source_id: source_id)

    assert {:ok, stream} =
             AppendStream.start_link(
               designated_tables: designated_tables,
               meta_conn: meta_conn,
               storage: storage,
               writer: %{format: :parquet, datetime_encoding: :unix_microseconds},
               committer_module: DuckFeeder.DuckLake.Committer.Postgres,
               pipeline_opts: %{max_rows: 2, max_bytes: 10_000, flush_interval_ms: 60_000},
               observer_pid: self(),
               object_prefix: source_name
             )

    assert :ok =
             AppendStream.append(stream, target_table, %{
               "kind" => "telemetry",
               "value" => 1,
               "ts" => DateTime.utc_now()
             })

    assert :ok =
             AppendStream.append(stream, target_table, %{
               "kind" => "error",
               "value" => 2,
               "message" => "boom"
             })

    assert_receive {:duck_feeder_append_batch_processed, {"raw", ^target_table}, {:ok, result},
                    batch},
                   10_000

    assert result.status in [:committed, :already_committed]
    assert batch.row_count == 2

    assert {:ok, [%{object_key: object_key}]} = Meta.list_batch_files(meta_conn, result.batch_id)
    assert File.exists?(Path.join(local_data_root, object_key))

    assert {:ok, %{rows: [[path, record_count, file_size_bytes]]}} =
             Postgrex.query(
               meta_conn,
               """
               SELECT path, record_count, file_size_bytes
               FROM ducklake_metadata.ducklake_data_file
               WHERE table_id = $1
               ORDER BY data_file_id DESC
               LIMIT 1
               """,
               [designated_table_id]
             )

    assert path == object_key
    assert record_count == 2
    assert file_size_bytes > 0

    assert {:ok, %{rows: [[stats_record_count, stats_next_row_id]]}} =
             Postgrex.query(
               meta_conn,
               """
               SELECT record_count, next_row_id
               FROM ducklake_metadata.ducklake_table_stats
               WHERE table_id = $1
               """,
               [designated_table_id]
             )

    assert stats_record_count >= 2
    assert stats_next_row_id >= 2

    assert {:ok, %{rows: [[table_column_stats_count]]}} =
             Postgrex.query(
               meta_conn,
               """
               SELECT count(*)
               FROM ducklake_metadata.ducklake_table_column_stats
               WHERE table_id = $1
               """,
               [designated_table_id]
             )

    assert table_column_stats_count >= 1

    assert {:ok, %{rows: [[file_column_stats_count]]}} =
             Postgrex.query(
               meta_conn,
               """
               SELECT count(*)
               FROM ducklake_metadata.ducklake_file_column_stats stats
               JOIN ducklake_metadata.ducklake_data_file files
                 ON files.data_file_id = stats.data_file_id
               WHERE files.table_id = $1
               """,
               [designated_table_id]
             )

    assert file_column_stats_count >= 1

    GenServer.stop(stream)
  end

  test "append stream can write delete-file metadata", %{meta_conn: meta_conn} do
    unique =
      "#{System.system_time(:microsecond)}_#{System.unique_integer([:positive, :monotonic])}"

    source_name = "append_delete_source_#{unique}"
    target_table = "append_delete_events_#{unique}"

    local_data_root =
      Path.join(
        System.tmp_dir!(),
        "duck_feeder_append_delete_trace_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(local_data_root)

    storage = %{
      provider: :s3,
      bucket: "bucket",
      adapter: LocalFilesystemStorage,
      root_dir: local_data_root
    }

    assert {:ok, source_id} =
             Meta.register_source(meta_conn, %{
               name: source_name,
               connection_info: %{"dsn" => "append://local"},
               slot_name: "append_delete_slot_#{unique}",
               publication_name: "append_delete_pub_#{unique}",
               status: "active"
             })

    assert {:ok, designated_table_id} =
             Meta.register_designated_table(meta_conn, %{
               source_id: source_id,
               source_schema: "app",
               source_table: "events",
               target_schema: "raw",
               target_table: target_table,
               mode: "cdc_changelog"
             })

    assert {:ok, designated_tables} = Meta.list_designated_tables(meta_conn, source_id: source_id)

    delete_path = "#{source_name}/raw.#{target_table}/delete-1.parquet"

    assert {:ok, stream} =
             AppendStream.start_link(
               designated_tables: designated_tables,
               meta_conn: meta_conn,
               storage: storage,
               writer: %{format: :parquet, datetime_encoding: :unix_microseconds},
               committer_module: DuckFeeder.DuckLake.Committer.Postgres,
               committer_opts: [
                 delete_files: [
                   %{path: delete_path, delete_count: 1, file_size_bytes: 11}
                 ]
               ],
               pipeline_opts: %{max_rows: 1, max_bytes: 10_000, flush_interval_ms: 60_000},
               observer_pid: self(),
               object_prefix: source_name
             )

    assert :ok = AppendStream.append(stream, target_table, %{"kind" => "first", "value" => 1})

    assert_receive {:duck_feeder_append_batch_processed, {"raw", ^target_table}, {:ok, _result},
                    _batch},
                   10_000

    assert {:ok, %{rows: [[data_file_id]]}} =
             Postgrex.query(
               meta_conn,
               """
               SELECT data_file_id
               FROM ducklake_metadata.ducklake_data_file
               WHERE table_id = $1
               ORDER BY data_file_id DESC
               LIMIT 1
               """,
               [designated_table_id]
             )

    assert {:ok, %{rows: [[delete_file_data_file_id, delete_end_snapshot]]}} =
             Postgrex.query(
               meta_conn,
               """
               SELECT data_file_id, end_snapshot
               FROM ducklake_metadata.ducklake_delete_file
               WHERE path = $1
               ORDER BY delete_file_id DESC
               LIMIT 1
               """,
               [delete_path]
             )

    assert delete_file_data_file_id == data_file_id
    assert is_nil(delete_end_snapshot)

    assert {:ok, %{rows: [[changes_made]]}} =
             Postgrex.query(
               meta_conn,
               """
               SELECT changes_made
               FROM ducklake_metadata.ducklake_snapshot_changes
               ORDER BY snapshot_id DESC
               LIMIT 1
               """,
               []
             )

    assert changes_made =~ "deleted_from_table:#{designated_table_id}"

    GenServer.stop(stream)
  end
end
