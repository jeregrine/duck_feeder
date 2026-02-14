defmodule DuckFeeder.RuntimeStreamIntegrationTest do
  use ExUnit.Case, async: false

  alias DuckFeeder.{Meta, Reconciler, Runtime}
  alias DuckFeeder.CDC.{ConnectionOptions, Lsn, Setup}

  @moduletag :integration

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

  defmodule FailingCommitter do
    @behaviour DuckFeeder.DuckLake.Committer

    @impl true
    def commit_batch(_meta_conn, _batch_id, _opts), do: {:error, :forced_commit_failure}
  end

  defmodule FakeCDCFailStart do
    def start_link(_opts), do: {:error, :failed_to_start_cdc}
  end

  defmodule FilteredMeta do
    def list_stale_batches(conn, opts) do
      designated_table_id = Keyword.get(opts, :designated_table_id)

      case DuckFeeder.Meta.list_stale_batches(conn, opts) do
        {:ok, batches} when is_integer(designated_table_id) ->
          {:ok, Enum.filter(batches, &(&1.designated_table_id == designated_table_id))}

        other ->
          other
      end
    end

    def list_batch_files(conn, batch_id), do: DuckFeeder.Meta.list_batch_files(conn, batch_id)

    def transition_batch(conn, batch_id, state, opts),
      do: DuckFeeder.Meta.transition_batch(conn, batch_id, state, opts)

    def commit_uploaded_batch(conn, batch_id),
      do: DuckFeeder.Meta.commit_uploaded_batch(conn, batch_id)
  end

  setup_all do
    integration_config = Application.get_env(:duck_feeder, :integration, [])
    meta_url = Keyword.get(integration_config, :meta_database_url)
    source_url = Keyword.get(integration_config, :source_database_url)

    assert is_binary(meta_url) and meta_url != "" and is_binary(source_url) and source_url != "",
           "set :duck_feeder, :integration, meta_database_url/source_database_url in config/test.exs"

    {:ok, meta_conn_opts} = ConnectionOptions.parse_url(meta_url)
    {:ok, source_conn_opts} = ConnectionOptions.parse_url(source_url)

    {:ok, meta_conn} = Postgrex.start_link(meta_conn_opts ++ [types: DuckFeeder.Postgrex.Types])

    assert {:ok, _} =
             Postgrex.query(meta_conn, "DROP SCHEMA IF EXISTS ducklake_metadata CASCADE", [])

    assert {:ok, _} =
             Postgrex.query(meta_conn, "DROP SCHEMA IF EXISTS duckfeeder_meta CASCADE", [])

    assert :ok = Meta.bootstrap(meta_conn)

    {:ok, source_conn} =
      Postgrex.start_link(source_conn_opts ++ [types: DuckFeeder.Postgrex.Types])

    on_exit(fn ->
      GenServer.stop(source_conn)
      GenServer.stop(meta_conn)
    end)

    {:ok,
     meta_conn: meta_conn, source_conn: source_conn, meta_url: meta_url, source_url: source_url}
  end

  setup %{meta_conn: meta_conn, source_conn: source_conn, source_url: source_url} do
    unique =
      "#{System.system_time(:microsecond)}_#{System.unique_integer([:positive, :monotonic])}"

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
               connection_info: %{"dsn" => source_url},
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
     source_id: source_id,
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
    assert [%{_op: "I", _record: %{"id" => "1", "name" => "duck"}}] = batch.rows

    assert {:ok, checkpoint} = Meta.fetch_checkpoint(meta_conn, designated_table_id)
    assert checkpoint != "0/0"

    GenServer.stop(cdc_pid)
    GenServer.stop(service_pid)
  end

  test "snapshot before stream replays existing rows then continues WAL without duplicate replay",
       %{
         meta_conn: meta_conn,
         source_conn: source_conn,
         source_name: source_name,
         source_table: source_table,
         target_table: target_table
       } do
    storage = %{provider: :s3, bucket: "bucket", adapter: FakeStorage}

    assert {:ok, _} =
             Postgrex.query(
               source_conn,
               "INSERT INTO public.\"#{source_table}\" (id, name) VALUES (1, 'preexisting')",
               []
             )

    assert {:ok, %{service_pid: service_pid, cdc_pid: cdc_pid}} =
             Runtime.start_stream(meta_conn, source_name, storage,
               observer_pid: self(),
               pipeline_opts: %{max_rows: 1, max_bytes: 10_000, flush_interval_ms: 60_000},
               snapshot_before_stream?: true,
               bootstrap_replication?: true,
               auto_reconnect: false,
               sync_connect: true
             )

    assert_receive {:duck_feeder_batch_processed, {"raw", ^target_table}, {:ok, _snapshot_result},
                    snapshot_batch},
                   10_000

    assert snapshot_batch.row_count == 1

    assert batch_has_record_id?(snapshot_batch, 1)

    assert {:ok, _} =
             Postgrex.query(
               source_conn,
               "INSERT INTO public.\"#{source_table}\" (id, name) VALUES (2, 'post_snapshot')",
               []
             )

    assert_receive {:duck_feeder_batch_processed, {"raw", ^target_table}, {:ok, _cdc_result},
                    cdc_batch},
                   10_000

    assert cdc_batch.row_count == 1
    assert batch_has_record_id?(cdc_batch, 2)

    refute batch_has_record_id?(cdc_batch, 1)

    GenServer.stop(cdc_pid)
    GenServer.stop(service_pid)
  end

  test "snapshot before stream handles larger snapshots across multiple batches", %{
    meta_conn: meta_conn,
    source_conn: source_conn,
    source_name: source_name,
    source_table: source_table,
    target_table: target_table
  } do
    storage = %{provider: :s3, bucket: "bucket", adapter: FakeStorage}
    snapshot_row_count = 25

    assert {:ok, _} =
             Postgrex.query(
               source_conn,
               """
               INSERT INTO public."#{source_table}" (id, name)
               SELECT g, concat('snapshot_', g)
               FROM generate_series(1, $1::int) AS g
               """,
               [snapshot_row_count]
             )

    assert {:ok, %{service_pid: service_pid, cdc_pid: cdc_pid}} =
             Runtime.start_stream(meta_conn, source_name, storage,
               observer_pid: self(),
               pipeline_opts: %{max_rows: 5, max_bytes: 10_000, flush_interval_ms: 60_000},
               snapshot_before_stream?: true,
               bootstrap_replication?: true,
               auto_reconnect: false,
               sync_connect: true
             )

    expected_ids =
      1..snapshot_row_count
      |> Enum.map(&to_string/1)
      |> MapSet.new()

    assert {:ok, snapshot_ids} =
             await_processed_ids(target_table, expected_ids, MapSet.new(), 15_000)

    assert snapshot_ids == expected_ids

    assert {:ok, _} =
             Postgrex.query(
               source_conn,
               """
               INSERT INTO public."#{source_table}" (id, name)
               SELECT g, concat('post_snapshot_', g)
               FROM generate_series(1000, 1004) AS g
               """,
               []
             )

    assert_receive {:duck_feeder_batch_processed, {"raw", ^target_table}, {:ok, _cdc_result},
                    cdc_batch},
                   10_000

    assert Enum.all?(1000..1004, &batch_has_record_id?(cdc_batch, &1))
    refute Enum.any?(batch_record_ids(cdc_batch), &MapSet.member?(expected_ids, &1))

    GenServer.stop(cdc_pid)
    GenServer.stop(service_pid)
  end

  test "restart after snapshot handoff does not replay already-committed snapshot rows", %{
    meta_conn: meta_conn,
    source_conn: source_conn,
    source_name: source_name,
    source_table: source_table,
    target_table: target_table,
    designated_table_id: designated_table_id
  } do
    storage = %{provider: :s3, bucket: "bucket", adapter: FakeStorage}

    assert {:ok, _} =
             Postgrex.query(
               source_conn,
               "INSERT INTO public.\"#{source_table}\" (id, name) VALUES (1, 'preexisting')",
               []
             )

    assert {:ok, %{service_pid: service_pid_1, cdc_pid: cdc_pid_1, start_lsn: start_lsn_1}} =
             Runtime.start_stream(meta_conn, source_name, storage,
               observer_pid: self(),
               pipeline_opts: %{max_rows: 1, max_bytes: 10_000, flush_interval_ms: 60_000},
               snapshot_before_stream?: true,
               bootstrap_replication?: true,
               auto_reconnect: false,
               sync_connect: true
             )

    assert_receive {:duck_feeder_batch_processed, {"raw", ^target_table}, {:ok, _snapshot_result},
                    snapshot_batch_1},
                   10_000

    assert batch_has_record_id?(snapshot_batch_1, 1)

    assert {:ok, checkpoint_after_first_start} =
             Meta.fetch_checkpoint(meta_conn, designated_table_id)

    assert Lsn.compare(checkpoint_after_first_start, start_lsn_1) in [:eq, :gt]

    GenServer.stop(cdc_pid_1)
    GenServer.stop(service_pid_1)

    assert {:ok, %{service_pid: service_pid_2, cdc_pid: cdc_pid_2, start_lsn: start_lsn_2}} =
             Runtime.start_stream(meta_conn, source_name, storage,
               observer_pid: self(),
               pipeline_opts: %{max_rows: 1, max_bytes: 10_000, flush_interval_ms: 60_000},
               snapshot_before_stream?: true,
               bootstrap_replication?: true,
               auto_reconnect: false,
               sync_connect: true
             )

    assert Lsn.compare(start_lsn_2, checkpoint_after_first_start) in [:eq, :gt]

    refute_receive {:duck_feeder_batch_processed, {"raw", ^target_table}, {:ok, _},
                    _replay_batch},
                   750

    assert {:ok, _} =
             Postgrex.query(
               source_conn,
               "INSERT INTO public.\"#{source_table}\" (id, name) VALUES (2, 'after_restart')",
               []
             )

    assert_receive {:duck_feeder_batch_processed, {"raw", ^target_table}, {:ok, _cdc_result},
                    cdc_batch_2},
                   10_000

    assert batch_has_record_id?(cdc_batch_2, 2)
    refute batch_has_record_id?(cdc_batch_2, 1)

    GenServer.stop(cdc_pid_2)
    GenServer.stop(service_pid_2)
  end

  test "cdc start failure after snapshot leaves pending handoff until explicit resume", %{
    meta_conn: meta_conn,
    source_conn: source_conn,
    source_id: source_id,
    source_name: source_name,
    source_table: source_table,
    target_table: target_table
  } do
    storage = %{provider: :s3, bucket: "bucket", adapter: FakeStorage}

    assert {:ok, _} =
             Postgrex.query(
               source_conn,
               "INSERT INTO public.\"#{source_table}\" (id, name) VALUES (1, 'preexisting')",
               []
             )

    assert {:error, :failed_to_start_cdc} =
             Runtime.start_stream(meta_conn, source_name, storage,
               observer_pid: self(),
               cdc_module: FakeCDCFailStart,
               pipeline_opts: %{max_rows: 1, max_bytes: 10_000, flush_interval_ms: 60_000},
               snapshot_before_stream?: true,
               bootstrap_replication?: true,
               auto_reconnect: false,
               sync_connect: true
             )

    assert {:ok, %{state: :pending}} = Meta.fetch_snapshot_handoff(meta_conn, source_id)

    assert {:error, {:snapshot_handoff_incomplete, %{source_id: ^source_id, state: :pending}}} =
             Runtime.start_stream(meta_conn, source_name, storage,
               observer_pid: self(),
               pipeline_opts: %{max_rows: 1, max_bytes: 10_000, flush_interval_ms: 60_000},
               snapshot_before_stream?: true,
               bootstrap_replication?: true,
               auto_reconnect: false,
               sync_connect: true
             )

    assert {:ok, %{service_pid: service_pid, cdc_pid: cdc_pid}} =
             Runtime.start_stream(meta_conn, source_name, storage,
               observer_pid: self(),
               pipeline_opts: %{max_rows: 1, max_bytes: 10_000, flush_interval_ms: 60_000},
               snapshot_before_stream?: true,
               resume_incomplete_snapshot?: true,
               bootstrap_replication?: true,
               auto_reconnect: false,
               sync_connect: true
             )

    assert {:ok, %{state: :complete}} = Meta.fetch_snapshot_handoff(meta_conn, source_id)

    assert {:ok, _} =
             Postgrex.query(
               source_conn,
               "INSERT INTO public.\"#{source_table}\" (id, name) VALUES (2, 'after_resume')",
               []
             )

    assert {:ok, cdc_batch} = await_batch_with_record_id(target_table, 2, 10_000)

    assert batch_has_record_id?(cdc_batch, 2)

    GenServer.stop(cdc_pid)
    GenServer.stop(service_pid)
  end

  test "pending snapshot handoff requires explicit resume flag", %{
    meta_conn: meta_conn,
    source_conn: source_conn,
    source_id: source_id,
    source_name: source_name,
    source_table: source_table,
    target_table: target_table
  } do
    storage = %{provider: :s3, bucket: "bucket", adapter: FakeStorage}

    assert {:ok, _} =
             Postgrex.query(
               source_conn,
               "INSERT INTO public.\"#{source_table}\" (id, name) VALUES (1, 'preexisting')",
               []
             )

    assert {:ok, "0/10"} = Meta.mark_snapshot_handoff_pending(meta_conn, source_id, "0/10")

    assert {:error, {:snapshot_handoff_incomplete, %{source_id: ^source_id, state: :pending}}} =
             Runtime.start_stream(meta_conn, source_name, storage,
               observer_pid: self(),
               pipeline_opts: %{max_rows: 1, max_bytes: 10_000, flush_interval_ms: 60_000},
               snapshot_before_stream?: true,
               bootstrap_replication?: true,
               auto_reconnect: false,
               sync_connect: true
             )

    assert {:ok, %{service_pid: service_pid, cdc_pid: cdc_pid}} =
             Runtime.start_stream(meta_conn, source_name, storage,
               observer_pid: self(),
               pipeline_opts: %{max_rows: 1, max_bytes: 10_000, flush_interval_ms: 60_000},
               snapshot_before_stream?: true,
               resume_incomplete_snapshot?: true,
               bootstrap_replication?: true,
               auto_reconnect: false,
               sync_connect: true
             )

    assert_receive {:duck_feeder_batch_processed, {"raw", ^target_table}, {:ok, _},
                    snapshot_batch},
                   10_000

    assert batch_has_record_id?(snapshot_batch, 1)
    assert {:ok, %{state: :complete}} = Meta.fetch_snapshot_handoff(meta_conn, source_id)

    GenServer.stop(cdc_pid)
    GenServer.stop(service_pid)
  end

  test "wal cdc to parquet upload and ducklake metadata commit", %{
    meta_conn: meta_conn,
    meta_url: meta_url,
    source_conn: source_conn,
    source_name: source_name,
    source_table: source_table,
    target_table: target_table,
    designated_table_id: designated_table_id
  } do
    local_data_root =
      Path.join(
        System.tmp_dir!(),
        "duck_feeder_ducklake_trace_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(local_data_root)

    storage = %{
      provider: :s3,
      bucket: "bucket",
      adapter: LocalFilesystemStorage,
      root_dir: local_data_root
    }

    assert {:ok, %{service_pid: service_pid, cdc_pid: cdc_pid}} =
             Runtime.start_stream(meta_conn, source_name, storage,
               observer_pid: self(),
               writer: %{format: :parquet, datetime_encoding: :unix_microseconds},
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

    local_parquet_path = Path.join(local_data_root, object_key)
    assert File.exists?(local_parquet_path)

    assert {:ok, %{rows: [[snapshot_count]]}} =
             Postgrex.query(
               meta_conn,
               "SELECT count(*) FROM ducklake_metadata.ducklake_snapshot",
               []
             )

    assert snapshot_count >= 2

    assert {:ok, %{rows: [[commit_count]]}} =
             Postgrex.query(
               meta_conn,
               "SELECT count(*) FROM duckfeeder_meta.ducklake_commits WHERE designated_table_id = $1",
               [designated_table_id]
             )

    assert commit_count >= 1

    assert {:ok, %{rows: [[ducklake_path, ducklake_file_size, ducklake_record_count]]}} =
             Postgrex.query(
               meta_conn,
               """
               SELECT data_file.path, data_file.file_size_bytes, data_file.record_count
               FROM ducklake_metadata.ducklake_data_file data_file
               WHERE data_file.table_id = $1
               ORDER BY data_file.data_file_id DESC
               LIMIT 1
               """,
               [designated_table_id]
             )

    assert ducklake_path == object_key
    assert ducklake_file_size > 0
    assert ducklake_record_count == 1

    assert {:ok, %{rows: [[stats_record_count, stats_next_row_id, stats_file_size_bytes]]}} =
             Postgrex.query(
               meta_conn,
               """
               SELECT record_count, next_row_id, file_size_bytes
               FROM ducklake_metadata.ducklake_table_stats
               WHERE table_id = $1
               """,
               [designated_table_id]
             )

    assert stats_record_count >= 1
    assert stats_next_row_id >= 1
    assert stats_file_size_bytes > 0

    assert {:ok, %{rows: [[table_column_stats_count]]}} =
             Postgrex.query(
               meta_conn,
               """
               SELECT count(*)
               FROM ducklake_metadata.ducklake_table_column_stats stats
               JOIN ducklake_metadata.ducklake_column col
                 ON col.table_id = stats.table_id
                AND col.column_id = stats.column_id
               WHERE stats.table_id = $1
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

    trace_unique =
      "#{System.system_time(:microsecond)}_#{System.unique_integer([:positive, :monotonic])}"

    trace_schema = "ducklake_trace_#{designated_table_id}_#{trace_unique}"
    trace_catalog = "metadata_trace_#{designated_table_id}_#{trace_unique}"
    trace_table = "users_trace_#{designated_table_id}_#{trace_unique}"

    ducklake_sql =
      [
        "INSTALL postgres;",
        "LOAD postgres;",
        "INSTALL ducklake;",
        "LOAD ducklake;",
        "ATTACH 'ducklake:",
        ducklake_postgres_connection(meta_url),
        "' AS dl (",
        "DATA_PATH '",
        String.replace(local_data_root, "'", "''"),
        "', METADATA_SCHEMA '",
        trace_schema,
        "', METADATA_CATALOG '",
        trace_catalog,
        "');",
        "CREATE TABLE dl.main.",
        trace_table,
        " AS SELECT * FROM read_parquet('",
        String.replace(local_parquet_path, "'", "''"),
        "') LIMIT 0;",
        "CALL ducklake_add_data_files('dl', '",
        trace_table,
        "', '",
        String.replace(local_parquet_path, "'", "''"),
        "');",
        "SELECT count(*) AS row_count FROM dl.main.",
        trace_table,
        ";",
        "COPY (",
        "SELECT _op, _record FROM dl.main.",
        trace_table,
        " ORDER BY _op",
        ") TO STDOUT (FORMAT CSV, HEADER);",
        "COPY (",
        "SELECT typeof(_xid) AS xid_type, typeof(_op) AS op_type, typeof(_ingest_ts) AS ingest_ts_type FROM dl.main.",
        trace_table,
        " LIMIT 1",
        ") TO STDOUT (FORMAT CSV, HEADER);"
      ]
      |> IO.iodata_to_binary()

    assert {duckdb_output, 0} = System.cmd("duckdb", ["-c", ducklake_sql], stderr_to_stdout: true)
    assert duckdb_output =~ "row_count"
    assert duckdb_output =~ "1"
    assert duckdb_output =~ "_op,_record"
    assert duckdb_output =~ "I,"
    assert duckdb_output =~ "goose"
    assert duckdb_output =~ "xid_type,op_type,ingest_ts_type"
    assert duckdb_output =~ "BIGINT,VARCHAR,BIGINT"

    GenServer.stop(cdc_pid)
    GenServer.stop(service_pid)
  end

  test "reconciler errors on failed batch without files when strict mode enabled", %{
    meta_conn: meta_conn,
    designated_table_id: designated_table_id
  } do
    batch_id = Meta.build_batch_id(designated_table_id, "0/10", "0/10", [99])

    assert {:ok, %{state: :failed}} =
             Meta.insert_batch(meta_conn, %{
               batch_id: batch_id,
               designated_table_id: designated_table_id,
               lsn_start: "0/10",
               lsn_end: "0/10",
               state: :failed
             })

    assert {:ok, summary} =
             Reconciler.reconcile(
               %{
                 meta_conn: meta_conn,
                 meta_module: FilteredMeta,
                 storage: %{
                   provider: :s3,
                   bucket: "bucket",
                   adapter: LocalFilesystemStorage,
                   root_dir: System.tmp_dir!()
                 }
               },
               states: [:failed],
               designated_table_id: designated_table_id,
               cleanup_failed_uploads?: true,
               require_failed_batch_files?: true,
               stale_before: DateTime.add(DateTime.utc_now(), 60, :second)
             )

    assert summary.retried == []
    assert summary.errors == [{batch_id, {:missing_batch_files, batch_id}}]
    assert {:ok, :failed} = Meta.get_batch_state(meta_conn, batch_id)
  end

  test "reconciler cleans failed uploaded batch and resets it to pending", %{
    meta_conn: meta_conn,
    source_conn: source_conn,
    source_name: source_name,
    source_table: source_table,
    target_table: target_table,
    designated_table_id: designated_table_id
  } do
    local_data_root =
      Path.join(
        System.tmp_dir!(),
        "duck_feeder_reconcile_trace_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(local_data_root)

    storage = %{
      provider: :s3,
      bucket: "bucket",
      adapter: LocalFilesystemStorage,
      root_dir: local_data_root
    }

    assert {:ok, %{service_pid: service_pid, cdc_pid: cdc_pid}} =
             Runtime.start_stream(meta_conn, source_name, storage,
               observer_pid: self(),
               writer: %{format: :parquet, datetime_encoding: :unix_microseconds},
               committer_module: FailingCommitter,
               pipeline_opts: %{max_rows: 1, max_bytes: 10_000, flush_interval_ms: 60_000},
               bootstrap_replication?: true,
               auto_reconnect: false,
               sync_connect: true
             )

    Process.sleep(150)

    assert {:ok, _} =
             Postgrex.query(
               source_conn,
               "INSERT INTO public.\"#{source_table}\" (id, name) VALUES (3, 'heron')",
               []
             )

    assert_receive {:duck_feeder_batch_processed, {"raw", ^target_table},
                    {:error, :forced_commit_failure}, _batch},
                   10_000

    assert {:ok, %{rows: [[batch_id, state]]}} =
             Postgrex.query(
               meta_conn,
               """
               SELECT batch_id, state
               FROM duckfeeder_meta.batches
               WHERE designated_table_id = $1
               ORDER BY inserted_at DESC
               LIMIT 1
               """,
               [designated_table_id]
             )

    assert state == "failed"

    assert {:ok, [%{object_key: object_key}]} = Meta.list_batch_files(meta_conn, batch_id)
    local_parquet_path = Path.join(local_data_root, object_key)
    assert File.exists?(local_parquet_path)

    assert {:ok, summary} =
             Reconciler.reconcile(
               %{
                 meta_conn: meta_conn,
                 meta_module: FilteredMeta,
                 storage: storage
               },
               states: [:failed],
               designated_table_id: designated_table_id,
               cleanup_failed_uploads?: true,
               stale_before: DateTime.add(DateTime.utc_now(), 60, :second)
             )

    assert summary.retried == [batch_id]
    assert summary.errors == []

    refute File.exists?(local_parquet_path)
    assert {:ok, :pending} = Meta.get_batch_state(meta_conn, batch_id)

    GenServer.stop(cdc_pid)
    GenServer.stop(service_pid)
  end

  defp await_processed_ids(target_table, expected_ids, acc, timeout_ms)
       when is_binary(target_table) and is_integer(timeout_ms) and timeout_ms > 0 do
    if MapSet.size(expected_ids) == MapSet.size(acc) do
      {:ok, acc}
    else
      receive do
        {:duck_feeder_batch_processed, {"raw", ^target_table}, {:ok, _result}, batch} ->
          updated_acc =
            batch
            |> batch_record_ids()
            |> MapSet.intersection(expected_ids)
            |> MapSet.union(acc)

          await_processed_ids(target_table, expected_ids, updated_acc, timeout_ms)
      after
        timeout_ms ->
          {:error, {:missing_processed_ids, MapSet.difference(expected_ids, acc)}}
      end
    end
  end

  defp batch_record_ids(batch) when is_map(batch) do
    batch.rows
    |> Enum.map(fn row ->
      row
      |> Map.get(:_record, %{})
      |> Map.get("id")
      |> to_string()
    end)
    |> MapSet.new()
  end

  defp await_batch_with_record_id(target_table, record_id, timeout_ms)
       when is_binary(target_table) and is_integer(timeout_ms) and timeout_ms > 0 do
    expected = to_string(record_id)

    receive do
      {:duck_feeder_batch_processed, {"raw", ^target_table}, {:ok, _}, batch} ->
        if MapSet.member?(batch_record_ids(batch), expected) do
          {:ok, batch}
        else
          await_batch_with_record_id(target_table, record_id, timeout_ms)
        end
    after
      timeout_ms ->
        {:error, {:missing_batch_with_record_id, expected}}
    end
  end

  defp batch_has_record_id?(batch, id) when is_map(batch) do
    expected = to_string(id)

    Enum.any?(batch.rows, fn row ->
      row_id =
        row
        |> Map.get(:_record, %{})
        |> Map.get("id")
        |> to_string()

      row_id == expected
    end)
  end

  defp ducklake_postgres_connection(url) do
    uri = URI.parse(url)

    userinfo = uri.userinfo || ""

    {username, password} =
      case String.split(userinfo, ":", parts: 2) do
        [u, p] -> {u, p}
        [u] -> {u, ""}
        _ -> {"", ""}
      end

    dbname = String.trim_leading(uri.path || "", "/")

    [
      "postgres:",
      "dbname=",
      dbname,
      " ",
      "host=",
      uri.host || "localhost",
      " ",
      "port=",
      Integer.to_string(uri.port || 5432),
      " ",
      "user=",
      username,
      " ",
      "password=",
      password
    ]
    |> IO.iodata_to_binary()
  end
end
