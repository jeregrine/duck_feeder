defmodule DuckFeeder.Storage.ProviderRuntimeIntegrationTest do
  use ExUnit.Case, async: false

  alias DuckFeeder.{AppendStream, Meta, Storage}
  alias DuckFeeder.CDC.ConnectionOptions

  @moduletag :provider_integration

  @integration_config Application.compile_env(:duck_feeder, :integration, [])
  @s3_storage Keyword.get(@integration_config, :s3_storage)
  @gcs_storage Keyword.get(@integration_config, :gcs_storage)

  setup_all do
    meta_url = Keyword.get(@integration_config, :meta_database_url)

    assert is_binary(meta_url) and meta_url != "",
           "set :duck_feeder, :integration, meta_database_url in config/test.exs"

    {:ok, meta_conn_opts} = ConnectionOptions.parse_url(meta_url)
    {:ok, meta_conn} = Postgrex.start_link(meta_conn_opts ++ [types: DuckFeeder.Postgrex.Types])

    assert :ok = Meta.bootstrap(meta_conn)

    on_exit(fn ->
      if Process.alive?(meta_conn), do: GenServer.stop(meta_conn)
    end)

    {:ok, meta_conn: meta_conn}
  end

  test "s3 provider append-stream commit path roundtrip", %{meta_conn: meta_conn} do
    assert is_map(@s3_storage), "configure DUCK_FEEDER_ITEST_S3_* env vars"

    assert :ok = run_append_stream_provider_flow(meta_conn, @s3_storage, :s3)
  end

  test "gcs provider append-stream commit path roundtrip", %{meta_conn: meta_conn} do
    assert is_map(@gcs_storage), "configure DUCK_FEEDER_ITEST_GCS_* env vars"

    assert :ok = run_append_stream_provider_flow(meta_conn, @gcs_storage, :gcs)
  end

  defp run_append_stream_provider_flow(meta_conn, storage, provider)
       when provider in [:s3, :gcs] do
    unique =
      "#{System.system_time(:microsecond)}_#{System.unique_integer([:positive, :monotonic])}"

    source_name = "provider_#{provider}_source_#{unique}"
    target_table = "provider_#{provider}_events_#{unique}"

    assert {:ok, source_id} =
             Meta.register_source(meta_conn, %{
               name: source_name,
               connection_info: %{"dsn" => "append://#{provider}"},
               slot_name: "provider_slot_#{unique}",
               publication_name: "provider_pub_#{unique}",
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

    {:ok, stream} =
      AppendStream.start_link(
        designated_tables: designated_tables,
        meta_conn: meta_conn,
        storage: storage,
        writer: %{format: :parquet, datetime_encoding: :unix_microseconds},
        committer_module: DuckFeeder.DuckLake.Committer.Postgres,
        pipeline_opts: %{max_rows: 1_000, max_bytes: 10_000_000, flush_interval_ms: 60_000},
        observer_pid: self(),
        object_prefix: source_name
      )

    try do
      assert :ok =
               AppendStream.append(stream, target_table, %{
                 "kind" => "subscription_created",
                 "value" => 1,
                 "provider" => Atom.to_string(provider)
               })

      assert :ok =
               AppendStream.append(stream, target_table, %{
                 "kind" => "invoice_paid",
                 "value" => 2,
                 "provider" => Atom.to_string(provider)
               })

      assert {:ok, flush_batch} = AppendStream.flush_table(stream, target_table)
      assert flush_batch.row_count == 2

      assert_receive {:duck_feeder_append_batch_processed, {"raw", ^target_table}, {:ok, result},
                      batch},
                     30_000

      assert result.status in [:committed, :already_committed]
      assert batch.row_count == 2

      assert {:ok, [%{object_key: object_key}]} =
               Meta.list_batch_files(meta_conn, result.batch_id)

      assert {:ok, _head} = Storage.head_object(storage, object_key)

      assert {:ok, %{rows: [[path, record_count]]}} =
               Postgrex.query(
                 meta_conn,
                 """
                 SELECT path, record_count
                 FROM ducklake_metadata.ducklake_data_file
                 WHERE table_id = $1
                 ORDER BY data_file_id DESC
                 LIMIT 1
                 """,
                 [designated_table_id]
               )

      assert path == object_key
      assert record_count == 2

      assert :ok = Storage.delete_object(storage, object_key)
      assert :ok = assert_provider_deleted(storage, object_key, provider, 15)

      :ok
    after
      safe_stop(stream)
    end
  end

  defp assert_provider_deleted(_storage, _object_key, _provider, 0),
    do: {:error, :provider_delete_visibility_timeout}

  defp assert_provider_deleted(storage, object_key, provider, attempts_left) do
    case Storage.head_object(storage, object_key) do
      {:error, :not_found} when provider == :s3 ->
        :ok

      {:error, {:gcs_request_failed, 404, _}} when provider == :gcs ->
        :ok

      {:ok, _head} ->
        Process.sleep(100)
        assert_provider_deleted(storage, object_key, provider, attempts_left - 1)

      {:error, _reason} = error ->
        error
    end
  end

  defp safe_stop(pid) when is_pid(pid) do
    if Process.alive?(pid), do: GenServer.stop(pid)
    :ok
  rescue
    _ -> :ok
  catch
    _, _ -> :ok
  end

  defp safe_stop(_), do: :ok
end
