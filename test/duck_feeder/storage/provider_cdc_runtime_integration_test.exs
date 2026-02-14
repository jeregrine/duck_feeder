defmodule DuckFeeder.Storage.ProviderCDCRuntimeIntegrationTest do
  use ExUnit.Case, async: false

  alias DuckFeeder.{Meta, Runtime, Storage}
  alias DuckFeeder.CDC.{ConnectionOptions, Setup}

  @moduletag :provider_integration

  @integration_config Application.compile_env(:duck_feeder, :integration, [])
  @s3_storage Keyword.get(@integration_config, :s3_storage)
  @gcs_storage Keyword.get(@integration_config, :gcs_storage)

  setup_all do
    meta_url = Keyword.get(@integration_config, :meta_database_url)
    source_url = Keyword.get(@integration_config, :source_database_url)

    assert is_binary(meta_url) and meta_url != "" and is_binary(source_url) and source_url != "",
           "set :duck_feeder, :integration, meta_database_url/source_database_url in config/test.exs"

    {:ok, meta_conn_opts} = ConnectionOptions.parse_url(meta_url)
    {:ok, source_conn_opts} = ConnectionOptions.parse_url(source_url)

    {:ok, meta_conn} = Postgrex.start_link(meta_conn_opts ++ [types: DuckFeeder.Postgrex.Types])

    {:ok, source_conn} =
      Postgrex.start_link(source_conn_opts ++ [types: DuckFeeder.Postgrex.Types])

    assert :ok = Meta.bootstrap(meta_conn)

    on_exit(fn ->
      safe_stop(source_conn)
      safe_stop(meta_conn)
    end)

    {:ok, meta_conn: meta_conn, source_conn: source_conn, source_url: source_url}
  end

  test "s3 provider runtime cdc end-to-end path", %{
    meta_conn: meta_conn,
    source_conn: source_conn,
    source_url: source_url
  } do
    assert is_map(@s3_storage), "configure DUCK_FEEDER_ITEST_S3_* env vars"

    assert :ok = run_runtime_provider_flow(meta_conn, source_conn, source_url, @s3_storage, :s3)
  end

  test "gcs provider runtime cdc end-to-end path", %{
    meta_conn: meta_conn,
    source_conn: source_conn,
    source_url: source_url
  } do
    assert is_map(@gcs_storage), "configure DUCK_FEEDER_ITEST_GCS_* env vars"

    assert :ok = run_runtime_provider_flow(meta_conn, source_conn, source_url, @gcs_storage, :gcs)
  end

  defp run_runtime_provider_flow(meta_conn, source_conn, source_url, storage, provider)
       when provider in [:s3, :gcs] do
    unique =
      "#{System.system_time(:microsecond)}_#{System.unique_integer([:positive, :monotonic])}"

    source_name = "provider_#{provider}_runtime_source_#{unique}"
    source_table = "provider_#{provider}_runtime_users_#{unique}"
    target_table = source_table
    slot_name = "provider_#{provider}_runtime_slot_#{unique}"
    publication_name = "provider_#{provider}_runtime_pub_#{unique}"

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

    try do
      Process.sleep(200)

      assert {:ok, _} =
               Postgrex.query(
                 source_conn,
                 "INSERT INTO public.\"#{source_table}\" (id, name) VALUES (1, 'duck')",
                 []
               )

      assert_receive {:duck_feeder_batch_processed, {"raw", ^target_table}, {:ok, result}, batch},
                     30_000

      assert result.status in [:committed, :already_committed]
      assert batch.row_count == 1

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
      assert record_count == 1

      assert :ok = Storage.delete_object(storage, object_key)
      assert :ok = assert_provider_deleted(storage, object_key, provider, 15)

      :ok
    after
      safe_stop(cdc_pid)
      safe_stop(service_pid)
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
