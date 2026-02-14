defmodule DuckFeeder.DuckLake.Committer.PostgresIntegrationTest do
  use ExUnit.Case, async: false

  alias DuckFeeder.Meta
  alias DuckFeeder.CDC.ConnectionOptions
  alias DuckFeeder.DuckLake.Committer.Postgres

  @moduletag :integration

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

    {:ok, meta_conn: meta_conn}
  end

  test "replacement commit retires prior active data and delete files", %{meta_conn: meta_conn} do
    unique =
      "#{System.system_time(:microsecond)}_#{System.unique_integer([:positive, :monotonic])}"

    source_name = "ducklake_replace_source_#{unique}"
    target_table = "ducklake_replace_table_#{unique}"

    assert {:ok, source_id} =
             Meta.register_source(meta_conn, %{
               name: source_name,
               connection_info: %{"dsn" => "replace://local"},
               slot_name: "slot_#{unique}",
               publication_name: "pub_#{unique}",
               status: "active"
             })

    assert {:ok, designated_table_id} =
             Meta.register_designated_table(meta_conn, %{
               source_id: source_id,
               source_schema: "public",
               source_table: "events",
               target_schema: "raw",
               target_table: target_table,
               mode: "cdc_changelog"
             })

    batch_1_id = "batch_1_#{unique}"
    assert :ok = insert_uploaded_batch(meta_conn, batch_1_id, designated_table_id, "0/1", "0/1")

    delete_path_1 = "raw/#{target_table}/delete-1.parquet"

    assert {:ok, %{batch_id: ^batch_1_id, committed?: true}} =
             Postgres.commit_batch(meta_conn, batch_1_id,
               object_key: "raw/#{target_table}/data-1.parquet",
               write_result: %{row_count: 1, file_size_bytes: 100},
               batch: %{rows: [%{"kind" => "first", "value" => 1}]},
               delete_files: [%{path: delete_path_1, delete_count: 1, file_size_bytes: 10}]
             )

    assert {:ok, %{rows: [[first_data_file_id]]}} =
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

    assert {:ok, %{rows: [[first_delete_end_snapshot]]}} =
             Postgrex.query(
               meta_conn,
               """
               SELECT end_snapshot
               FROM ducklake_metadata.ducklake_delete_file
               WHERE path = $1
               ORDER BY delete_file_id DESC
               LIMIT 1
               """,
               [delete_path_1]
             )

    assert is_nil(first_delete_end_snapshot)

    batch_2_id = "batch_2_#{unique}"
    assert :ok = insert_uploaded_batch(meta_conn, batch_2_id, designated_table_id, "0/2", "0/2")

    assert {:ok, %{batch_id: ^batch_2_id, committed?: true}} =
             Postgres.commit_batch(meta_conn, batch_2_id,
               object_key: "raw/#{target_table}/data-2.parquet",
               write_result: %{row_count: 1, file_size_bytes: 90},
               batch: %{rows: [%{"kind" => "second", "value" => 2}]},
               replace_data_file_ids: [first_data_file_id],
               table_stats_row_delta: 0
             )

    assert {:ok, %{rows: [[first_data_file_end_snapshot]]}} =
             Postgrex.query(
               meta_conn,
               """
               SELECT end_snapshot
               FROM ducklake_metadata.ducklake_data_file
               WHERE data_file_id = $1
               """,
               [first_data_file_id]
             )

    refute is_nil(first_data_file_end_snapshot)

    assert {:ok, %{rows: [[first_delete_file_end_snapshot]]}} =
             Postgrex.query(
               meta_conn,
               """
               SELECT end_snapshot
               FROM ducklake_metadata.ducklake_delete_file
               WHERE path = $1
               ORDER BY delete_file_id DESC
               LIMIT 1
               """,
               [delete_path_1]
             )

    refute is_nil(first_delete_file_end_snapshot)

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

    assert changes_made =~ "compacted_table:#{designated_table_id}"
    refute changes_made =~ "deleted_from_table:#{designated_table_id}"
  end

  defp insert_uploaded_batch(meta_conn, batch_id, designated_table_id, lsn_start, lsn_end) do
    with {:ok, _} <-
           Meta.insert_batch(meta_conn, %{
             batch_id: batch_id,
             designated_table_id: designated_table_id,
             lsn_start: lsn_start,
             lsn_end: lsn_end,
             state: :pending
           }),
         {:ok, _} <- Meta.transition_batch(meta_conn, batch_id, :encoded),
         {:ok, _} <- Meta.transition_batch(meta_conn, batch_id, :uploaded) do
      :ok
    else
      {:error, reason} -> {:error, reason}
    end
  end
end
