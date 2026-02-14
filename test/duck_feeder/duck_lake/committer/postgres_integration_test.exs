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

    assert {:ok, %{rows: [[scheduled_file_id, scheduled_path, scheduled_relative]]}} =
             Postgrex.query(
               meta_conn,
               """
               SELECT data_file_id, path, path_is_relative
               FROM ducklake_metadata.ducklake_files_scheduled_for_deletion
               WHERE data_file_id = $1
               LIMIT 1
               """,
               [first_data_file_id]
             )

    assert scheduled_file_id == first_data_file_id
    assert scheduled_path == "raw/#{target_table}/data-1.parquet"
    assert scheduled_relative == true
  end

  test "snapshot changes include created_table and altered_table markers for schema evolution", %{
    meta_conn: meta_conn
  } do
    unique =
      "#{System.system_time(:microsecond)}_#{System.unique_integer([:positive, :monotonic])}"

    source_name = "ducklake_schema_source_#{unique}"
    target_table = "ducklake_schema_table_#{unique}"

    assert {:ok, source_id} =
             Meta.register_source(meta_conn, %{
               name: source_name,
               connection_info: %{"dsn" => "schema://local"},
               slot_name: "slot_schema_#{unique}",
               publication_name: "pub_schema_#{unique}",
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

    batch_1_id = "batch_schema_1_#{unique}"
    assert :ok = insert_uploaded_batch(meta_conn, batch_1_id, designated_table_id, "0/10", "0/10")

    assert {:ok, %{batch_id: ^batch_1_id, committed?: true}} =
             Postgres.commit_batch(meta_conn, batch_1_id,
               object_key: "raw/#{target_table}/schema-1.parquet",
               write_result: %{row_count: 1, file_size_bytes: 50},
               batch: %{rows: [%{"kind" => "first", "value" => 1}]}
             )

    assert {:ok, %{rows: [[first_changes]]}} =
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

    assert first_changes =~ ~s(created_table:"#{target_table}")

    assert {:ok, %{rows: [[schema_version_after_first]]}} =
             Postgrex.query(
               meta_conn,
               """
               SELECT schema_version
               FROM ducklake_metadata.ducklake_snapshot
               ORDER BY snapshot_id DESC
               LIMIT 1
               """,
               []
             )

    batch_2_id = "batch_schema_2_#{unique}"
    assert :ok = insert_uploaded_batch(meta_conn, batch_2_id, designated_table_id, "0/11", "0/11")

    assert {:ok, %{batch_id: ^batch_2_id, committed?: true}} =
             Postgres.commit_batch(meta_conn, batch_2_id,
               object_key: "raw/#{target_table}/schema-2.parquet",
               write_result: %{row_count: 1, file_size_bytes: 50},
               batch: %{rows: [%{"kind" => "second", "value" => 2, "extra" => "new-column"}]}
             )

    assert {:ok, %{rows: [[second_changes]]}} =
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

    assert second_changes =~ "altered_table:#{designated_table_id}"

    assert {:ok, %{rows: [[schema_version_after_second]]}} =
             Postgrex.query(
               meta_conn,
               """
               SELECT schema_version
               FROM ducklake_metadata.ducklake_snapshot
               ORDER BY snapshot_id DESC
               LIMIT 1
               """,
               []
             )

    assert schema_version_after_second > schema_version_after_first
  end

  test "schema_changes supports rename/drop/type-change metadata updates", %{meta_conn: meta_conn} do
    unique =
      "#{System.system_time(:microsecond)}_#{System.unique_integer([:positive, :monotonic])}"

    source_name = "ducklake_schema_change_source_#{unique}"
    target_table = "ducklake_schema_change_table_#{unique}"

    assert {:ok, source_id} =
             Meta.register_source(meta_conn, %{
               name: source_name,
               connection_info: %{"dsn" => "schema-change://local"},
               slot_name: "slot_schema_change_#{unique}",
               publication_name: "pub_schema_change_#{unique}",
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

    batch_1_id = "batch_schema_change_1_#{unique}"

    assert :ok =
             insert_uploaded_batch(meta_conn, batch_1_id, designated_table_id, "0/20", "0/20")

    assert {:ok, %{batch_id: ^batch_1_id, committed?: true}} =
             Postgres.commit_batch(meta_conn, batch_1_id,
               object_key: "raw/#{target_table}/schema-change-1.parquet",
               write_result: %{row_count: 1, file_size_bytes: 60},
               batch: %{rows: [%{"kind" => "first", "value" => 1, "legacy" => "x"}]}
             )

    batch_2_id = "batch_schema_change_2_#{unique}"

    assert :ok =
             insert_uploaded_batch(meta_conn, batch_2_id, designated_table_id, "0/21", "0/21")

    assert {:ok, %{batch_id: ^batch_2_id, committed?: true}} =
             Postgres.commit_batch(meta_conn, batch_2_id,
               object_key: "raw/#{target_table}/schema-change-2.parquet",
               write_result: %{row_count: 1, file_size_bytes: 65},
               batch: %{rows: [%{"event_kind" => "second", "value" => 2}]},
               schema_changes: [
                 %{op: :rename_column, from: "kind", to: "event_kind"},
                 %{op: :drop_column, column: "legacy"},
                 %{op: :alter_column_type, column: "value", type: "BIGINT"}
               ]
             )

    assert {:ok, %{rows: [[kind_active_count]]}} =
             Postgrex.query(
               meta_conn,
               """
               SELECT count(*)
               FROM ducklake_metadata.ducklake_column
               WHERE table_id = $1
                 AND column_name = 'kind'
                 AND end_snapshot IS NULL
               """,
               [designated_table_id]
             )

    assert kind_active_count == 0

    assert {:ok, %{rows: [[kind_historical_count]]}} =
             Postgrex.query(
               meta_conn,
               """
               SELECT count(*)
               FROM ducklake_metadata.ducklake_column
               WHERE table_id = $1
                 AND column_name = 'kind'
                 AND end_snapshot IS NOT NULL
               """,
               [designated_table_id]
             )

    assert kind_historical_count >= 1

    assert {:ok, %{rows: [[event_kind_active_count]]}} =
             Postgrex.query(
               meta_conn,
               """
               SELECT count(*)
               FROM ducklake_metadata.ducklake_column
               WHERE table_id = $1
                 AND column_name = 'event_kind'
                 AND end_snapshot IS NULL
               """,
               [designated_table_id]
             )

    assert event_kind_active_count == 1

    assert {:ok, %{rows: [[legacy_active_count]]}} =
             Postgrex.query(
               meta_conn,
               """
               SELECT count(*)
               FROM ducklake_metadata.ducklake_column
               WHERE table_id = $1
                 AND column_name = 'legacy'
                 AND end_snapshot IS NULL
               """,
               [designated_table_id]
             )

    assert legacy_active_count == 0

    assert {:ok, %{rows: [[value_type]]}} =
             Postgrex.query(
               meta_conn,
               """
               SELECT column_type
               FROM ducklake_metadata.ducklake_column
               WHERE table_id = $1
                 AND column_name = 'value'
                 AND end_snapshot IS NULL
               LIMIT 1
               """,
               [designated_table_id]
             )

    assert value_type == "BIGINT"

    assert {:ok, %{rows: [[kind_column_id, event_kind_column_id]]}} =
             Postgrex.query(
               meta_conn,
               """
               SELECT
                 (SELECT column_id
                  FROM ducklake_metadata.ducklake_column
                  WHERE table_id = $1
                    AND column_name = 'kind'
                  ORDER BY begin_snapshot DESC
                  LIMIT 1),
                 (SELECT column_id
                  FROM ducklake_metadata.ducklake_column
                  WHERE table_id = $1
                    AND column_name = 'event_kind'
                    AND end_snapshot IS NULL
                  LIMIT 1)
               """,
               [designated_table_id]
             )

    assert kind_column_id == event_kind_column_id

    assert {:ok, %{rows: [[value_version_count, value_historical_count]]}} =
             Postgrex.query(
               meta_conn,
               """
               SELECT count(*),
                      count(*) FILTER (WHERE end_snapshot IS NOT NULL)
               FROM ducklake_metadata.ducklake_column
               WHERE table_id = $1
                 AND column_name = 'value'
               """,
               [designated_table_id]
             )

    assert value_version_count >= 2
    assert value_historical_count >= 1

    assert {:ok, %{rows: [[name_mapping_count]]}} =
             Postgrex.query(
               meta_conn,
               """
               SELECT count(*)
               FROM ducklake_metadata.ducklake_name_mapping
               WHERE mapping_id = $1
                 AND source_name = 'event_kind'
               """,
               [designated_table_id]
             )

    assert name_mapping_count == 1

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

    assert changes_made =~ "altered_table:#{designated_table_id}"
  end

  test "schema_changes rejects non-promotable type changes", %{meta_conn: meta_conn} do
    unique =
      "#{System.system_time(:microsecond)}_#{System.unique_integer([:positive, :monotonic])}"

    source_name = "ducklake_schema_conflict_source_#{unique}"
    target_table = "ducklake_schema_conflict_table_#{unique}"

    assert {:ok, source_id} =
             Meta.register_source(meta_conn, %{
               name: source_name,
               connection_info: %{"dsn" => "schema-conflict://local"},
               slot_name: "slot_schema_conflict_#{unique}",
               publication_name: "pub_schema_conflict_#{unique}",
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

    batch_1_id = "batch_schema_conflict_1_#{unique}"

    assert :ok =
             insert_uploaded_batch(meta_conn, batch_1_id, designated_table_id, "0/30", "0/30")

    assert {:ok, %{batch_id: ^batch_1_id, committed?: true}} =
             Postgres.commit_batch(meta_conn, batch_1_id,
               object_key: "raw/#{target_table}/schema-conflict-1.parquet",
               write_result: %{row_count: 1, file_size_bytes: 60},
               batch: %{rows: [%{"value" => 100}]}
             )

    batch_2_id = "batch_schema_conflict_2_#{unique}"

    assert :ok =
             insert_uploaded_batch(meta_conn, batch_2_id, designated_table_id, "0/31", "0/31")

    assert {:error, {:ducklake_sql_failed, sql, _postgres_error}} =
             Postgres.commit_batch(meta_conn, batch_2_id,
               object_key: "raw/#{target_table}/schema-conflict-2.parquet",
               write_result: %{row_count: 1, file_size_bytes: 60},
               batch: %{rows: [%{"value" => 1}]},
               schema_changes: [
                 %{op: :alter_column_type, column: "value", type: "INTEGER"}
               ]
             )

    assert sql =~ "can_promote"
    assert {:ok, :uploaded} = Meta.get_batch_state(meta_conn, batch_2_id)
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
