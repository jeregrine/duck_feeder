defmodule DuckFeeder.DuckLake.SQLTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.DuckLake.SQL

  test "returns default DuckLake commit statements when object key exists" do
    statements =
      SQL.commit_statements("batch-1",
        object_key: "raw/users/file-1.parquet",
        write_result: %{row_count: 10, file_size_bytes: 1024},
        batch: %{rows: [%{"id" => 1, "name" => "duck"}]}
      )

    assert length(statements) >= 8

    assert Enum.any?(statements, fn {sql, _params} ->
             sql =~ "INSERT INTO ducklake_metadata.ducklake_snapshot"
           end)

    assert Enum.any?(statements, fn {sql, _params} ->
             sql =~ "INSERT INTO ducklake_metadata.ducklake_table"
           end)

    assert Enum.any?(statements, fn {sql, _params} ->
             sql =~ "SELECT 1 /" and sql =~ "incoming_type"
           end)

    assert Enum.any?(statements, fn {sql, _params} ->
             sql =~ "INSERT INTO ducklake_metadata.ducklake_column"
           end)

    assert Enum.any?(statements, fn {sql, _params} ->
             sql =~ "INSERT INTO ducklake_metadata.ducklake_data_file"
           end)

    assert Enum.any?(statements, fn {sql, _params} ->
             sql =~ "INSERT INTO ducklake_metadata.ducklake_table_stats"
           end)

    assert Enum.any?(statements, fn {sql, _params} ->
             sql =~ "INSERT INTO ducklake_metadata.ducklake_table_column_stats"
           end)

    assert Enum.any?(statements, fn {sql, _params} ->
             sql =~ "INSERT INTO ducklake_metadata.ducklake_file_column_stats"
           end)

    assert Enum.any?(statements, fn {sql, _params} ->
             sql =~ "INSERT INTO ducklake_metadata.ducklake_snapshot_changes"
           end)

    {snapshot_changes_sql, _} =
      Enum.find(statements, fn {sql, _params} ->
        sql =~ "INSERT INTO ducklake_metadata.ducklake_snapshot_changes"
      end)

    assert snapshot_changes_sql =~ "created_table:"
    assert snapshot_changes_sql =~ "altered_table:"

    assert Enum.any?(statements, fn {sql, _params} ->
             sql =~ "INSERT INTO duckfeeder_meta.schema_history"
           end)

    assert Enum.any?(statements, fn {sql, _params} ->
             sql =~ "INSERT INTO duckfeeder_meta.ducklake_commits"
           end)
  end

  test "returns empty list when default statement lacks object key" do
    assert [] == SQL.commit_statements("batch-1")
  end

  test "can disable commit-log statement" do
    statements =
      SQL.commit_statements("batch-1",
        object_key: "raw/users/file-1.parquet",
        write_result: %{row_count: 10, file_size_bytes: 1024},
        batch: %{rows: [%{"id" => 1}]},
        include_commit_log?: false
      )

    refute Enum.any?(statements, fn {sql, _params} ->
             sql =~ "INSERT INTO duckfeeder_meta.ducklake_commits"
           end)
  end

  test "adds delete-file and replacement metadata statements when configured" do
    statements =
      SQL.commit_statements("batch-1",
        object_key: "raw/users/file-1.parquet",
        write_result: %{row_count: 10, file_size_bytes: 1024},
        batch: %{rows: [%{"id" => 1, "name" => "duck"}]},
        delete_files: [
          %{
            path: "raw/users/file-1-deletes.parquet",
            data_file_id: 77,
            delete_count: 3,
            file_size_bytes: 64
          }
        ],
        replace_data_file_ids: [77, 55]
      )

    assert Enum.any?(statements, fn {sql, _params} ->
             sql =~ "INSERT INTO ducklake_metadata.ducklake_delete_file"
           end)

    assert Enum.any?(statements, fn {sql, _params} ->
             sql =~ "UPDATE ducklake_metadata.ducklake_data_file"
           end)

    assert Enum.any?(statements, fn {sql, _params} ->
             sql =~ "UPDATE ducklake_metadata.ducklake_delete_file"
           end)

    assert Enum.any?(statements, fn {sql, _params} ->
             sql =~ "INSERT INTO ducklake_metadata.ducklake_files_scheduled_for_deletion"
           end)

    {snapshot_sql, snapshot_params} =
      Enum.find(statements, fn {sql, _params} ->
        sql =~ "INSERT INTO ducklake_metadata.ducklake_snapshot"
      end)

    assert snapshot_sql =~ "latest.next_file_id + $3::bigint"
    assert ["batch-1", _column_names, 2, false] = snapshot_params

    {_changes_sql, changes_params} =
      Enum.find(statements, fn {sql, _params} ->
        sql =~ "INSERT INTO ducklake_metadata.ducklake_snapshot_changes"
      end)

    assert ["batch-1", changes_made] = changes_params
    assert changes_made =~ "inserted_into_table:{table_id}"
    assert changes_made =~ "deleted_from_table:{table_id}"
    assert changes_made =~ "compacted_table:{table_id}"
  end

  test "adds schema-change statements and forces schema version bump" do
    statements =
      SQL.commit_statements("batch-1",
        object_key: "raw/users/file-1.parquet",
        write_result: %{row_count: 1, file_size_bytes: 10},
        batch: %{rows: [%{"value" => 1}]},
        schema_changes: [
          %{op: :rename_table, from: "users", to: "users_v2"},
          %{op: :alter_column_type, column: "value", type: "double"},
          %{op: :drop_column, column: "legacy"},
          %{op: :rename_column, from: "value", to: "metric"}
        ]
      )

    assert Enum.any?(statements, fn {sql, _params} ->
             sql =~ "UPDATE ducklake_metadata.ducklake_table table_entry" and
               sql =~ "table_entry.table_name <>"
           end)

    assert Enum.any?(statements, fn {sql, _params} ->
             sql =~ "SELECT 1 /" and sql =~ "can_promote"
           end)

    assert Enum.any?(statements, fn {sql, _params} ->
             sql =~ "INSERT INTO ducklake_metadata.ducklake_column" and
               sql =~ "previous.column_id"
           end)

    assert Enum.any?(statements, fn {sql, _params} ->
             sql =~ "DELETE FROM ducklake_metadata.ducklake_table_column_stats"
           end)

    assert Enum.any?(statements, fn {sql, _params} ->
             sql =~ "UPDATE ducklake_metadata.ducklake_name_mapping" and sql =~ "SET source_name"
           end)

    {snapshot_sql, snapshot_params} =
      Enum.find(statements, fn {sql, _params} ->
        sql =~ "INSERT INTO ducklake_metadata.ducklake_snapshot"
      end)

    assert snapshot_sql =~ "OR $4::boolean"
    assert ["batch-1", _column_names, 1, true] = snapshot_params

    {_changes_sql, ["batch-1", changes_made]} =
      Enum.find(statements, fn {sql, _params} ->
        sql =~ "INSERT INTO ducklake_metadata.ducklake_snapshot_changes"
      end)

    assert changes_made =~ "altered_table:{table_id}"
  end

  test "adds partition metadata statements when configured" do
    statements =
      SQL.commit_statements("batch-1",
        object_key: "raw/users/file-1.parquet",
        write_result: %{row_count: 2, file_size_bytes: 1024},
        batch: %{rows: [%{"id" => 1, "tenant_id" => "acme"}]},
        partition_by: ["tenant_id"],
        partition_values: %{"tenant_id" => "acme"}
      )

    assert Enum.any?(statements, fn {sql, _params} ->
             sql =~ "INSERT INTO ducklake_metadata.ducklake_partition_info"
           end)

    assert Enum.any?(statements, fn {sql, _params} ->
             sql =~ "INSERT INTO ducklake_metadata.ducklake_partition_column"
           end)

    assert Enum.any?(statements, fn {sql, _params} ->
             sql =~ "INSERT INTO ducklake_metadata.ducklake_file_partition_value"
           end)

    {_data_file_sql, data_file_params} =
      Enum.find(statements, fn {sql, _params} ->
        sql =~ "INSERT INTO ducklake_metadata.ducklake_data_file"
      end)

    assert ["batch-1", _object_key, _row_count, _file_size, _delete_count, partition_signature] =
             data_file_params

    assert is_binary(partition_signature)
    assert String.contains?(partition_signature, "tenant_id")
  end

  test "supports nested-field style schema_changes aliases" do
    statements =
      SQL.commit_statements("batch-1",
        object_key: "raw/users/file-1.parquet",
        write_result: %{row_count: 1, file_size_bytes: 10},
        batch: %{rows: [%{"payload.user_name" => "alice", "payload.age" => 20}]},
        schema_changes: [
          %{op: :rename_field, from_path: "payload.user_name", to_path: "payload.name"},
          %{op: :drop_field, path: "payload.legacy"},
          %{op: :alter_field_type, path: "payload.age", type: "BIGINT"}
        ]
      )

    assert Enum.any?(statements, fn {_sql, params} ->
             params == ["batch-1", "payload.user_name", "payload.name"]
           end)

    assert Enum.any?(statements, fn {_sql, params} ->
             params == ["batch-1", "payload.legacy"]
           end)

    assert Enum.any?(statements, fn {_sql, params} ->
             params == ["batch-1", "payload.age", "BIGINT"]
           end)

    {snapshot_sql, snapshot_params} =
      Enum.find(statements, fn {sql, _params} ->
        sql =~ "INSERT INTO ducklake_metadata.ducklake_snapshot"
      end)

    assert snapshot_sql =~ "OR $4::boolean"
    assert ["batch-1", _column_names, 1, true] = snapshot_params
  end

  test "respects custom statement list and function" do
    assert ["SELECT 1"] == SQL.commit_statements("batch-1", ducklake_sql: ["SELECT 1"])

    assert ["SELECT 2"] ==
             SQL.commit_statements("batch-1", ducklake_sql: fn _batch_id -> ["SELECT 2"] end)
  end
end
