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

    {snapshot_sql, snapshot_params} =
      Enum.find(statements, fn {sql, _params} ->
        sql =~ "INSERT INTO ducklake_metadata.ducklake_snapshot"
      end)

    assert snapshot_sql =~ "latest.next_file_id + $3::bigint"
    assert ["batch-1", _column_names_json, 2] = snapshot_params

    {_changes_sql, changes_params} =
      Enum.find(statements, fn {sql, _params} ->
        sql =~ "INSERT INTO ducklake_metadata.ducklake_snapshot_changes"
      end)

    assert ["batch-1", changes_made] = changes_params
    assert changes_made =~ "inserted_into_table:{table_id}"
    assert changes_made =~ "deleted_from_table:{table_id}"
    assert changes_made =~ "compacted_table:{table_id}"
  end

  test "respects custom statement list and function" do
    assert ["SELECT 1"] == SQL.commit_statements("batch-1", ducklake_sql: ["SELECT 1"])

    assert ["SELECT 2"] ==
             SQL.commit_statements("batch-1", ducklake_sql: fn _batch_id -> ["SELECT 2"] end)
  end
end
