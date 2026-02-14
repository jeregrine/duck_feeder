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

  test "respects custom statement list and function" do
    assert ["SELECT 1"] == SQL.commit_statements("batch-1", ducklake_sql: ["SELECT 1"])

    assert ["SELECT 2"] ==
             SQL.commit_statements("batch-1", ducklake_sql: fn _batch_id -> ["SELECT 2"] end)
  end
end
