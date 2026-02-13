defmodule DuckFeeder.DuckLake.SQLTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.DuckLake.SQL

  test "returns default spec-aligned + commit-log statements when object key exists" do
    statements =
      SQL.commit_statements("batch-1",
        object_key: "raw/users/file-1.parquet",
        write_result: %{row_count: 10, file_size_bytes: 1024}
      )

    assert [
             {spec_sql, ["batch-1", "raw/users/file-1.parquet", 10, 1024]},
             {stats_sql, ["batch-1"]},
             {log_sql, ["batch-1", "raw/users/file-1.parquet", 10, 1024]}
           ] = statements

    assert spec_sql =~ "INSERT INTO ducklake_metadata.ducklake_snapshot"
    assert spec_sql =~ "INSERT INTO ducklake_metadata.ducklake_data_file"
    assert spec_sql =~ "INSERT INTO ducklake_metadata.ducklake_snapshot_changes"

    assert stats_sql =~ "INSERT INTO ducklake_metadata.ducklake_table_stats"

    assert log_sql =~ "INSERT INTO duckfeeder_meta.ducklake_commits"
  end

  test "returns empty list when default statement lacks object key" do
    assert [] == SQL.commit_statements("batch-1")
  end

  test "can disable commit-log statement" do
    statements =
      SQL.commit_statements("batch-1",
        object_key: "raw/users/file-1.parquet",
        write_result: %{row_count: 10, file_size_bytes: 1024},
        include_commit_log?: false
      )

    assert [
             {spec_sql, ["batch-1", "raw/users/file-1.parquet", 10, 1024]},
             {stats_sql, ["batch-1"]}
           ] = statements

    assert spec_sql =~ "ducklake_metadata.ducklake_snapshot"
    assert stats_sql =~ "ducklake_metadata.ducklake_table_stats"
  end

  test "respects custom statement list and function" do
    assert ["SELECT 1"] == SQL.commit_statements("batch-1", ducklake_sql: ["SELECT 1"])

    assert ["SELECT 2"] ==
             SQL.commit_statements("batch-1", ducklake_sql: fn _batch_id -> ["SELECT 2"] end)
  end
end
