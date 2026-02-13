defmodule DuckFeeder.DuckLake.SQLTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.DuckLake.SQL

  test "returns default commit log statement when object key exists" do
    statements =
      SQL.commit_statements("batch-1",
        object_key: "raw/users/file-1.parquet",
        write_result: %{row_count: 10, file_size_bytes: 1024}
      )

    assert [{sql, ["batch-1", "raw/users/file-1.parquet", 10, 1024]}] = statements
    assert sql =~ "INSERT INTO duckfeeder_meta.ducklake_commits"
  end

  test "returns empty list when default statement lacks object key" do
    assert [] == SQL.commit_statements("batch-1")
  end

  test "respects custom statement list and function" do
    assert ["SELECT 1"] == SQL.commit_statements("batch-1", ducklake_sql: ["SELECT 1"])

    assert ["SELECT 2"] ==
             SQL.commit_statements("batch-1", ducklake_sql: fn _batch_id -> ["SELECT 2"] end)
  end
end
