defmodule DuckFeeder.CDC.SnapshotBoundaryTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.CDC.SnapshotBoundary

  test "tags snapshot row metadata" do
    row = %{"id" => 1}

    tagged =
      SnapshotBoundary.tag_snapshot_row(
        row,
        "0/100",
        xid: 10,
        source_ts: ~U[2026-01-01 00:00:00Z],
        ingest_ts: ~U[2026-01-01 00:00:10Z]
      )

    assert tagged._op == "R"
    assert tagged._commit_lsn == "0/100"
    assert tagged._xid == 10
    assert tagged._source_ts == ~U[2026-01-01 00:00:00Z]
    assert tagged._ingest_ts == ~U[2026-01-01 00:00:10Z]
  end
end
