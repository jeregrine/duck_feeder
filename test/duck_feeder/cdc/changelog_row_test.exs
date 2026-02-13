defmodule DuckFeeder.CDC.ChangelogRowTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.CDC.ChangelogRow

  test "builds changelog row with metadata" do
    change = %{
      op: :update,
      relation: {"public", "users"},
      record: %{"id" => "1", "name" => "new"},
      old_record: %{"id" => "1", "name" => "old"}
    }

    transaction = %{xid: 10, end_lsn: "0/120", commit_timestamp: ~U[2026-01-01 00:00:00Z]}

    row = ChangelogRow.from_change(change, transaction, ingest_ts: ~U[2026-01-01 00:00:10Z])

    assert row._op == "U"
    assert row._commit_lsn == "0/120"
    assert row._xid == 10
    assert row._source_ts == ~U[2026-01-01 00:00:00Z]
    assert row._ingest_ts == ~U[2026-01-01 00:00:10Z]
    assert row._relation_schema == "public"
    assert row._relation_table == "users"
    assert row._record["name"] == "new"
    assert row._old_record["name"] == "old"
  end

  test "op_code fallback for unknown op" do
    assert ChangelogRow.op_code(:merge) == "merge"
    assert ChangelogRow.op_code("I") == "I"
  end
end
