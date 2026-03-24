defmodule DuckFeeder.IngestTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.Ingest

  test "routes transaction changes to designated table pipelines" do
    designated_tables = [
      %{
        checkpoint_key: "source-a:raw.users",
        source_schema: "public",
        source_table: "users",
        target_schema: "raw",
        target_table: "users"
      }
    ]

    {:ok, ingest} =
      Ingest.start_link(
        designated_tables: designated_tables,
        sink_pid: self(),
        pipeline_opts: %{max_rows: 1, max_bytes: 10_000, flush_interval_ms: 60_000}
      )

    transaction = %{
      xid: 100,
      begin_lsn: "0/100",
      end_lsn: "0/120",
      changes: [
        %{op: :insert, relation: {"public", "users"}, record: %{"id" => "1"}},
        %{op: :delete, relation: {"public", "orders"}, old_record: %{"id" => "9"}}
      ]
    }

    :ok = Ingest.ingest_transaction(ingest, transaction)

    assert_receive {:duck_feeder_batch, {"raw", "users"}, batch}, 300

    assert batch.row_count == 1
    assert [row] = batch.rows
    assert row.checkpoint_key == "source-a:raw.users"
    assert row._op == "I"
    assert row._commit_lsn == "0/120"
    assert row._xid == 100
  end

  test "flush_table flushes buffered rows" do
    designated_tables = [
      %{
        checkpoint_key: "source-a:raw.events",
        source_schema: "public",
        source_table: "events",
        target_schema: "raw",
        target_table: "events"
      }
    ]

    {:ok, ingest} =
      Ingest.start_link(
        designated_tables: designated_tables,
        sink_pid: self(),
        pipeline_opts: %{max_rows: 100, max_bytes: 10_000, flush_interval_ms: 60_000}
      )

    transaction = %{
      xid: 200,
      begin_lsn: "0/200",
      end_lsn: "0/220",
      changes: [%{op: :insert, relation: {"public", "events"}, record: %{"id" => "1"}}]
    }

    :ok = Ingest.ingest_transaction(ingest, transaction)
    :timer.sleep(20)

    assert {:ok, pipeline_pid} = Ingest.table_pipeline(ingest, {"raw", "events"})
    assert is_pid(pipeline_pid)

    assert {:ok, batch} = Ingest.flush_table(ingest, {"raw", "events"})
    assert batch.row_count == 1

    assert_receive {:duck_feeder_batch, {"raw", "events"}, _batch}, 200
  end
end
