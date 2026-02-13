defmodule DuckFeeder.CDC.PipelineTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.CDC.{Event, Pipeline}
  alias DuckFeeder.Ingest

  test "forwards committed transactions to ingest" do
    designated_tables = [
      %{
        id: 1,
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

    {:ok, pipeline} = Pipeline.start_link(ingest_pid: ingest)

    assert :buffering =
             Pipeline.push_event(
               pipeline,
               %Event.Relation{id: 1, schema: "public", table: "users"}
             )

    assert :buffering =
             Pipeline.push_event(
               pipeline,
               %Event.Begin{xid: 500, final_lsn: "0/100"}
             )

    assert :buffering =
             Pipeline.push_event(
               pipeline,
               %Event.Insert{relation_id: 1, record: %{"id" => "1"}}
             )

    assert {:committed, transaction} =
             Pipeline.push_event(
               pipeline,
               %Event.Commit{xid: 500, end_lsn: "0/120"}
             )

    assert transaction.xid == 500
    assert transaction.change_count == 1

    assert_receive {:duck_feeder_batch, {"raw", "users"}, batch}, 300
    assert batch.row_count == 1

    refute Pipeline.in_transaction?(pipeline)
  end

  test "returns errors for invalid event sequences" do
    {:ok, ingest} =
      Ingest.start_link(
        designated_tables: [],
        sink_pid: self(),
        pipeline_opts: %{max_rows: 10, max_bytes: 10_000, flush_interval_ms: 60_000}
      )

    {:ok, pipeline} = Pipeline.start_link(ingest_pid: ingest)

    assert :buffering =
             Pipeline.push_event(
               pipeline,
               %Event.Relation{id: 1, schema: "public", table: "users"}
             )

    assert {:error, :change_outside_transaction} =
             Pipeline.push_event(pipeline, %Event.Insert{relation_id: 1, record: %{}})
  end
end
