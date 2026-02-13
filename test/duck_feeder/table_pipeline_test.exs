defmodule DuckFeeder.TablePipelineTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.TablePipeline

  test "emits batch when row threshold is hit" do
    {:ok, pid} =
      TablePipeline.start_link(
        table: {"raw", "users"},
        max_rows: 2,
        max_bytes: 10_000,
        flush_interval_ms: 60_000,
        sink_pid: self()
      )

    :ok = TablePipeline.append(pid, %{"id" => 1}, "0/10")
    :ok = TablePipeline.append(pid, %{"id" => 2}, "0/11")

    assert_receive {:duck_feeder_batch, {"raw", "users"}, batch}, 200

    assert batch.row_count == 2
    assert batch.lsn_start == "0/10"
    assert batch.lsn_end == "0/11"
  end

  test "flushes by interval" do
    {:ok, pid} =
      TablePipeline.start_link(
        table: {"raw", "events"},
        max_rows: 10,
        max_bytes: 10_000,
        flush_interval_ms: 30,
        sink_pid: self()
      )

    :ok = TablePipeline.append(pid, %{"id" => 1}, "0/20")

    assert_receive {:duck_feeder_batch, {"raw", "events"}, batch}, 400
    assert batch.row_count == 1
  end

  test "manual flush returns batch and clears buffer" do
    {:ok, pid} =
      TablePipeline.start_link(
        table: {"raw", "orders"},
        max_rows: 10,
        max_bytes: 10_000,
        flush_interval_ms: 60_000,
        sink_pid: self()
      )

    :ok = TablePipeline.append(pid, %{"id" => 1}, "0/31")

    assert {:ok, batch} = TablePipeline.flush(pid)
    assert batch.row_count == 1

    stats = TablePipeline.stats(pid)
    assert stats.row_count == 0

    assert :empty = TablePipeline.flush(pid)
  end
end
