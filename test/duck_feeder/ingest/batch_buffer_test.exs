defmodule DuckFeeder.Ingest.BatchBufferTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.Ingest.BatchBuffer

  test "flushes on max_rows threshold" do
    state = BatchBuffer.new(max_rows: 2, max_bytes: 10_000, flush_interval_ms: 60_000)

    assert {:ok, state} =
             BatchBuffer.append(state, %{"id" => 1}, "0/10", row_size: 10, now_mono_ms: 100)

    assert {:flush, batch, state} =
             BatchBuffer.append(state, %{"id" => 2}, "0/11", row_size: 10, now_mono_ms: 101)

    assert batch.row_count == 2
    assert batch.byte_count == 20
    assert batch.lsn_start == "0/10"
    assert batch.lsn_end == "0/11"
    assert Enum.map(batch.rows, & &1["id"]) == [1, 2]

    assert BatchBuffer.empty?(state)
  end

  test "flushes on max_bytes threshold" do
    state = BatchBuffer.new(max_rows: 10, max_bytes: 15, flush_interval_ms: 60_000)

    assert {:ok, state} =
             BatchBuffer.append(state, %{"id" => 1}, "0/10", row_size: 10, now_mono_ms: 100)

    assert {:flush, batch, _state} =
             BatchBuffer.append(state, %{"id" => 2}, "0/11", row_size: 6, now_mono_ms: 101)

    assert batch.row_count == 2
    assert batch.byte_count == 16
  end

  test "due_flush? checks buffer age" do
    state = BatchBuffer.new(flush_interval_ms: 500)
    assert {:ok, state} = BatchBuffer.append(state, %{"id" => 1}, "0/1", now_mono_ms: 1_000)

    refute BatchBuffer.due_flush?(state, 1_400)
    assert BatchBuffer.due_flush?(state, 1_500)
    assert BatchBuffer.due_flush?(state, 1_501)
  end

  test "estimates row size for nested rows without explicit row_size" do
    state = BatchBuffer.new(max_rows: 10, max_bytes: 1_000_000, flush_interval_ms: 60_000)

    row = %{
      "id" => 1,
      "name" => "duck",
      "tags" => ["a", "b", "c"],
      "meta" => %{"active" => true, "score" => 10.5}
    }

    assert {:ok, next_state} = BatchBuffer.append(state, row, "0/10", now_mono_ms: 100)
    assert next_state.row_count == 1
    assert next_state.byte_count > 0
  end

  test "flush returns empty when no rows" do
    state = BatchBuffer.new()

    assert {:empty, ^state} = BatchBuffer.flush(state)
  end
end
