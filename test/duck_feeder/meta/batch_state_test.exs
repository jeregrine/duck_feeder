defmodule DuckFeeder.Meta.BatchStateTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.Meta.BatchState

  test "accepts valid forward transitions" do
    assert :ok = BatchState.validate_transition(:pending, :encoded)
    assert :ok = BatchState.validate_transition(:encoded, :uploaded)
    assert :ok = BatchState.validate_transition(:uploaded, :committed)
  end

  test "rejects invalid transitions" do
    assert {:error, {:invalid_batch_transition, :pending, :committed}} =
             BatchState.validate_transition(:pending, :committed)

    assert {:error, {:invalid_batch_transition, :committed, :pending}} =
             BatchState.validate_transition(:committed, :pending)
  end

  test "supports retry path from failed to pending" do
    assert :ok = BatchState.validate_transition(:failed, :pending)
  end

  test "normalizes db state strings" do
    assert {:ok, :uploaded} = BatchState.from_db("uploaded")
    assert {:ok, "uploaded"} = BatchState.to_db(:uploaded)
  end
end
