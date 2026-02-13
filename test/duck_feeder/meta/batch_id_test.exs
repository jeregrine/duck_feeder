defmodule DuckFeeder.Meta.BatchIdTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.Meta.BatchId

  test "is deterministic" do
    id1 = BatchId.build(10, "0/16B6A98", "0/16B6AF0", [2, 1])
    id2 = BatchId.build(10, "0/16B6A98", "0/16B6AF0", [1, 2])

    assert id1 == id2
    assert String.starts_with?(id1, "b_")
    assert byte_size(id1) == 42
  end

  test "changes when lsn range changes" do
    id1 = BatchId.build(10, "0/16B6A98", "0/16B6AF0", [1])
    id2 = BatchId.build(10, "0/16B6A98", "0/16B6B00", [1])

    refute id1 == id2
  end
end
