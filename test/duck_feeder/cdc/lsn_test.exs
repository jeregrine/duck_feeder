defmodule DuckFeeder.CDC.LsnTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.CDC.Lsn

  test "parse and to_string roundtrip" do
    assert {:ok, value} = Lsn.parse("16/B374D848")
    assert is_integer(value)
    assert Lsn.to_string(value) == "16/B374D848"
  end

  test "compare lsn values" do
    assert :lt = Lsn.compare("0/1", "0/2")
    assert :gt = Lsn.compare("0/3", "0/2")
    assert :eq = Lsn.compare("0/2", "0/2")
  end

  test "max picks largest lsn" do
    assert "0/10" == Lsn.max("0/A", "0/10")
    assert "0/10" == Lsn.max("0/10", "0/10")
  end

  test "rejects invalid lsn strings" do
    assert {:error, {:invalid_lsn, "bad"}} = Lsn.parse("bad")

    assert_raise ArgumentError, ~r/invalid lsn/, fn ->
      Lsn.parse!("bad")
    end
  end
end
