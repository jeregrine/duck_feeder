defmodule DuckFeeder.Writer.ParquetNifTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.Writer.ParquetNif

  test "returns not implemented error" do
    assert {:error, :parquet_nif_not_implemented} =
             ParquetNif.write_batch(%{}, %{rows: []}, [])
  end
end
