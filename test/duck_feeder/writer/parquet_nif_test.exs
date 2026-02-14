defmodule DuckFeeder.Writer.ParquetNifTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.Writer.ParquetNif

  test "writes parquet file" do
    assert {:ok, result} =
             ParquetNif.write_batch(%{}, %{rows: [%{"id" => 1, "name" => "duck"}]}, [])

    assert result.format == :parquet
    assert result.file_size_bytes > 0
    assert File.exists?(result.local_path)

    assert :ok = ParquetNif.cleanup(%{}, result)
    refute File.exists?(result.local_path)
  end

  test "supports unix microseconds datetime encoding" do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    assert {:ok, result} =
             ParquetNif.write_batch(
               %{datetime_encoding: :unix_microseconds},
               %{rows: [%{"captured_at" => now}]},
               []
             )

    assert result.file_size_bytes > 0
    assert File.exists?(result.local_path)
    assert :ok = ParquetNif.cleanup(%{}, result)
  end

  test "returns error for empty row list" do
    assert {:error, :empty_rows} = ParquetNif.write_batch(%{}, %{rows: []}, [])
  end
end
