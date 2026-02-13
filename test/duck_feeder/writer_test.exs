defmodule DuckFeeder.WriterTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.Writer

  test "writes jsonl batch and cleans up file" do
    batch = %{rows: [%{"id" => 1}, %{"id" => 2}]}

    assert {:ok, result} = Writer.write_batch(%{}, batch)

    assert result.format == :jsonl
    assert result.adapter == DuckFeeder.Writer.Jsonl
    assert result.row_count == 2
    assert result.file_size_bytes > 0
    assert File.exists?(result.local_path)

    assert :ok = Writer.cleanup(%{}, result)
    refute File.exists?(result.local_path)
  end

  test "supports format-based adapter selection and fallback" do
    assert {:error, :parquet_nif_not_implemented} =
             Writer.write_batch(%{format: :parquet}, %{rows: []})

    assert {:ok, result} =
             Writer.write_batch(%{format: :parquet, fallback_format: :jsonl}, %{
               rows: [%{"id" => 1}]
             })

    assert result.format == :jsonl
    assert result.adapter == DuckFeeder.Writer.Jsonl
    assert :ok = Writer.cleanup(%{format: :parquet, fallback_format: :jsonl}, result)
  end

  test "returns error for invalid adapter and format" do
    assert {:error, {:invalid_writer_adapter, "bad"}} =
             Writer.write_batch(%{adapter: "bad"}, %{rows: []})

    assert {:error, {:invalid_writer_format, :csv}} =
             Writer.write_batch(%{format: :csv}, %{rows: []})

    assert {:error, {:invalid_writer_fallback_adapter, "bad"}} =
             Writer.write_batch(%{format: :parquet, fallback_adapter: "bad"}, %{rows: []})
  end
end
