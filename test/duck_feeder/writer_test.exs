defmodule DuckFeeder.WriterTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.Writer

  defmodule NotImplementedParquetAdapter do
    @behaviour DuckFeeder.Writer.Adapter

    @impl true
    def write_batch(_config, _batch, _opts), do: {:error, :parquet_nif_not_implemented}

    @impl true
    def cleanup(_config, _result), do: :ok
  end

  test "writes parquet batch by default and cleans up file" do
    batch = %{rows: [%{"id" => 1}, %{"id" => 2}]}

    assert {:ok, result} = Writer.write_batch(%{}, batch)

    assert result.format == :parquet
    assert result.adapter == DuckFeeder.Writer.ParquetNif
    assert result.row_count == 2
    assert result.file_size_bytes > 0
    assert File.exists?(result.local_path)

    assert :ok = Writer.cleanup(%{}, result)
    refute File.exists?(result.local_path)
  end

  test "supports format-based adapter selection and parquet writing" do
    {:ok, parquet_result} =
      Writer.write_batch(%{format: :parquet}, %{rows: [%{"id" => 1, "name" => "duck"}]})

    assert parquet_result.format == :parquet
    assert parquet_result.adapter == DuckFeeder.Writer.ParquetNif
    assert parquet_result.file_size_bytes > 0
    assert File.exists?(parquet_result.local_path)
    assert :ok = Writer.cleanup(%{format: :parquet}, parquet_result)

    {:ok, result} =
      Writer.write_batch(%{format: :parquet, fallback_format: :jsonl}, %{rows: [%{"id" => 1}]})

    assert result.format == :parquet
    assert result.adapter == DuckFeeder.Writer.ParquetNif
    assert :ok = Writer.cleanup(%{format: :parquet, fallback_format: :jsonl}, result)
  end

  test "supports explicit jsonl format" do
    assert {:ok, result} = Writer.write_batch(%{format: :jsonl}, %{rows: [%{"id" => 1}]})
    assert result.format == :jsonl
    assert result.adapter == DuckFeeder.Writer.Jsonl
    assert :ok = Writer.cleanup(%{format: :jsonl}, result)
  end

  test "falls back to configured writer when parquet adapter is unavailable" do
    assert {:ok, result} =
             Writer.write_batch(
               %{adapter: NotImplementedParquetAdapter, fallback_format: :jsonl},
               %{rows: [%{"id" => 1}]}
             )

    assert result.format == :jsonl
    assert result.adapter == DuckFeeder.Writer.Jsonl
    assert :ok = Writer.cleanup(%{format: :jsonl}, result)
  end

  test "returns error for invalid adapter and format" do
    assert {:error, {:invalid_writer_adapter, "bad"}} =
             Writer.write_batch(%{adapter: "bad"}, %{rows: []})

    assert {:error, {:invalid_writer_format, :csv}} =
             Writer.write_batch(%{format: :csv}, %{rows: []})
  end
end
