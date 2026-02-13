defmodule DuckFeeder.WriterTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.Writer

  test "writes jsonl batch and cleans up file" do
    batch = %{rows: [%{"id" => 1}, %{"id" => 2}]}

    assert {:ok, result} = Writer.write_batch(%{}, batch)

    assert result.format == :jsonl
    assert result.row_count == 2
    assert result.file_size_bytes > 0
    assert File.exists?(result.local_path)

    assert :ok = Writer.cleanup(%{}, result)
    refute File.exists?(result.local_path)
  end

  test "returns error for invalid adapter" do
    assert {:error, {:invalid_writer_adapter, "bad"}} =
             Writer.write_batch(%{adapter: "bad"}, %{rows: []})
  end
end
