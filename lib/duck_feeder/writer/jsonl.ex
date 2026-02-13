defmodule DuckFeeder.Writer.Jsonl do
  @moduledoc """
  Temporary write adapter used until Parquet writer integration lands.

  Writes rows as JSON Lines to local disk.
  """

  @behaviour DuckFeeder.Writer.Adapter

  @impl true
  def write_batch(config, batch, _opts) do
    rows = Map.get(batch, :rows, [])

    with {:ok, path} <- temp_path(config),
         :ok <- write_rows(path, rows),
         {:ok, size} <- file_size(path) do
      {:ok,
       %{
         local_path: path,
         row_count: length(rows),
         file_size_bytes: size,
         format: :jsonl
       }}
    end
  end

  @impl true
  def cleanup(_config, %{local_path: path}) when is_binary(path) do
    case File.rm(path) do
      :ok -> :ok
      {:error, :enoent} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp temp_path(config) do
    tmp_dir =
      config
      |> Map.get(:adapter_opts, %{})
      |> Map.new()
      |> Map.get(:tmp_dir, System.tmp_dir!())

    name = "duck_feeder_#{System.unique_integer([:positive, :monotonic])}.jsonl"
    {:ok, Path.join(tmp_dir, name)}
  end

  defp write_rows(path, rows) do
    rows
    |> Enum.map(fn row -> JSON.encode!(row) <> "\n" end)
    |> then(&File.write(path, &1))
  end

  defp file_size(path) do
    case File.stat(path) do
      {:ok, %{size: size}} -> {:ok, size}
      {:error, reason} -> {:error, reason}
    end
  end
end
