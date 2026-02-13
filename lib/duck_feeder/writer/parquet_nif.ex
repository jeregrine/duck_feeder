defmodule DuckFeeder.Writer.ParquetNif do
  @moduledoc """
  Parquet writer backed by a Rustler NIF.
  """

  use Rustler,
    otp_app: :duck_feeder,
    crate: "duck_feeder_parquet"

  @behaviour DuckFeeder.Writer.Adapter

  @impl true
  def write_batch(_config, %{rows: []}, _opts), do: {:error, :empty_rows}

  def write_batch(_config, %{rows: rows}, _opts) when is_list(rows) do
    with {:ok, path} <- temp_parquet_path(),
         rows_json <- JSON.encode!(rows),
         :ok <- run_nif_write(path, rows_json),
         {:ok, %{size: size}} <- File.stat(path) do
      {:ok,
       %{
         local_path: path,
         row_count: length(rows),
         file_size_bytes: size,
         format: :parquet
       }}
    end
  end

  @impl true
  def cleanup(_config, %{local_path: local_path}) do
    _ = File.rm(local_path)
    :ok
  end

  defp temp_parquet_path do
    {:ok,
     Path.join(
       System.tmp_dir!(),
       "duck_feeder_#{System.unique_integer([:positive])}.parquet"
     )}
  end

  defp run_nif_write(path, rows_json) do
    case nif_write_parquet(path, rows_json) do
      :ok -> :ok
      {:ok, :ok} -> :ok
      {:error, reason} -> {:error, reason}
      {:error, atom, reason} -> {:error, {atom, reason}}
      other -> {:error, {:unexpected_parquet_nif_result, other}}
    end
  end

  defp nif_write_parquet(_path, _rows_json), do: :erlang.nif_error(:nif_not_loaded)
end
