defmodule DuckFeeder.Writer.ParquetNif do
  @moduledoc """
  Placeholder adapter for upcoming Rustler/Parquet integration.

  The adapter shape is in place so runtime config can switch from JSONL to
  a native Parquet implementation when ready.
  """

  @behaviour DuckFeeder.Writer.Adapter

  @impl true
  def write_batch(_config, _batch, _opts), do: {:error, :parquet_nif_not_implemented}

  @impl true
  def cleanup(_config, _write_result), do: :ok
end
