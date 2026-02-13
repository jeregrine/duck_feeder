defmodule DuckFeeder.Writer do
  @moduledoc """
  Write-path entrypoint.

  Defaults to `DuckFeeder.Writer.Jsonl` as a placeholder adapter until
  Parquet/Rust writer integration is wired in.
  """

  alias DuckFeeder.Writer.{Adapter, Jsonl}

  @type t :: %{
          optional(:adapter) => module(),
          optional(:adapter_opts) => map()
        }

  @spec write_batch(t(), Adapter.batch(), keyword()) ::
          {:ok, Adapter.write_result()} | {:error, term()}
  def write_batch(config, batch, opts \\ []) when is_map(config) and is_map(batch) do
    with {:ok, adapter} <- adapter_module(config) do
      adapter.write_batch(config, batch, opts)
    end
  end

  @spec cleanup(t(), Adapter.write_result()) :: :ok | {:error, term()}
  def cleanup(config, write_result) when is_map(config) and is_map(write_result) do
    with {:ok, adapter} <- adapter_module(config) do
      adapter.cleanup(config, write_result)
    end
  end

  @spec adapter_module(t()) :: {:ok, module()} | {:error, term()}
  def adapter_module(config) when is_map(config) do
    case Map.get(config, :adapter) do
      adapter when is_atom(adapter) and not is_nil(adapter) -> {:ok, adapter}
      nil -> {:ok, Jsonl}
      other -> {:error, {:invalid_writer_adapter, other}}
    end
  end
end
