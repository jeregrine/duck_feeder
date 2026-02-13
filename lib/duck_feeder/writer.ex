defmodule DuckFeeder.Writer do
  @moduledoc """
  Write-path entrypoint.

  Defaults to `DuckFeeder.Writer.Jsonl` as a placeholder adapter until
  Parquet/Rust writer integration is wired in.
  """

  alias DuckFeeder.Writer.{Adapter, Jsonl, ParquetNif}

  @type t :: %{
          optional(:adapter) => module(),
          optional(:format) => :jsonl | :parquet | :parquet_nif,
          optional(:fallback_adapter) => module(),
          optional(:fallback_format) => :jsonl | :parquet | :parquet_nif,
          optional(:adapter_opts) => map()
        }

  @spec write_batch(t(), Adapter.batch(), keyword()) ::
          {:ok, Adapter.write_result()} | {:error, term()}
  def write_batch(config, batch, opts \\ []) when is_map(config) and is_map(batch) do
    with {:ok, adapter} <- adapter_module(config) do
      case adapter.write_batch(config, batch, opts) do
        {:ok, result} when is_map(result) ->
          {:ok, Map.put(result, :adapter, adapter)}

        {:error, :parquet_nif_not_implemented} = error ->
          maybe_fallback_write(config, batch, opts, error)

        other ->
          other
      end
    end
  end

  @spec cleanup(t(), Adapter.write_result()) :: :ok | {:error, term()}
  def cleanup(config, write_result) when is_map(config) and is_map(write_result) do
    with {:ok, adapter} <- cleanup_adapter_module(config, write_result) do
      adapter.cleanup(config, Map.delete(write_result, :adapter))
    end
  end

  @spec adapter_module(t()) :: {:ok, module()} | {:error, term()}
  def adapter_module(config) when is_map(config) do
    case Map.get(config, :adapter) do
      adapter when is_atom(adapter) and not is_nil(adapter) ->
        {:ok, adapter}

      nil ->
        adapter_from_format(Map.get(config, :format, :jsonl))

      other ->
        {:error, {:invalid_writer_adapter, other}}
    end
  end

  defp adapter_from_format(:jsonl), do: {:ok, Jsonl}
  defp adapter_from_format(:parquet), do: {:ok, ParquetNif}
  defp adapter_from_format(:parquet_nif), do: {:ok, ParquetNif}
  defp adapter_from_format(other), do: {:error, {:invalid_writer_format, other}}

  defp maybe_fallback_write(config, batch, opts, primary_error) do
    with {:ok, fallback_adapter} <- fallback_adapter_module(config),
         {:ok, result} <- fallback_adapter.write_batch(config, batch, opts) do
      {:ok, Map.put(result, :adapter, fallback_adapter)}
    else
      {:error, {:no_writer_fallback, _}} -> primary_error
      {:error, _reason} = error -> error
    end
  end

  defp fallback_adapter_module(config) do
    case Map.get(config, :fallback_adapter) do
      adapter when is_atom(adapter) and not is_nil(adapter) ->
        {:ok, adapter}

      nil ->
        case Map.fetch(config, :fallback_format) do
          {:ok, format} -> adapter_from_format(format)
          :error -> {:error, {:no_writer_fallback, :missing_fallback}}
        end

      other ->
        {:error, {:invalid_writer_fallback_adapter, other}}
    end
  end

  defp cleanup_adapter_module(_config, %{adapter: adapter}) when is_atom(adapter),
    do: {:ok, adapter}

  defp cleanup_adapter_module(config, _write_result), do: adapter_module(config)
end
