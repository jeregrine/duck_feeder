defmodule DuckFeeder.Storage do
  @moduledoc """
  Semi-generic storage entrypoint for object writes.

  Supported providers:
  - `:s3` via `DuckFeeder.Storage.S3`
  - `:gcs` via `DuckFeeder.Storage.GCS`

  Configuration shape:

      %{
        provider: :s3 | :gcs,
        bucket: "my-bucket",
        prefix: "optional/prefix",
        adapter: OptionalCustomAdapter,
        adapter_opts: %{}
      }
  """

  alias DuckFeeder.Storage.{Adapter, GCS, S3}

  @type provider :: :s3 | :gcs

  @type t :: %{
          required(:provider) => provider(),
          required(:bucket) => String.t(),
          optional(:prefix) => String.t(),
          optional(:adapter) => module(),
          optional(:adapter_opts) => map()
        }

  @spec put_file(t(), Path.t(), String.t(), keyword()) ::
          {:ok, Adapter.put_result()} | {:error, term()}
  def put_file(config, local_path, relative_key, opts \\ [])
      when is_map(config) and is_binary(local_path) and is_binary(relative_key) do
    with {:ok, adapter} <- adapter_module(config),
         {:ok, object_ref} <- object_ref(config, relative_key) do
      adapter.put_file(config, local_path, object_ref, opts)
    end
  end

  @spec head_object(t(), String.t()) :: {:ok, map()} | {:error, term()}
  def head_object(config, relative_key) when is_map(config) and is_binary(relative_key) do
    with {:ok, adapter} <- adapter_module(config),
         {:ok, object_ref} <- object_ref(config, relative_key) do
      adapter.head_object(config, object_ref)
    end
  end

  @spec delete_object(t(), String.t()) :: :ok | {:error, term()}
  def delete_object(config, relative_key) when is_map(config) and is_binary(relative_key) do
    with {:ok, adapter} <- adapter_module(config),
         {:ok, object_ref} <- object_ref(config, relative_key) do
      adapter.delete_object(config, object_ref)
    end
  end

  @spec object_ref(t(), String.t()) :: {:ok, Adapter.object_ref()} | {:error, term()}
  def object_ref(config, relative_key) when is_map(config) and is_binary(relative_key) do
    case Map.get(config, :bucket) do
      bucket when is_binary(bucket) and bucket != "" ->
        {:ok,
         %{
           bucket: bucket,
           key: join_key(Map.get(config, :prefix, ""), relative_key)
         }}

      _ ->
        {:error, :missing_bucket}
    end
  end

  @spec adapter_module(t()) :: {:ok, module()} | {:error, term()}
  def adapter_module(config) when is_map(config) do
    adapter = Map.get(config, :adapter)

    cond do
      is_atom(adapter) and not is_nil(adapter) ->
        {:ok, adapter}

      config[:provider] == :s3 ->
        {:ok, S3}

      config[:provider] == :gcs ->
        {:ok, GCS}

      true ->
        {:error, {:unsupported_provider, config[:provider]}}
    end
  end

  defp join_key(prefix, relative_key) do
    [prefix, relative_key]
    |> Enum.reject(&blank?/1)
    |> Enum.map(&trim_slashes/1)
    |> Enum.join("/")
  end

  defp blank?(value), do: is_nil(value) or value == ""

  defp trim_slashes(value) do
    value
    |> String.trim_leading("/")
    |> String.trim_trailing("/")
  end
end
