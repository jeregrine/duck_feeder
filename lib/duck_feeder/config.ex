defmodule DuckFeeder.Config do
  @moduledoc """
  Runtime configuration validation and normalization.

  Validates source, storage, metadata DB, and ingest knobs with NimbleOptions.
  """

  @source_schema [
    postgres_url: [type: :string, required: true],
    slot_name: [type: :string, required: true],
    publication_name: [type: :string, required: true],
    designated_tables: [type: {:list, :keyword_list}, default: []]
  ]

  @designated_table_schema [
    source_schema: [type: :string, required: true],
    source_table: [type: :string, required: true],
    target_schema: [type: :string, required: true],
    target_table: [type: :string, required: true],
    mode: [type: :string, default: "cdc_changelog"],
    primary_keys: [type: {:list, :string}, default: []]
  ]

  @storage_schema [
    provider: [type: {:in, [:s3, :gcs]}, required: true],
    bucket: [type: :string, required: true],
    prefix: [type: :string, default: ""],
    adapter_opts: [type: :map, default: %{}],
    region: [type: :string, default: "us-east-1"],
    endpoint: [type: {:or, [:string, nil]}, default: nil],
    force_path_style: [type: :boolean, default: false],
    access_key_id: [type: {:or, [:string, nil]}, default: nil],
    secret_access_key: [type: {:or, [:string, nil]}, default: nil],
    session_token: [type: {:or, [:string, nil]}, default: nil],
    token: [type: {:or, [:string, nil]}, default: nil],
    token_fun: [type: :any, default: nil],
    base_url: [type: :string, default: "https://storage.googleapis.com"]
  ]

  @metadata_schema [
    postgres_url: [type: :string, required: true]
  ]

  @ingest_schema [
    max_rows: [type: :pos_integer, default: 10_000],
    max_bytes: [type: :pos_integer, default: 128 * 1_024 * 1_024],
    flush_interval_ms: [type: :pos_integer, default: 5_000],
    table_worker_concurrency: [type: :pos_integer, default: 4]
  ]

  @schema NimbleOptions.new!(
            source: [type: :keyword_list, required: true, keys: @source_schema],
            storage: [type: :keyword_list, required: true, keys: @storage_schema],
            metadata: [type: :keyword_list, required: true, keys: @metadata_schema],
            ingest: [type: :keyword_list, default: [], keys: @ingest_schema]
          )

  @known_keys %{
    "source" => :source,
    "storage" => :storage,
    "metadata" => :metadata,
    "ingest" => :ingest,
    "postgres_url" => :postgres_url,
    "slot_name" => :slot_name,
    "publication_name" => :publication_name,
    "designated_tables" => :designated_tables,
    "source_schema" => :source_schema,
    "source_table" => :source_table,
    "target_schema" => :target_schema,
    "target_table" => :target_table,
    "mode" => :mode,
    "primary_keys" => :primary_keys,
    "provider" => :provider,
    "bucket" => :bucket,
    "prefix" => :prefix,
    "adapter_opts" => :adapter_opts,
    "region" => :region,
    "endpoint" => :endpoint,
    "force_path_style" => :force_path_style,
    "access_key_id" => :access_key_id,
    "secret_access_key" => :secret_access_key,
    "session_token" => :session_token,
    "token" => :token,
    "token_fun" => :token_fun,
    "base_url" => :base_url,
    "max_rows" => :max_rows,
    "max_bytes" => :max_bytes,
    "flush_interval_ms" => :flush_interval_ms,
    "table_worker_concurrency" => :table_worker_concurrency
  }

  @type t :: %{
          source: map(),
          storage: map(),
          metadata: map(),
          ingest: map()
        }

  @spec validate(map() | keyword()) :: {:ok, t()} | {:error, Exception.t()}
  def validate(config) when is_map(config) or is_list(config) do
    with {:ok, keyword_config} <- to_keyword(config),
         {:ok, validated} <- NimbleOptions.validate(keyword_config, @schema),
         {:ok, validated_tables} <-
           validate_designated_tables(validated[:source][:designated_tables]),
         {:ok, _} <- validate_storage_provider_opts(validated[:storage]) do
      source = Keyword.put(validated[:source], :designated_tables, validated_tables)

      {:ok,
       %{
         source: deep_to_map(source),
         storage: deep_to_map(validated[:storage]),
         metadata: deep_to_map(validated[:metadata]),
         ingest: deep_to_map(validated[:ingest])
       }}
    end
  end

  @spec validate!(map() | keyword()) :: t()
  def validate!(config) do
    case validate(config) do
      {:ok, validated} -> validated
      {:error, error} -> raise error
    end
  end

  @doc """
  Returns storage config shape expected by `DuckFeeder.Storage`.
  """
  @spec storage_config(t()) :: map()
  def storage_config(validated_config) do
    storage = Map.fetch!(validated_config, :storage)

    base = %{
      provider: storage.provider,
      bucket: storage.bucket,
      prefix: storage.prefix,
      adapter_opts: storage.adapter_opts
    }

    case storage.provider do
      :s3 ->
        base
        |> Map.put(:region, storage.region)
        |> put_if_present(:endpoint, storage.endpoint)
        |> Map.put(:force_path_style, storage.force_path_style)
        |> put_if_present(:access_key_id, storage.access_key_id)
        |> put_if_present(:secret_access_key, storage.secret_access_key)
        |> put_if_present(:session_token, storage.session_token)

      :gcs ->
        base
        |> put_if_present(:token, storage.token)
        |> put_if_present(:token_fun, storage.token_fun)
        |> Map.put(:base_url, storage.base_url)
    end
  end

  @doc false
  def schema, do: @schema

  defp validate_designated_tables(designated_tables) when is_list(designated_tables) do
    designated_tables
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, []}, fn {table, index}, {:ok, acc} ->
      with {:ok, table_kw} <- to_keyword(table),
           {:ok, validated} <- NimbleOptions.validate(table_kw, @designated_table_schema),
           :ok <- validate_mode(validated[:mode], index) do
        {:cont, {:ok, [validated | acc]}}
      else
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, tables} -> {:ok, Enum.reverse(tables)}
      {:error, _} = error -> error
    end
  end

  defp validate_storage_provider_opts(storage) do
    provider = storage[:provider]

    case provider do
      :s3 ->
        cond do
          blank?(storage[:access_key_id]) ->
            {:error,
             ArgumentError.exception("storage.access_key_id is required for provider :s3")}

          blank?(storage[:secret_access_key]) ->
            {:error,
             ArgumentError.exception("storage.secret_access_key is required for provider :s3")}

          true ->
            {:ok, :valid}
        end

      :gcs ->
        token = storage[:token]
        token_fun = storage[:token_fun]

        cond do
          is_binary(token) and token != "" ->
            {:ok, :valid}

          is_function(token_fun, 0) ->
            {:ok, :valid}

          true ->
            {:error,
             ArgumentError.exception(
               "storage.token or storage.token_fun/0 is required for provider :gcs"
             )}
        end

      other ->
        {:error, ArgumentError.exception("unsupported storage provider: #{inspect(other)}")}
    end
  end

  defp validate_mode("cdc_changelog", _index), do: :ok

  defp validate_mode(mode, index) do
    {:error,
     ArgumentError.exception(
       "source.designated_tables[#{index}].mode must be \"cdc_changelog\", got: #{inspect(mode)}"
     )}
  end

  defp to_keyword(keyword) when is_list(keyword) do
    if Keyword.keyword?(keyword) do
      {:ok, keyword}
    else
      {:error, ArgumentError.exception("expected keyword list, got: #{inspect(keyword)}")}
    end
  end

  defp to_keyword(map) when is_map(map) do
    map
    |> Enum.reduce_while({:ok, []}, fn {key, value}, {:ok, acc} ->
      with {:ok, normalized_key} <- normalize_key(key),
           {:ok, normalized_value} <- normalize_value_for_key(normalized_key, value) do
        {:cont, {:ok, [{normalized_key, normalized_value} | acc]}}
      else
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, entries} -> {:ok, Enum.reverse(entries)}
      {:error, _reason} = error -> error
    end
  end

  defp to_keyword(other),
    do: {:error, ArgumentError.exception("expected map/keyword, got: #{inspect(other)}")}

  defp normalize_key(key) when is_atom(key), do: {:ok, key}

  defp normalize_key(key) when is_binary(key) do
    case Map.fetch(@known_keys, key) do
      {:ok, normalized} -> {:ok, normalized}
      :error -> {:error, ArgumentError.exception("unknown config key: #{inspect(key)}")}
    end
  end

  defp normalize_key(key),
    do: {:error, ArgumentError.exception("invalid config key: #{inspect(key)}")}

  defp normalize_value_for_key(key, value)
       when key in [:source, :storage, :metadata, :ingest] and (is_map(value) or is_list(value)) do
    to_keyword(value)
  end

  defp normalize_value_for_key(:designated_tables, value) when is_list(value) do
    value
    |> Enum.reduce_while({:ok, []}, fn entry, {:ok, acc} ->
      case to_keyword(entry) do
        {:ok, keyword_entry} -> {:cont, {:ok, [keyword_entry | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, entries} -> {:ok, Enum.reverse(entries)}
      {:error, _reason} = error -> error
    end
  end

  defp normalize_value_for_key(_key, value), do: {:ok, value}

  defp deep_to_map(list) when is_list(list) do
    if Keyword.keyword?(list) do
      list
      |> Enum.map(fn {k, v} -> {k, deep_to_map(v)} end)
      |> Map.new()
    else
      Enum.map(list, &deep_to_map/1)
    end
  end

  defp deep_to_map(other), do: other

  defp put_if_present(map, _key, value) when is_nil(value), do: map
  defp put_if_present(map, _key, ""), do: map
  defp put_if_present(map, key, value), do: Map.put(map, key, value)

  defp blank?(value), do: is_nil(value) or value == ""
end
