defmodule DuckFeeder.Config do
  @moduledoc """
  Runtime configuration validation and normalization.

  Validates source, DuckDB, metadata DB, and ingest knobs with NimbleOptions.
  """

  @source_schema [
    postgres_url: [type: :string, required: true],
    slot_name: [type: :string, required: true],
    publication_name: [type: :string, required: true],
    designated_tables: [type: {:list, :any}, default: []]
  ]

  @designated_table_schema [
    source_schema: [type: :string, required: true],
    source_table: [type: :string, required: true],
    target_schema: [type: :string, required: true],
    target_table: [type: :string, required: true],
    mode: [type: :string, default: "cdc_changelog"],
    primary_keys: [type: {:list, :string}, default: []]
  ]

  @duckdb_schema [
    path: [type: {:or, [:string, nil]}, default: nil],
    catalog: [type: {:or, [:string, nil]}, default: nil],
    setup_sql: [type: {:list, :string}, default: []],
    setup_fun: [type: :any, default: nil]
  ]

  @metadata_schema [
    postgres_url: [type: :string, required: true]
  ]

  @ingest_schema [
    max_rows: [type: :pos_integer, default: 10_000],
    max_bytes: [type: :pos_integer, default: 128 * 1_024 * 1_024],
    flush_interval_ms: [type: :pos_integer, default: 5_000]
  ]

  @schema NimbleOptions.new!(
            source: [type: :keyword_list, required: true, keys: @source_schema],
            duckdb: [type: :keyword_list, required: true, keys: @duckdb_schema],
            metadata: [type: :keyword_list, required: true, keys: @metadata_schema],
            ingest: [type: :keyword_list, default: [], keys: @ingest_schema]
          )

  @known_keys %{
    "source" => :source,
    "duckdb" => :duckdb,
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
    "path" => :path,
    "catalog" => :catalog,
    "setup_sql" => :setup_sql,
    "setup_fun" => :setup_fun,
    "max_rows" => :max_rows,
    "max_bytes" => :max_bytes,
    "flush_interval_ms" => :flush_interval_ms
  }

  @type t :: %{
          source: map(),
          duckdb: map(),
          metadata: map(),
          ingest: map()
        }

  @spec validate(map() | keyword()) :: {:ok, t()} | {:error, Exception.t()}
  def validate(config) when is_map(config) or is_list(config) do
    with {:ok, keyword_config} <- to_keyword(config),
         {:ok, validated} <- NimbleOptions.validate(keyword_config, @schema),
         {:ok, validated_tables} <-
           validate_designated_tables(validated[:source][:designated_tables]) do
      source = Keyword.put(validated[:source], :designated_tables, validated_tables)

      {:ok,
       %{
         source: deep_to_map(source),
         duckdb: deep_to_map(validated[:duckdb]),
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
  Returns the DuckDB options shape expected by runtime/service startup.
  """
  @spec duckdb(t()) :: map()
  def duckdb(validated_config) do
    validated_config
    |> Map.fetch!(:duckdb)
    |> Map.take([:path, :catalog, :setup_sql, :setup_fun])
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
       when key in [:source, :duckdb, :metadata, :ingest] and (is_map(value) or is_list(value)) do
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

  defp deep_to_map([]), do: []

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
end
