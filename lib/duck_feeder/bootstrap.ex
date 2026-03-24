defmodule DuckFeeder.Bootstrap do
  @moduledoc """
  Helpers for bootstrapping `duckfeeder_meta` from runtime config.
  """

  alias DuckFeeder.{Config, Meta, Runtime}
  alias DuckFeeder.CDC.ConnectionOptions

  @spec seed_meta(pid(), map() | keyword(), keyword()) :: {:ok, map()} | {:error, term()}
  def seed_meta(meta_conn, config, opts \\ []) do
    meta_module = Keyword.get(opts, :meta_module, Meta)

    with {:ok, validated} <- Config.validate(config),
         :ok <- maybe_bootstrap(meta_module, meta_conn, opts),
         source_name <- source_name(validated.source, opts),
         {:ok, designated_tables} <-
           resolve_designated_tables(validated.source.designated_tables, opts) do
      {:ok,
       %{
         source_name: source_name,
         source: Runtime.build_runtime_source(source_name, validated.source),
         designated_tables: Runtime.put_runtime_checkpoint_keys(source_name, designated_tables)
       }}
    end
  end

  @spec seed_and_start_stream(pid(), map() | keyword(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def seed_and_start_stream(meta_conn, config, opts \\ []) do
    seed_opts = Keyword.get(opts, :seed_opts, [])
    start_opts = Keyword.get(opts, :start_opts, [])
    runtime_module = Keyword.get(opts, :runtime_module, Runtime)

    with {:ok, validated} <- Config.validate(config),
         {:ok, seed_result} <- seed_meta(meta_conn, config, seed_opts),
         {:ok, runtime_start_opts} <-
           runtime_start_opts(
             seed_result.source,
             seed_result.designated_tables,
             seed_opts,
             start_opts
           ),
         duckdb_config <- Config.duckdb_config(validated),
         {:ok, runtime_result} <-
           runtime_module.start_stream(
             meta_conn,
             seed_result.source_name,
             duckdb_config,
             runtime_start_opts
           ) do
      {:ok,
       %{
         source_name: seed_result.source_name,
         source: seed_result.source,
         designated_tables: seed_result.designated_tables,
         runtime: runtime_result
       }}
    end
  end

  defp maybe_bootstrap(meta_module, meta_conn, opts) do
    if Keyword.get(opts, :bootstrap_schema?, true) do
      meta_module.bootstrap(meta_conn)
    else
      :ok
    end
  end

  defp resolve_designated_tables(config_tables, opts) when is_list(config_tables) do
    case Keyword.get(opts, :tables) do
      nil ->
        {:ok, config_tables}

      selections when is_list(selections) ->
        build_selected_tables(config_tables, selections, opts)

      other ->
        {:error, {:invalid_tables_selection, other}}
    end
  end

  defp resolve_designated_tables(%{} = config_tables, opts) do
    if map_size(config_tables) == 0 do
      resolve_designated_tables([], opts)
    else
      {:error, {:invalid_designated_tables, config_tables}}
    end
  end

  defp build_selected_tables(config_tables, selections, opts) do
    defaults = %{
      source_schema: Keyword.get(opts, :default_source_schema, "public"),
      target_schema: Keyword.get(opts, :default_target_schema, "raw"),
      mode: "cdc_changelog",
      primary_keys: []
    }

    selections
    |> Enum.reduce_while({:ok, []}, fn selection, {:ok, acc} ->
      with {:ok, normalized} <- normalize_selection(selection),
           table <- merge_selection(config_tables, normalized, defaults) do
        {:cont, {:ok, [table | acc]}}
      else
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, tables} -> {:ok, Enum.reverse(tables)}
      {:error, _reason} = error -> error
    end
  end

  defp normalize_selection(source_table) when is_binary(source_table) do
    {:ok, %{source_table: source_table}}
  end

  defp normalize_selection({target_table, source_table})
       when is_binary(target_table) and is_binary(source_table) do
    {:ok, %{source_table: source_table, target_table: target_table}}
  end

  defp normalize_selection(selection) when is_map(selection) do
    selection = Map.new(selection)

    case Map.get(selection, :source_table) || Map.get(selection, "source_table") do
      source_table when is_binary(source_table) and source_table != "" ->
        {:ok,
         %{
           source_schema:
             Map.get(selection, :source_schema) || Map.get(selection, "source_schema"),
           source_table: source_table,
           target_schema:
             Map.get(selection, :target_schema) || Map.get(selection, "target_schema"),
           target_table: Map.get(selection, :target_table) || Map.get(selection, "target_table"),
           mode: Map.get(selection, :mode) || Map.get(selection, "mode"),
           primary_keys: Map.get(selection, :primary_keys) || Map.get(selection, "primary_keys")
         }
         |> Enum.reject(fn {_k, v} -> is_nil(v) end)
         |> Map.new()}

      _ ->
        {:error, {:invalid_table_selection, selection}}
    end
  end

  defp normalize_selection(other), do: {:error, {:invalid_table_selection, other}}

  defp merge_selection(config_tables, normalized, defaults) do
    source_schema = Map.get(normalized, :source_schema, defaults.source_schema)
    source_table = normalized.source_table

    base =
      Enum.find(config_tables, fn table ->
        table.source_schema == source_schema and table.source_table == source_table
      end) ||
        %{
          source_schema: source_schema,
          source_table: source_table,
          target_schema: defaults.target_schema,
          target_table: source_table,
          mode: defaults.mode,
          primary_keys: defaults.primary_keys
        }

    base
    |> Map.merge(normalized)
    |> Map.put_new(:source_schema, source_schema)
    |> Map.put_new(:target_schema, defaults.target_schema)
    |> Map.put_new(:target_table, source_table)
    |> Map.put_new(:mode, defaults.mode)
    |> Map.put_new(:primary_keys, defaults.primary_keys)
  end

  defp runtime_start_opts(source, designated_tables, seed_opts, start_opts)
       when is_map(source) and is_list(designated_tables) do
    seed_meta_module = Keyword.get(seed_opts, :meta_module)

    opts =
      start_opts
      |> maybe_put_missing(:meta_module, seed_meta_module)
      |> maybe_put_missing(:source, source)
      |> maybe_put_missing(:designated_tables, designated_tables)
      |> maybe_put_runtime_connection_opts(source)

    {:ok, opts}
  end

  defp maybe_put_runtime_connection_opts(start_opts, source) do
    if Keyword.has_key?(start_opts, :connection_opts) do
      start_opts
    else
      case ConnectionOptions.parse_url(Map.get(source, :postgres_url, "")) do
        {:ok, connection_opts} -> Keyword.put(start_opts, :connection_opts, connection_opts)
        {:error, _reason} -> start_opts
      end
    end
  end

  defp source_name(source, opts) do
    Keyword.get(opts, :source_name, Map.get(source, :name, "default"))
  end

  defp maybe_put_missing(opts, _key, nil), do: opts

  defp maybe_put_missing(opts, key, value) do
    if Keyword.has_key?(opts, key), do: opts, else: Keyword.put(opts, key, value)
  end
end
