defmodule DuckFeeder.Bootstrap do
  @moduledoc """
  Helpers for bootstrapping `duckfeeder_meta` from runtime config.
  """

  alias DuckFeeder.{Config, Meta, Runtime}

  @spec seed_meta(pid(), map() | keyword(), keyword()) :: {:ok, map()} | {:error, term()}
  def seed_meta(meta_conn, config, opts \\ []) do
    meta_module = Keyword.get(opts, :meta_module, Meta)

    with {:ok, validated} <- Config.validate(config),
         :ok <- maybe_bootstrap(meta_module, meta_conn, opts),
         {:ok, source_id} <- register_source(meta_module, meta_conn, validated.source, opts),
         {:ok, designated_tables} <-
           resolve_designated_tables(validated.source.designated_tables, opts),
         {:ok, designated_table_ids} <-
           register_designated_tables(
             meta_module,
             meta_conn,
             source_id,
             designated_tables
           ) do
      {:ok,
       %{
         source_id: source_id,
         designated_table_ids: designated_table_ids,
         source_name: source_name(validated.source, opts)
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
           runtime_start_opts(seed_opts, start_opts),
         storage_config <- Config.storage_config(validated),
         {:ok, runtime_result} <-
           runtime_module.start_stream(
             meta_conn,
             seed_result.source_name,
             storage_config,
             runtime_start_opts
           ) do
      {:ok,
       %{
         source_id: seed_result.source_id,
         designated_table_ids: seed_result.designated_table_ids,
         source_name: seed_result.source_name,
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

  defp register_source(meta_module, meta_conn, source, opts) do
    source_name = source_name(source, opts)

    base_connection_info = %{
      postgres_url: source.postgres_url
    }

    connection_info =
      opts
      |> Keyword.get(:connection_info, %{})
      |> Map.new()
      |> Map.merge(base_connection_info)

    meta_module.register_source(meta_conn, %{
      name: source_name,
      connection_info: connection_info,
      slot_name: source.slot_name,
      publication_name: source.publication_name,
      status: Keyword.get(opts, :source_status, "active")
    })
  end

  defp register_designated_tables(meta_module, meta_conn, source_id, tables) do
    tables
    |> Enum.reduce_while({:ok, []}, fn table, {:ok, acc} ->
      attrs =
        table
        |> Map.take([
          :source_schema,
          :source_table,
          :target_schema,
          :target_table,
          :mode,
          :primary_keys
        ])
        |> Map.put(:source_id, source_id)

      case meta_module.register_designated_table(meta_conn, attrs) do
        {:ok, id} -> {:cont, {:ok, [id | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, ids} -> {:ok, Enum.reverse(ids)}
      {:error, _reason} = error -> error
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

  defp runtime_start_opts(seed_opts, start_opts) do
    seed_meta_module = Keyword.get(seed_opts, :meta_module)

    opts =
      case Keyword.has_key?(start_opts, :meta_module) do
        true -> start_opts
        false -> maybe_put(start_opts, :meta_module, seed_meta_module)
      end

    {:ok, opts}
  end

  defp source_name(source, opts) do
    Keyword.get(opts, :source_name, Map.get(source, :name, "default"))
  end

  defp maybe_put(opts, _key, nil), do: opts
  defp maybe_put(opts, key, value), do: Keyword.put(opts, key, value)
end
