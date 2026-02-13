defmodule DuckFeeder.Bootstrap do
  @moduledoc """
  Helpers for bootstrapping `duckfeeder_meta` from runtime config.
  """

  alias DuckFeeder.{Config, Meta}

  @spec seed_meta(pid(), map() | keyword(), keyword()) :: {:ok, map()} | {:error, term()}
  def seed_meta(meta_conn, config, opts \\ []) do
    meta_module = Keyword.get(opts, :meta_module, Meta)

    with {:ok, validated} <- Config.validate(config),
         :ok <- maybe_bootstrap(meta_module, meta_conn, opts),
         {:ok, source_id} <- register_source(meta_module, meta_conn, validated.source, opts),
         {:ok, designated_table_ids} <-
           register_designated_tables(
             meta_module,
             meta_conn,
             source_id,
             validated.source.designated_tables
           ) do
      {:ok,
       %{
         source_id: source_id,
         designated_table_ids: designated_table_ids,
         source_name: source_name(validated.source, opts)
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

  defp source_name(source, opts) do
    Keyword.get(opts, :source_name, Map.get(source, :name, "default"))
  end
end
