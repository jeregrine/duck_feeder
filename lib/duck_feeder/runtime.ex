defmodule DuckFeeder.Runtime do
  @moduledoc """
  Runtime wiring helpers to start `DuckFeeder.Service` from metadata tables.
  """

  alias DuckFeeder.{Meta, Service}

  @spec service_options(pid(), String.t(), map(), keyword()) ::
          {:ok, keyword()} | {:error, term()}
  def service_options(meta_conn, source_name, storage_config, opts \\ [])
      when is_binary(source_name) and is_map(storage_config) do
    meta_module = Keyword.get(opts, :meta_module, Meta)

    with {:ok, source} <- meta_module.get_source(meta_conn, source_name),
         {:ok, designated_tables} <-
           meta_module.list_designated_tables(meta_conn, source_id: source.id) do
      {:ok,
       [
         name: Keyword.get(opts, :name),
         designated_tables: designated_tables,
         meta_conn: meta_conn,
         storage: storage_config,
         writer: Keyword.get(opts, :writer, %{}),
         object_prefix: Keyword.get(opts, :object_prefix, source.name),
         pipeline_opts: Keyword.get(opts, :pipeline_opts, %{}),
         max_tx_changes: Keyword.get(opts, :max_tx_changes),
         observer_pid: Keyword.get(opts, :observer_pid),
         meta_module: meta_module
       ]
       |> Enum.reject(fn {_key, value} -> is_nil(value) end)}
    end
  end

  @spec start_service(pid(), String.t(), map(), keyword()) ::
          GenServer.on_start() | {:error, term()}
  def start_service(meta_conn, source_name, storage_config, opts \\ []) do
    with {:ok, service_opts} <- service_options(meta_conn, source_name, storage_config, opts) do
      Service.start_link(service_opts)
    end
  end
end
