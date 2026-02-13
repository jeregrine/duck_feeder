defmodule DuckFeeder.Runtime do
  @moduledoc """
  Runtime wiring helpers to start `DuckFeeder.Service` from metadata tables.
  """

  alias DuckFeeder.{Meta, Service}
  alias DuckFeeder.CDC.{Bootstrap, Connection, ConnectionOptions, Lsn}

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

  @spec start_stream(pid(), String.t(), map(), keyword()) ::
          {:ok, %{service_pid: pid(), cdc_pid: pid(), start_lsn: String.t(), source: map()}}
          | {:error, term()}
  def start_stream(meta_conn, source_name, storage_config, opts \\ []) do
    meta_module = Keyword.get(opts, :meta_module, Meta)
    service_module = Keyword.get(opts, :service_module, Service)
    cdc_module = Keyword.get(opts, :cdc_module, Connection)
    connection_options_module = Keyword.get(opts, :connection_options_module, ConnectionOptions)

    with {:ok, source} <- meta_module.get_source(meta_conn, source_name),
         {:ok, designated_tables} <-
           meta_module.list_designated_tables(meta_conn, source_id: source.id),
         {:ok, slot_name} <- require_source_field(source, :slot_name),
         {:ok, publication_name} <- require_source_field(source, :publication_name),
         {:ok, meta_start_lsn} <-
           meta_module.fetch_source_start_lsn(
             meta_conn,
             source.id,
             Keyword.get(opts, :default_start_lsn, "0/0")
           ),
         {:ok, connection_opts} <- connection_options_module.resolve(source, opts),
         {:ok, bootstrap_start_lsn} <-
           maybe_bootstrap_start_lsn(
             connection_opts,
             slot_name,
             publication_name,
             designated_tables,
             opts
           ),
         {:ok, snapshot_boundary_lsn} <-
           maybe_snapshot_boundary_lsn(connection_opts, designated_tables, opts),
         {:ok, start_lsn} <-
           resolve_start_lsn(meta_start_lsn, [bootstrap_start_lsn, snapshot_boundary_lsn]),
         {:ok, service_pid} <-
           service_module.start_link(
             build_service_opts(
               meta_conn,
               source,
               designated_tables,
               storage_config,
               meta_module,
               opts
             )
           ) do
      case cdc_module.start_link(
             build_cdc_opts(
               connection_opts,
               slot_name,
               publication_name,
               start_lsn,
               service_module,
               service_pid,
               opts
             )
           ) do
        {:ok, cdc_pid} ->
          {:ok,
           %{service_pid: service_pid, cdc_pid: cdc_pid, start_lsn: start_lsn, source: source}}

        {:error, reason} ->
          _ = GenServer.stop(service_pid)
          {:error, reason}
      end
    end
  end

  defp build_service_opts(
         meta_conn,
         source,
         designated_tables,
         storage_config,
         meta_module,
         opts
       ) do
    [
      name: Keyword.get(opts, :service_name),
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
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
  end

  defp build_cdc_opts(
         connection_opts,
         slot_name,
         publication_name,
         start_lsn,
         service_module,
         service_pid,
         opts
       ) do
    event_sink = fn event ->
      case service_module.push_event(service_pid, event) do
        {:error, reason} -> {:error, reason}
        _ -> :ok
      end
    end

    [
      name: Keyword.get(opts, :cdc_name),
      connection_opts: connection_opts,
      slot_name: slot_name,
      publication_name: publication_name,
      start_lsn: start_lsn,
      status_interval_ms: Keyword.get(opts, :status_interval_ms, 10_000),
      decoder_module: Keyword.get(opts, :decoder_module),
      converter_module: Keyword.get(opts, :converter_module),
      event_sink: event_sink,
      auto_reconnect: Keyword.get(opts, :auto_reconnect, true),
      sync_connect: Keyword.get(opts, :sync_connect, true)
    ]
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
  end

  defp maybe_bootstrap_start_lsn(
         connection_opts,
         slot_name,
         publication_name,
         designated_tables,
         opts
       ) do
    if Keyword.get(opts, :bootstrap_replication?, true) do
      bootstrap_module = Keyword.get(opts, :bootstrap_module, Bootstrap)
      query_connect_fun = Keyword.get(opts, :query_connect_fun, &Postgrex.start_link/1)
      query_disconnect_fun = Keyword.get(opts, :query_disconnect_fun, &GenServer.stop/1)

      case query_connect_fun.(connection_opts) do
        {:ok, query_conn} ->
          result =
            bootstrap_module.bootstrap(query_conn, %{
              publication_name: publication_name,
              slot_name: slot_name,
              designated_tables: designated_tables
            })

          _ = safe_disconnect_query_conn(query_disconnect_fun, query_conn)

          case result do
            {:ok, %{start_lsn: start_lsn}} -> {:ok, start_lsn}
            {:error, _reason} = error -> error
          end

        {:error, reason} ->
          {:error, {:query_connection_failed, reason}}
      end
    else
      {:ok, nil}
    end
  end

  defp maybe_snapshot_boundary_lsn(connection_opts, designated_tables, opts) do
    if Keyword.get(opts, :snapshot_before_stream?, false) do
      snapshot_runner_module =
        Keyword.get(opts, :snapshot_runner_module, DuckFeeder.CDC.InitialSnapshot.Runner)

      query_connect_fun = Keyword.get(opts, :query_connect_fun, &Postgrex.start_link/1)
      query_disconnect_fun = Keyword.get(opts, :query_disconnect_fun, &GenServer.stop/1)
      snapshot_runner_opts = Keyword.get(opts, :snapshot_runner_opts, [])

      with {:ok, row_handler} <- fetch_snapshot_row_handler(opts),
           {:ok, query_conn} <- query_connect_fun.(connection_opts) do
        result =
          snapshot_runner_module.run(
            query_conn,
            designated_tables,
            Keyword.merge(snapshot_runner_opts, row_handler: row_handler)
          )

        _ = safe_disconnect_query_conn(query_disconnect_fun, query_conn)

        case result do
          {:ok, %{boundary_lsn: boundary_lsn}} -> {:ok, boundary_lsn}
          {:error, reason} -> {:error, {:initial_snapshot_failed, reason}}
        end
      else
        {:error, :missing_snapshot_row_handler} = error -> error
        {:error, reason} -> {:error, {:query_connection_failed, reason}}
      end
    else
      {:ok, nil}
    end
  end

  defp fetch_snapshot_row_handler(opts) do
    case Keyword.get(opts, :snapshot_row_handler) do
      handler when is_function(handler, 2) -> {:ok, handler}
      _ -> {:error, :missing_snapshot_row_handler}
    end
  end

  defp resolve_start_lsn(meta_start_lsn, candidates) when is_list(candidates) do
    candidates
    |> Enum.reject(&is_nil/1)
    |> Enum.reduce_while({:ok, meta_start_lsn}, fn candidate, {:ok, current} ->
      case Lsn.max(current, candidate) do
        {:error, _reason} = error -> {:halt, error}
        max_lsn -> {:cont, {:ok, max_lsn}}
      end
    end)
  end

  defp safe_disconnect_query_conn(disconnect_fun, query_conn) do
    disconnect_fun.(query_conn)
  rescue
    _ -> :ok
  catch
    _, _ -> :ok
  end

  defp require_source_field(source, key) do
    case Map.get(source, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, {:missing_source_field, key}}
    end
  end
end
