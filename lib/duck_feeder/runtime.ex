defmodule DuckFeeder.Runtime do
  @moduledoc """
  Runtime wiring helpers to start `DuckFeeder.Service` from metadata tables.
  """

  alias DuckFeeder.{Meta, Service}
  alias DuckFeeder.CDC.{Bootstrap, Connection, ConnectionOptions, Lsn}

  @default_reconnect_backoff 1_000

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
         committer_module: Keyword.get(opts, :committer_module),
         committer_opts: Keyword.get(opts, :committer_opts),
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
         {:ok, snapshot_handoff} <-
           fetch_snapshot_handoff(meta_module, meta_conn, source.id),
         {:ok, snapshot_plan} <- snapshot_plan(meta_start_lsn, snapshot_handoff, opts),
         {:ok, connection_opts} <- connection_options_module.resolve(source, opts),
         {:ok, bootstrap_start_lsn} <-
           maybe_bootstrap_start_lsn(
             connection_opts,
             slot_name,
             publication_name,
             designated_tables,
             opts
           ),
         {:ok, snapshot_result} <-
           maybe_snapshot_boundary_lsn(
             connection_opts,
             designated_tables,
             meta_start_lsn,
             snapshot_plan,
             opts
           ),
         {:ok, start_lsn} <-
           resolve_start_lsn(meta_start_lsn, [bootstrap_start_lsn, snapshot_result.boundary_lsn]),
         {:ok, snapshot_replay_plan} <-
           snapshot_replay_plan(meta_start_lsn, snapshot_result),
         {:ok, service_pid} <-
           service_module.start_link(
             build_service_opts(
               meta_conn,
               source,
               designated_tables,
               storage_config,
               meta_module,
               with_snapshot_lsn_start(opts, snapshot_replay_plan.snapshot_lsn_start)
             )
           ) do
      with :ok <-
             maybe_mark_snapshot_handoff_pending(
               meta_module,
               meta_conn,
               source.id,
               snapshot_plan,
               snapshot_result,
               opts
             ),
           :ok <-
             maybe_replay_snapshot_rows(service_module, service_pid, snapshot_replay_plan.rows) do
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
            with :ok <- attach_cdc_to_service(service_module, service_pid, cdc_pid),
                 :ok <-
                   maybe_mark_snapshot_handoff_complete(
                     meta_module,
                     meta_conn,
                     source.id,
                     snapshot_result,
                     opts
                   ) do
              {:ok,
               %{
                 service_pid: service_pid,
                 cdc_pid: cdc_pid,
                 start_lsn: start_lsn,
                 source: source
               }}
            else
              {:error, {:service_attach_cdc_failed, _} = reason} ->
                _ = safe_stop_cdc(cdc_pid)
                _ = safe_stop_service(service_pid)
                {:error, reason}

              {:error, reason} ->
                _ = safe_stop_cdc(cdc_pid)
                _ = safe_stop_service(service_pid)
                {:error, {:snapshot_handoff_mark_complete_failed, reason}}
            end

          {:error, reason} ->
            _ = safe_stop_service(service_pid)
            {:error, reason}
        end
      else
        {:error, reason} ->
          _ = safe_stop_service(service_pid)
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
      committer_module: Keyword.get(opts, :committer_module),
      committer_opts: Keyword.get(opts, :committer_opts),
      object_prefix: Keyword.get(opts, :object_prefix, source.name),
      pipeline_opts: Keyword.get(opts, :pipeline_opts, %{}),
      max_tx_changes: Keyword.get(opts, :max_tx_changes),
      observer_pid: Keyword.get(opts, :observer_pid),
      snapshot_lsn_start: Keyword.get(opts, :snapshot_lsn_start),
      max_inflight_batches: Keyword.get(opts, :max_inflight_batches),
      max_pending_batches: Keyword.get(opts, :max_pending_batches),
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
    event_sink_mode = Keyword.get(opts, :event_sink_mode, :pid)

    event_sink =
      case event_sink_mode do
        :pid ->
          service_pid

        :call ->
          fn event ->
            case service_module.push_event(service_pid, event) do
              {:error, reason} -> {:error, reason}
              _ -> :ok
            end
          end
      end

    reconnect_backoff =
      case Keyword.fetch(opts, :reconnect_backoff) do
        {:ok, value} -> value
        :error -> @default_reconnect_backoff
      end

    reconnect_backoff =
      normalize_reconnect_backoff(
        reconnect_backoff,
        Keyword.get(opts, :reconnect_backoff_min_ms),
        Keyword.get(opts, :reconnect_backoff_max_ms),
        Keyword.get(opts, :reconnect_backoff_jitter_ms, 0),
        Keyword.get(opts, :reconnect_backoff_jitter_fun)
      )

    [
      name: Keyword.get(opts, :cdc_name),
      connection_opts: connection_opts,
      slot_name: slot_name,
      publication_name: publication_name,
      start_lsn: start_lsn,
      status_interval_ms: Keyword.get(opts, :status_interval_ms, 10_000),
      max_lag_bytes: Keyword.get(opts, :max_lag_bytes),
      backpressure_lag_bytes: Keyword.get(opts, :backpressure_lag_bytes),
      decoder_module: Keyword.get(opts, :decoder_module),
      converter_module: Keyword.get(opts, :converter_module),
      event_sink: event_sink,
      auto_reconnect: Keyword.get(opts, :auto_reconnect, true),
      reconnect_backoff: reconnect_backoff,
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
            safe_bootstrap(
              bootstrap_module,
              query_conn,
              publication_name,
              slot_name,
              designated_tables
            )

          _ = safe_disconnect_query_conn(query_disconnect_fun, query_conn)

          case result do
            {:ok, %{slot: {:created, _slot}, start_lsn: start_lsn}} when is_binary(start_lsn) ->
              {:ok, start_lsn}

            {:ok, %{slot: :exists}} ->
              {:ok, nil}

            {:ok, %{start_lsn: start_lsn}} when is_binary(start_lsn) ->
              {:ok, start_lsn}

            {:ok, other} ->
              {:error, {:invalid_bootstrap_result, other}}

            {:error, _reason} = error ->
              error
          end

        {:error, reason} ->
          {:error, {:query_connection_failed, reason}}
      end
    else
      {:ok, nil}
    end
  end

  defp maybe_snapshot_boundary_lsn(
         _connection_opts,
         _designated_tables,
         _meta_start_lsn,
         %{run_snapshot?: false, handoff_boundary_lsn: boundary_lsn},
         _opts
       ) do
    {:ok, %{boundary_lsn: boundary_lsn, rows: []}}
  end

  defp maybe_snapshot_boundary_lsn(
         connection_opts,
         designated_tables,
         _meta_start_lsn,
         %{run_snapshot?: true},
         opts
       ) do
    snapshot_runner_module =
      Keyword.get(opts, :snapshot_runner_module, DuckFeeder.CDC.InitialSnapshot.Runner)

    query_connect_fun = Keyword.get(opts, :query_connect_fun, &Postgrex.start_link/1)
    query_disconnect_fun = Keyword.get(opts, :query_disconnect_fun, &GenServer.stop/1)
    snapshot_runner_opts = Keyword.get(opts, :snapshot_runner_opts, [])

    case snapshot_row_handler_with_collector(opts) do
      {:ok, row_handler, collect_rows} ->
        case query_connect_fun.(connection_opts) do
          {:ok, query_conn} ->
            result =
              safe_snapshot_run(
                snapshot_runner_module,
                query_conn,
                designated_tables,
                Keyword.merge(snapshot_runner_opts, row_handler: row_handler)
              )

            rows = collect_rows.()
            _ = safe_disconnect_query_conn(query_disconnect_fun, query_conn)

            case result do
              {:ok, %{boundary_lsn: boundary_lsn}} ->
                {:ok, %{boundary_lsn: boundary_lsn, rows: rows}}

              {:ok, other} ->
                {:error, {:initial_snapshot_failed, {:invalid_snapshot_result, other}}}

              {:error, reason} ->
                {:error, {:initial_snapshot_failed, reason}}
            end

          {:error, reason} ->
            _ = collect_rows.()
            {:error, {:query_connection_failed, reason}}
        end

      {:error, :missing_snapshot_row_handler} = error ->
        error
    end
  end

  defp snapshot_plan(_meta_start_lsn, _snapshot_handoff, opts)
       when not is_list(opts),
       do: {:ok, %{run_snapshot?: false, mark_pending?: false, handoff_boundary_lsn: nil}}

  defp snapshot_plan(meta_start_lsn, snapshot_handoff, opts) when is_binary(meta_start_lsn) do
    snapshot_before_stream? = Keyword.get(opts, :snapshot_before_stream?, false)

    cond do
      match?(%{state: :pending}, snapshot_handoff) ->
        snapshot_plan_from_pending_handoff(
          meta_start_lsn,
          snapshot_handoff,
          snapshot_before_stream?,
          opts
        )

      match?(%{state: :complete}, snapshot_handoff) ->
        if snapshot_before_stream? and Keyword.get(opts, :snapshot_on_restart?, false) do
          {:ok, %{run_snapshot?: true, mark_pending?: true, handoff_boundary_lsn: nil}}
        else
          {:ok, %{run_snapshot?: false, mark_pending?: false, handoff_boundary_lsn: nil}}
        end

      snapshot_before_stream? ->
        if should_run_snapshot_before_stream?(meta_start_lsn, opts) do
          {:ok, %{run_snapshot?: true, mark_pending?: true, handoff_boundary_lsn: nil}}
        else
          {:ok, %{run_snapshot?: false, mark_pending?: false, handoff_boundary_lsn: nil}}
        end

      true ->
        {:ok, %{run_snapshot?: false, mark_pending?: false, handoff_boundary_lsn: nil}}
    end
  end

  defp snapshot_plan_from_pending_handoff(
         meta_start_lsn,
         %{boundary_lsn: boundary_lsn} = snapshot_handoff,
         snapshot_before_stream?,
         opts
       ) do
    if Keyword.get(opts, :resume_incomplete_snapshot?, false) do
      with {:ok, meta_at_or_past_boundary?} <- lsn_at_or_past?(meta_start_lsn, boundary_lsn) do
        cond do
          meta_at_or_past_boundary? ->
            {:ok,
             %{run_snapshot?: false, mark_pending?: false, handoff_boundary_lsn: boundary_lsn}}

          snapshot_before_stream? ->
            {:ok, %{run_snapshot?: true, mark_pending?: true, handoff_boundary_lsn: boundary_lsn}}

          true ->
            {:error, {:snapshot_resume_requires_snapshot_before_stream, snapshot_handoff}}
        end
      end
    else
      {:error, {:snapshot_handoff_incomplete, snapshot_handoff}}
    end
  end

  defp snapshot_plan_from_pending_handoff(
         _meta_start_lsn,
         snapshot_handoff,
         _snapshot_before_stream?,
         _opts
       ) do
    {:error, {:snapshot_handoff_incomplete, snapshot_handoff}}
  end

  defp lsn_at_or_past?(_meta_start_lsn, nil), do: {:ok, false}

  defp lsn_at_or_past?(meta_start_lsn, boundary_lsn)
       when is_binary(meta_start_lsn) and is_binary(boundary_lsn) do
    case Lsn.compare(meta_start_lsn, boundary_lsn) do
      :eq -> {:ok, true}
      :gt -> {:ok, true}
      :lt -> {:ok, false}
      {:error, reason} -> {:error, {:invalid_snapshot_handoff_lsn, reason}}
    end
  end

  defp should_run_snapshot_before_stream?(meta_start_lsn, opts) when is_binary(meta_start_lsn) do
    if Keyword.get(opts, :snapshot_on_restart?, false) do
      true
    else
      default_start_lsn = Keyword.get(opts, :default_start_lsn, "0/0")

      case Lsn.compare(meta_start_lsn, default_start_lsn) do
        :eq -> true
        _ -> false
      end
    end
  end

  defp fetch_snapshot_handoff(meta_module, meta_conn, source_id)
       when is_atom(meta_module) and is_integer(source_id) and source_id > 0 do
    if function_exported?(meta_module, :fetch_snapshot_handoff, 2) do
      meta_module.fetch_snapshot_handoff(meta_conn, source_id)
    else
      {:ok, nil}
    end
  end

  defp maybe_mark_snapshot_handoff_pending(
         _meta_module,
         _meta_conn,
         source_id,
         %{mark_pending?: false},
         _snapshot_result,
         _opts
       )
       when is_integer(source_id) and source_id > 0,
       do: :ok

  defp maybe_mark_snapshot_handoff_pending(
         meta_module,
         meta_conn,
         source_id,
         %{mark_pending?: true},
         %{boundary_lsn: boundary_lsn},
         opts
       )
       when is_atom(meta_module) and is_integer(source_id) and source_id > 0 do
    if is_binary(boundary_lsn) and
         function_exported?(meta_module, :mark_snapshot_handoff_pending, 3) do
      retry_mark_snapshot_handoff(opts, fn ->
        case meta_module.mark_snapshot_handoff_pending(meta_conn, source_id, boundary_lsn) do
          {:ok, _} -> :ok
          {:error, _reason} = error -> error
        end
      end)
    else
      :ok
    end
  end

  defp maybe_mark_snapshot_handoff_complete(
         meta_module,
         meta_conn,
         source_id,
         %{boundary_lsn: boundary_lsn},
         opts
       )
       when is_atom(meta_module) and is_integer(source_id) and source_id > 0 do
    if is_binary(boundary_lsn) and
         function_exported?(meta_module, :mark_snapshot_handoff_complete, 3) do
      retry_mark_snapshot_handoff(opts, fn ->
        case meta_module.mark_snapshot_handoff_complete(meta_conn, source_id, boundary_lsn) do
          {:ok, _} -> :ok
          {:error, _reason} = error -> error
        end
      end)
    else
      :ok
    end
  end

  defp retry_mark_snapshot_handoff(opts, mark_fun)
       when is_list(opts) and is_function(mark_fun, 0) do
    retries =
      normalize_snapshot_handoff_mark_retries(
        Keyword.get(opts, :snapshot_handoff_mark_retries, 2)
      )

    delay_ms =
      normalize_snapshot_handoff_mark_retry_delay_ms(
        Keyword.get(opts, :snapshot_handoff_mark_retry_delay_ms, 0)
      )

    do_retry_mark_snapshot_handoff(mark_fun, retries, delay_ms)
  end

  defp do_retry_mark_snapshot_handoff(mark_fun, retries_left, delay_ms)
       when is_function(mark_fun, 0) and is_integer(retries_left) and retries_left >= 0 do
    case mark_fun.() do
      :ok ->
        :ok

      {:error, _reason} = error ->
        if retries_left > 0 do
          if delay_ms > 0, do: Process.sleep(delay_ms)
          do_retry_mark_snapshot_handoff(mark_fun, retries_left - 1, delay_ms)
        else
          error
        end

      other ->
        {:error, {:invalid_snapshot_handoff_mark_result, other}}
    end
  end

  defp normalize_snapshot_handoff_mark_retries(value)
       when is_integer(value) and value >= 0,
       do: value

  defp normalize_snapshot_handoff_mark_retries(_value), do: 2

  defp normalize_snapshot_handoff_mark_retry_delay_ms(value)
       when is_integer(value) and value >= 0,
       do: value

  defp normalize_snapshot_handoff_mark_retry_delay_ms(_value), do: 0

  defp snapshot_row_handler_with_collector(opts) do
    case Keyword.get(opts, :snapshot_row_handler) do
      handler when is_function(handler, 2) ->
        {:ok, handler, fn -> [] end}

      _ ->
        if Keyword.get(opts, :snapshot_ingest?, true) do
          case Agent.start_link(fn -> [] end) do
            {:ok, collector} ->
              row_handler = fn designated_table, row ->
                snapshot_collector_push(collector, designated_table, row)
              end

              collect_rows = fn -> snapshot_collector_drain(collector) end

              {:ok, row_handler, collect_rows}

            {:error, reason} ->
              {:error, {:snapshot_collector_start_failed, reason}}
          end
        else
          {:error, :missing_snapshot_row_handler}
        end
    end
  end

  defp snapshot_collector_push(collector, designated_table, row) when is_pid(collector) do
    Agent.update(collector, fn rows -> [{designated_table, row} | rows] end)
  rescue
    exception ->
      {:error, {:snapshot_collector_push_exception, exception}}
  catch
    :exit, reason ->
      {:error, {:snapshot_collector_push_exit, reason}}

    kind, reason ->
      {:error, {:snapshot_collector_push_throw, kind, reason}}
  else
    :ok -> :ok
    other -> {:error, {:invalid_snapshot_collector_push_result, other}}
  end

  defp snapshot_collector_drain(collector) when is_pid(collector) do
    rows =
      try do
        Agent.get_and_update(collector, fn current_rows -> {Enum.reverse(current_rows), []} end)
      rescue
        _ -> []
      catch
        _, _ -> []
      end

    _ = safe_stop_collector(collector)
    rows
  end

  defp safe_stop_collector(collector) when is_pid(collector) do
    if Process.alive?(collector), do: Agent.stop(collector)
    :ok
  rescue
    _ -> :ok
  catch
    _, _ -> :ok
  end

  defp snapshot_replay_plan(meta_start_lsn, %{boundary_lsn: nil, rows: rows})
       when is_binary(meta_start_lsn) and is_list(rows) do
    {:ok, %{rows: [], snapshot_lsn_start: nil}}
  end

  defp snapshot_replay_plan(meta_start_lsn, %{boundary_lsn: boundary_lsn, rows: rows})
       when is_binary(meta_start_lsn) and is_binary(boundary_lsn) and is_list(rows) do
    row_count = length(rows)

    case Lsn.compare(meta_start_lsn, boundary_lsn) do
      :lt ->
        with {:ok, snapshot_lsn_start_counter} <-
               snapshot_lsn_start_counter(boundary_lsn, row_count),
             {:ok, replayed_count} <-
               replayed_snapshot_row_count(meta_start_lsn, snapshot_lsn_start_counter, row_count) do
          remaining_rows = Enum.drop(rows, replayed_count)
          snapshot_lsn_start = Lsn.to_string(snapshot_lsn_start_counter + replayed_count)

          {:ok, %{rows: remaining_rows, snapshot_lsn_start: snapshot_lsn_start}}
        end

      :eq ->
        {:ok, %{rows: [], snapshot_lsn_start: nil}}

      :gt ->
        {:ok, %{rows: [], snapshot_lsn_start: nil}}

      {:error, reason} ->
        {:error, {:invalid_snapshot_handoff_lsn, reason}}
    end
  end

  defp snapshot_lsn_start_counter(boundary_lsn, row_count)
       when is_binary(boundary_lsn) and is_integer(row_count) and row_count >= 0 do
    with {:ok, boundary} <- Lsn.parse(boundary_lsn) do
      {:ok, max(boundary - row_count, 0)}
    end
  end

  defp replayed_snapshot_row_count(meta_start_lsn, snapshot_lsn_start_counter, row_count)
       when is_binary(meta_start_lsn) and is_integer(snapshot_lsn_start_counter) and
              is_integer(row_count) and row_count >= 0 do
    with {:ok, meta_counter} <- Lsn.parse(meta_start_lsn) do
      replayed = max(meta_counter - snapshot_lsn_start_counter, 0)
      {:ok, min(replayed, row_count)}
    end
  end

  defp with_snapshot_lsn_start(opts, nil) when is_list(opts), do: opts

  defp with_snapshot_lsn_start(opts, snapshot_lsn_start) when is_list(opts) do
    Keyword.put(opts, :snapshot_lsn_start, snapshot_lsn_start)
  end

  defp maybe_replay_snapshot_rows(_service_module, _service_pid, []), do: :ok

  defp maybe_replay_snapshot_rows(service_module, service_pid, rows)
       when is_list(rows) do
    Enum.reduce_while(rows, :ok, fn {designated_table, row}, :ok ->
      case safe_snapshot_ingest(service_module, service_pid, designated_table, row) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {:snapshot_replay_failed, reason}}}
      end
    end)
    |> case do
      :ok ->
        :ok

      {:error, _reason} = error ->
        _ = safe_stop_service(service_pid)
        error
    end
  end

  defp safe_bootstrap(
         bootstrap_module,
         query_conn,
         publication_name,
         slot_name,
         designated_tables
       ) do
    bootstrap_module.bootstrap(query_conn, %{
      publication_name: publication_name,
      slot_name: slot_name,
      designated_tables: designated_tables
    })
  rescue
    exception ->
      {:error, {:bootstrap_exception, exception}}
  catch
    kind, reason ->
      {:error, {:bootstrap_throw, kind, reason}}
  end

  defp safe_snapshot_run(
         snapshot_runner_module,
         query_conn,
         designated_tables,
         snapshot_runner_opts
       ) do
    snapshot_runner_module.run(query_conn, designated_tables, snapshot_runner_opts)
  rescue
    exception ->
      {:error, {:snapshot_runner_exception, exception}}
  catch
    kind, reason ->
      {:error, {:snapshot_runner_throw, kind, reason}}
  end

  defp safe_snapshot_ingest(service_module, service_pid, designated_table, row) do
    case service_module.ingest_snapshot_row(service_pid, designated_table, row) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
      other -> {:error, {:invalid_snapshot_ingest_result, other}}
    end
  rescue
    exception ->
      {:error, {:snapshot_ingest_exception, exception}}
  catch
    :exit, reason ->
      {:error, {:snapshot_ingest_exit, reason}}

    kind, reason ->
      {:error, {:snapshot_ingest_throw, kind, reason}}
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

  defp attach_cdc_to_service(service_module, service_pid, cdc_pid)
       when is_atom(service_module) and is_pid(service_pid) and is_pid(cdc_pid) do
    if function_exported?(service_module, :attach_cdc, 2) do
      case service_module.attach_cdc(service_pid, cdc_pid) do
        :ok -> :ok
        {:error, reason} -> {:error, {:service_attach_cdc_failed, reason}}
        other -> {:error, {:service_attach_cdc_failed, {:invalid_attach_result, other}}}
      end
    else
      :ok
    end
  rescue
    exception ->
      {:error, {:service_attach_cdc_failed, {:exception, exception}}}
  catch
    kind, reason ->
      {:error, {:service_attach_cdc_failed, {kind, reason}}}
  end

  defp safe_stop_service(service_pid) when is_pid(service_pid) do
    if Process.alive?(service_pid) do
      GenServer.stop(service_pid)
    else
      :ok
    end
  rescue
    _ -> :ok
  catch
    _, _ -> :ok
  end

  defp safe_stop_cdc(cdc_pid) when is_pid(cdc_pid) do
    if Process.alive?(cdc_pid), do: Process.exit(cdc_pid, :shutdown)
    :ok
  rescue
    _ -> :ok
  catch
    _, _ -> :ok
  end

  defp safe_disconnect_query_conn(disconnect_fun, query_conn) do
    disconnect_fun.(query_conn)
  rescue
    _ -> :ok
  catch
    _, _ -> :ok
  end

  defp normalize_reconnect_backoff(base_backoff, min_ms, max_ms, jitter_ms, jitter_fun)
       when is_integer(base_backoff) do
    {min_ms, max_ms} = normalize_reconnect_backoff_bounds(min_ms, max_ms)
    jitter_ms = normalize_non_neg_integer(jitter_ms, 0)

    bounded = clamp_reconnect_backoff(base_backoff, min_ms, max_ms)

    jitter =
      case jitter_fun do
        fun when is_function(fun, 2) ->
          fun.(bounded, jitter_ms)
          |> normalize_reconnect_jitter(jitter_ms)

        _ ->
          if jitter_ms > 0, do: :rand.uniform(jitter_ms * 2 + 1) - (jitter_ms + 1), else: 0
      end

    bounded
    |> Kernel.+(jitter)
    |> clamp_reconnect_backoff(min_ms, max_ms)
  end

  defp normalize_reconnect_backoff(_base_backoff, _min_ms, _max_ms, _jitter_ms, _jitter_fun),
    do: @default_reconnect_backoff

  defp normalize_reconnect_backoff_bounds(min_ms, max_ms) do
    min_ms = normalize_non_neg_integer(min_ms, 0)

    max_ms =
      case max_ms do
        value when is_integer(value) and value >= min_ms -> value
        _ -> nil
      end

    {min_ms, max_ms}
  end

  defp clamp_reconnect_backoff(value, min_ms, nil) when is_integer(value), do: max(value, min_ms)

  defp clamp_reconnect_backoff(value, min_ms, max_ms)
       when is_integer(value) and is_integer(max_ms),
       do: value |> max(min_ms) |> min(max_ms)

  defp normalize_reconnect_jitter(value, jitter_ms)
       when is_integer(value) and is_integer(jitter_ms) and jitter_ms >= 0 do
    cond do
      value < -jitter_ms -> -jitter_ms
      value > jitter_ms -> jitter_ms
      true -> value
    end
  end

  defp normalize_reconnect_jitter(_value, _jitter_ms), do: 0

  defp normalize_non_neg_integer(value, _default)
       when is_integer(value) and value >= 0,
       do: value

  defp normalize_non_neg_integer(_value, default), do: default

  defp require_source_field(source, key) do
    case Map.get(source, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, {:missing_source_field, key}}
    end
  end
end
