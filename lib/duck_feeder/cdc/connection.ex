defmodule DuckFeeder.CDC.Connection do
  @moduledoc """
  PostgreSQL logical replication client based on `Postgrex.ReplicationConnection`.

  Responsibilities:
  - start logical replication stream (`START_REPLICATION ...`)
  - decode pgoutput messages
  - convert wire messages into normalized `DuckFeeder.CDC.Event` structs
  - dispatch normalized events to a sink process/function
  - emit standby status updates (acks)

  Ack model (durable-checkpoint driven):

      WAL frame -> decode -> dispatch event to sink (Service)
                                            |
                                            v
                              batch commit persists checkpoint_lsn
                                            |
                                            v
                             {:duck_feeder_ack_lsn, checkpoint_lsn}
                                            |
                                            v
                         apply_ack_lsn + standby_status_update

  The connection does not eagerly acknowledge commit decode; it advances applied
  LSN from explicit ack feedback to avoid ack-before-durable-commit loss windows.
  """

  use Postgrex.ReplicationConnection

  alias DuckFeeder.CDC.{Lsn, ReplicationProtocol}
  alias DuckFeeder.CDC.Event
  alias DuckFeeder.CDC.LogicalReplication.{Converter, Decoder}

  @type event_sink :: pid() | (Event.t() -> :ok | {:error, term()}) | {module(), atom(), [term()]}

  defmodule State do
    @enforce_keys [
      :slot_name,
      :publication_name,
      :start_lsn,
      :event_sink,
      :decoder_module,
      :converter_module,
      :converter_state,
      :status_interval_ms
    ]
    defstruct [
      :slot_name,
      :publication_name,
      :start_lsn,
      :event_sink,
      :decoder_module,
      :converter_module,
      :converter_state,
      :status_interval_ms,
      :max_lag_bytes,
      :backpressure_lag_bytes,
      :status_timer_ref,
      :last_disconnect_monotonic_ms,
      :connected_at_monotonic_ms,
      step: :disconnected,
      received_lsn: 0,
      flushed_lsn: 0,
      applied_lsn: 0,
      reconnect_count: 0,
      backpressure_active: false
    ]

    @type t :: %__MODULE__{
            slot_name: String.t(),
            publication_name: String.t(),
            start_lsn: String.t(),
            event_sink: DuckFeeder.CDC.Connection.event_sink(),
            decoder_module: module(),
            converter_module: module(),
            converter_state: term(),
            status_interval_ms: non_neg_integer(),
            max_lag_bytes: non_neg_integer() | nil,
            backpressure_lag_bytes: non_neg_integer() | nil,
            status_timer_ref: reference() | nil,
            last_disconnect_monotonic_ms: integer() | nil,
            connected_at_monotonic_ms: integer() | nil,
            step: :disconnected | :streaming,
            received_lsn: non_neg_integer(),
            flushed_lsn: non_neg_integer(),
            applied_lsn: non_neg_integer(),
            reconnect_count: non_neg_integer(),
            backpressure_active: boolean()
          }
  end

  @type option ::
          {:name, GenServer.name()}
          | {:connection_opts, keyword()}
          | {:slot_name, String.t()}
          | {:publication_name, String.t()}
          | {:start_lsn, String.t()}
          | {:event_sink, event_sink()}
          | {:decoder_module, module()}
          | {:converter_module, module()}
          | {:converter_state, term()}
          | {:status_interval_ms, non_neg_integer()}
          | {:max_lag_bytes, non_neg_integer()}
          | {:backpressure_lag_bytes, non_neg_integer()}
          | {:auto_reconnect, boolean()}
          | {:reconnect_backoff, non_neg_integer()}
          | {:sync_connect, boolean()}

  @spec start_link([option()]) :: :gen_statem.start_ret()
  def start_link(opts) do
    {name_opt, opts} = Keyword.pop(opts, :name)
    connection_opts = Keyword.fetch!(opts, :connection_opts)

    init_arg =
      opts
      |> Keyword.drop([
        :connection_opts,
        :name,
        :auto_reconnect,
        :reconnect_backoff,
        :sync_connect
      ])

    start_opts =
      connection_opts
      |> maybe_put(:name, name_opt)
      |> Keyword.put_new(:auto_reconnect, Keyword.get(opts, :auto_reconnect, true))
      |> maybe_put(:reconnect_backoff, Keyword.get(opts, :reconnect_backoff))
      |> Keyword.put_new(:sync_connect, Keyword.get(opts, :sync_connect, true))

    Postgrex.ReplicationConnection.start_link(__MODULE__, init_arg, start_opts)
  end

  @spec ack_lsn(GenServer.server(), String.t() | non_neg_integer(), timeout()) ::
          :ok | {:error, term()}
  def ack_lsn(server, lsn, timeout \\ 5_000) do
    Postgrex.ReplicationConnection.call(server, {:ack_lsn, lsn}, timeout)
  catch
    :exit, reason ->
      {:error, {:ack_call_exit, reason}}
  end

  @impl true
  def init(opts) do
    slot_name = Keyword.fetch!(opts, :slot_name)
    publication_name = Keyword.fetch!(opts, :publication_name)
    start_lsn = Keyword.get(opts, :start_lsn, "0/0")
    start_lsn_int = Lsn.parse!(start_lsn)
    event_sink = Keyword.fetch!(opts, :event_sink)

    decoder_module = Keyword.get(opts, :decoder_module, Decoder)
    converter_module = Keyword.get(opts, :converter_module, Converter)

    converter_state =
      Keyword.get_lazy(opts, :converter_state, fn ->
        converter_module.new()
      end)

    status_interval_ms = Keyword.get(opts, :status_interval_ms, 10_000)
    max_lag_bytes = Keyword.get(opts, :max_lag_bytes)
    backpressure_lag_bytes = Keyword.get(opts, :backpressure_lag_bytes)

    {:ok,
     %State{
       slot_name: slot_name,
       publication_name: publication_name,
       start_lsn: start_lsn,
       event_sink: event_sink,
       decoder_module: decoder_module,
       converter_module: converter_module,
       converter_state: converter_state,
       status_interval_ms: status_interval_ms,
       max_lag_bytes: max_lag_bytes,
       backpressure_lag_bytes: backpressure_lag_bytes,
       received_lsn: start_lsn_int,
       flushed_lsn: start_lsn_int,
       applied_lsn: start_lsn_int
     }}
  end

  @impl true
  def handle_connect(%State{} = state) do
    query =
      ReplicationProtocol.start_replication_sql(
        state.slot_name,
        state.start_lsn,
        state.publication_name
      )

    now_ms = System.monotonic_time(:millisecond)

    DuckFeeder.Telemetry.cdc_connection(:stream_starting, %{
      slot_name: state.slot_name,
      publication_name: state.publication_name,
      start_lsn: state.start_lsn,
      reconnect_count: state.reconnect_count
    })

    if state.reconnect_count > 0 and is_integer(state.last_disconnect_monotonic_ms) do
      DuckFeeder.Telemetry.cdc_connection(:reconnected, %{
        slot_name: state.slot_name,
        publication_name: state.publication_name,
        reconnect_count: state.reconnect_count,
        downtime_ms: max(now_ms - state.last_disconnect_monotonic_ms, 0)
      })
    end

    {:stream, query, [],
     state
     |> Map.put(:step, :streaming)
     |> Map.put(:connected_at_monotonic_ms, now_ms)
     |> schedule_status_tick()}
  end

  @impl true
  def handle_data(<<?k, wal_end::64, _clock::64-signed, reply::8>>, %State{} = state) do
    state = bump_received_lsn(state, wal_end)

    with :ok <- maybe_enforce_lag(state) do
      if reply == 1 do
        DuckFeeder.Telemetry.cdc_frame(:keepalive, :ack_requested, %{wal_end: wal_end})
        state = maybe_advance_flush_from_keepalive(state, wal_end)
        state = maybe_track_backpressure(state, :keepalive)
        {:noreply, [standby_status_update(state)], state}
      else
        DuckFeeder.Telemetry.cdc_frame(:keepalive, :noop, %{wal_end: wal_end})
        state = maybe_track_backpressure(state, :keepalive)
        {:noreply, [], state}
      end
    else
      {:error, reason} ->
        disconnect_with_reason(state, reason)
    end
  end

  def handle_data(
        <<?w, _wal_start::64, wal_end::64, _clock::64-signed, payload::binary>>,
        %State{} = state
      ) do
    decoded = state.decoder_module.decode(payload)

    case state.converter_module.convert(decoded, state.converter_state) do
      {:ignore, converter_state} ->
        state = %{state | converter_state: converter_state} |> bump_received_lsn(wal_end)

        with :ok <- maybe_enforce_lag(state) do
          DuckFeeder.Telemetry.cdc_frame(:xlog, :ignored, %{wal_end: wal_end})
          state = maybe_track_backpressure(state, :xlog)
          {:noreply, [], state}
        else
          {:error, reason} -> disconnect_with_reason(state, reason)
        end

      {:ok, event, converter_state} ->
        with :ok <- dispatch_event(state.event_sink, event) do
          state =
            state
            |> Map.put(:converter_state, converter_state)
            |> bump_received_lsn(wal_end)

          with :ok <- maybe_enforce_lag(state) do
            DuckFeeder.Telemetry.cdc_frame(:xlog, :event, %{
              wal_end: wal_end,
              event_type: event.__struct__
            })

            {acks, state} = maybe_ack_event(state, event)
            state = maybe_track_backpressure(state, :xlog)
            {:noreply, acks, state}
          else
            {:error, reason} -> disconnect_with_reason(state, reason)
          end
        else
          {:error, reason} ->
            DuckFeeder.Telemetry.cdc_connection(:event_sink_error, %{reason: reason})
            disconnect_with_reason(state, {:event_sink_failed, reason})
        end

      {:error, reason} ->
        DuckFeeder.Telemetry.cdc_connection(:convert_error, %{reason: reason})
        disconnect_with_reason(state, {:logical_replication_convert_failed, reason})
    end
  end

  def handle_data(_other, %State{} = state), do: {:noreply, state}

  @impl true
  def handle_call({:ack_lsn, lsn}, from, %State{} = state) do
    case apply_ack_lsn(state, lsn, :ack_call) do
      {:ok, {acks, next_state}} ->
        Postgrex.ReplicationConnection.reply(from, :ok)
        {:noreply, acks, next_state}

      {:error, reason} ->
        Postgrex.ReplicationConnection.reply(from, {:error, reason})

        DuckFeeder.Telemetry.cdc_connection(:ack_lsn_error, %{
          reason: reason,
          slot_name: state.slot_name,
          publication_name: state.publication_name
        })

        {:noreply, state}
    end
  end

  @impl true
  def handle_info({:duck_feeder_ack_lsn, lsn}, %State{} = state) do
    case apply_ack_lsn(state, lsn, :ack_message) do
      {:ok, {acks, next_state}} ->
        {:noreply, acks, next_state}

      {:error, reason} ->
        DuckFeeder.Telemetry.cdc_connection(:ack_lsn_error, %{
          reason: reason,
          slot_name: state.slot_name,
          publication_name: state.publication_name
        })

        {:noreply, state}
    end
  end

  @impl true
  def handle_info(:status_tick, %State{} = state) do
    lag = lag_bytes(state)

    DuckFeeder.Telemetry.cdc_frame(:status_tick, :ack_sent, %{applied_lsn: state.applied_lsn})

    DuckFeeder.Telemetry.cdc_lag(
      %{
        lag_bytes: lag,
        received_lsn: state.received_lsn,
        applied_lsn: state.applied_lsn,
        max_lag_bytes: state.max_lag_bytes || -1
      },
      %{
        source: :status_tick,
        slot_name: state.slot_name,
        publication_name: state.publication_name
      }
    )

    state = maybe_track_backpressure(state, :status_tick)

    {:noreply, [standby_status_update(state)], schedule_status_tick(state)}
  end

  def handle_info(_info, %State{} = state), do: {:noreply, state}

  @impl true
  def handle_disconnect(%State{} = state) do
    now_ms = System.monotonic_time(:millisecond)
    reconnect_count = state.reconnect_count + 1

    DuckFeeder.Telemetry.cdc_connection(:disconnected, %{
      slot_name: state.slot_name,
      publication_name: state.publication_name,
      applied_lsn: state.applied_lsn,
      lag_bytes: lag_bytes(state),
      max_lag_bytes: state.max_lag_bytes,
      reconnect_count: reconnect_count,
      uptime_ms:
        if(is_integer(state.connected_at_monotonic_ms),
          do: max(now_ms - state.connected_at_monotonic_ms, 0),
          else: nil
        )
    })

    {:noreply,
     %{
       state
       | reconnect_count: reconnect_count,
         last_disconnect_monotonic_ms: now_ms,
         connected_at_monotonic_ms: nil,
         backpressure_active: false,
         step: :disconnected
     }}
  end

  defp maybe_ack_event(state, _event), do: {[], state}

  defp maybe_advance_flush_from_keepalive(state, _wal_end), do: state

  defp bump_received_lsn(%State{} = state, wal_end) do
    %{state | received_lsn: max(state.received_lsn, wal_end)}
  end

  defp apply_ack_lsn(%State{} = state, lsn, source) when is_atom(source) do
    with {:ok, ack_lsn} <- normalize_ack_lsn(lsn) do
      next_applied_lsn =
        ack_lsn
        |> min(state.received_lsn)
        |> max(state.applied_lsn)

      next_state =
        %{
          state
          | flushed_lsn: max(state.flushed_lsn, next_applied_lsn),
            applied_lsn: next_applied_lsn
        }
        |> maybe_track_backpressure(source)

      acks =
        if next_state.applied_lsn > state.applied_lsn do
          [standby_status_update(next_state)]
        else
          []
        end

      {:ok, {acks, next_state}}
    end
  end

  defp normalize_ack_lsn(lsn) when is_integer(lsn) and lsn >= 0, do: {:ok, lsn}

  defp normalize_ack_lsn(lsn) when is_binary(lsn) do
    case Lsn.parse(lsn) do
      {:ok, parsed} -> {:ok, parsed}
      {:error, reason} -> {:error, {:invalid_ack_lsn, reason}}
    end
  end

  defp normalize_ack_lsn(lsn), do: {:error, {:invalid_ack_lsn, lsn}}

  defp maybe_enforce_lag(%State{max_lag_bytes: nil}), do: :ok

  defp maybe_enforce_lag(%State{max_lag_bytes: max_lag_bytes} = state)
       when is_integer(max_lag_bytes) and max_lag_bytes >= 0 do
    lag = lag_bytes(state)

    if lag > max_lag_bytes do
      reason = {:max_lag_exceeded, lag, max_lag_bytes}

      DuckFeeder.Telemetry.cdc_connection(:lag_exceeded, %{
        lag_bytes: lag,
        max_lag_bytes: max_lag_bytes,
        slot_name: state.slot_name
      })

      DuckFeeder.Telemetry.cdc_lag(
        %{
          lag_bytes: lag,
          received_lsn: state.received_lsn,
          applied_lsn: state.applied_lsn,
          max_lag_bytes: max_lag_bytes
        },
        %{
          source: :lag_guard,
          status: :exceeded,
          slot_name: state.slot_name,
          publication_name: state.publication_name
        }
      )

      {:error, reason}
    else
      :ok
    end
  end

  defp maybe_track_backpressure(%State{backpressure_lag_bytes: nil} = state, _source), do: state

  defp maybe_track_backpressure(
         %State{backpressure_lag_bytes: threshold, backpressure_active: active?} = state,
         source
       )
       when is_integer(threshold) and threshold >= 0 and is_atom(source) do
    lag = lag_bytes(state)

    cond do
      lag >= threshold and not active? ->
        DuckFeeder.Telemetry.cdc_backpressure(
          %{lag_bytes: lag, threshold_bytes: threshold},
          %{
            status: :entered,
            source: source,
            slot_name: state.slot_name,
            publication_name: state.publication_name
          }
        )

        %{state | backpressure_active: true}

      lag < threshold and active? ->
        DuckFeeder.Telemetry.cdc_backpressure(
          %{lag_bytes: lag, threshold_bytes: threshold},
          %{
            status: :cleared,
            source: source,
            slot_name: state.slot_name,
            publication_name: state.publication_name
          }
        )

        %{state | backpressure_active: false}

      true ->
        state
    end
  end

  defp disconnect_with_reason(%State{} = state, reason) do
    DuckFeeder.Telemetry.cdc_connection(:disconnecting, %{
      reason: reason,
      slot_name: state.slot_name,
      publication_name: state.publication_name,
      lag_bytes: lag_bytes(state),
      max_lag_bytes: state.max_lag_bytes,
      reconnect_count: state.reconnect_count
    })

    {:disconnect, reason}
  end

  defp lag_bytes(%State{} = state) do
    max(state.received_lsn - state.applied_lsn, 0)
  end

  defp standby_status_update(%State{} = state) do
    ReplicationProtocol.encode_standby_status_update(
      state.received_lsn + 1,
      state.flushed_lsn + 1,
      state.applied_lsn + 1,
      false
    )
  end

  defp schedule_status_tick(
         %State{status_interval_ms: interval, status_timer_ref: timer_ref} = state
       )
       when interval > 0 do
    if is_reference(timer_ref), do: Process.cancel_timer(timer_ref)
    %{state | status_timer_ref: Process.send_after(self(), :status_tick, interval)}
  end

  defp schedule_status_tick(%State{} = state), do: %{state | status_timer_ref: nil}

  defp dispatch_event(pid, event) when is_pid(pid) do
    send(pid, {:duck_feeder_cdc_event, event})
    :ok
  end

  defp dispatch_event(fun, event) when is_function(fun, 1) do
    case fun.(event) do
      :ok -> :ok
      {:error, _reason} = error -> error
      _ -> :ok
    end
  rescue
    exception -> {:error, {:exception, exception}}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp dispatch_event({module, function, args}, event)
       when is_atom(module) and is_atom(function) and is_list(args) do
    dispatch_event(fn msg -> apply(module, function, [msg | args]) end, event)
  end

  defp dispatch_event(other, _event), do: {:error, {:invalid_event_sink, other}}

  defp maybe_put(opts, _key, nil), do: opts
  defp maybe_put(opts, key, value), do: Keyword.put(opts, key, value)
end
