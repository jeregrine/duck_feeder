defmodule DuckFeeder.CDC.Connection do
  @moduledoc """
  PostgreSQL logical replication client based on `Postgrex.ReplicationConnection`.

  Responsibilities:
  - start logical replication stream (`START_REPLICATION ...`)
  - decode pgoutput messages
  - convert wire messages into normalized `DuckFeeder.CDC.Event` structs
  - dispatch normalized events to a sink process/function
  - emit standby status updates (acks)
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
      :status_timer_ref,
      step: :disconnected,
      received_lsn: 0,
      flushed_lsn: 0,
      applied_lsn: 0
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
            status_timer_ref: reference() | nil,
            step: :disconnected | :streaming,
            received_lsn: non_neg_integer(),
            flushed_lsn: non_neg_integer(),
            applied_lsn: non_neg_integer()
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

    DuckFeeder.Telemetry.cdc_connection(:stream_starting, %{
      slot_name: state.slot_name,
      publication_name: state.publication_name,
      start_lsn: state.start_lsn
    })

    {:stream, query, [], state |> Map.put(:step, :streaming) |> schedule_status_tick()}
  end

  @impl true
  def handle_data(<<?k, wal_end::64, _clock::64-signed, reply::8>>, %State{} = state) do
    state = bump_received_lsn(state, wal_end)

    with :ok <- maybe_enforce_lag(state) do
      if reply == 1 do
        DuckFeeder.Telemetry.cdc_frame(:keepalive, :ack_requested, %{wal_end: wal_end})
        state = maybe_advance_flush_from_keepalive(state, wal_end)
        {:noreply, [standby_status_update(state)], state}
      else
        DuckFeeder.Telemetry.cdc_frame(:keepalive, :noop, %{wal_end: wal_end})
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

    {:noreply, [standby_status_update(state)], schedule_status_tick(state)}
  end

  def handle_info(_info, %State{} = state), do: {:noreply, state}

  @impl true
  def handle_disconnect(%State{} = state) do
    DuckFeeder.Telemetry.cdc_connection(:disconnected, %{
      slot_name: state.slot_name,
      publication_name: state.publication_name,
      applied_lsn: state.applied_lsn,
      lag_bytes: lag_bytes(state),
      max_lag_bytes: state.max_lag_bytes
    })

    {:noreply, state}
  end

  defp maybe_ack_event(state, %Event.Commit{end_lsn: end_lsn}) do
    lsn = Lsn.parse!(end_lsn)

    state = %{
      state
      | flushed_lsn: max(state.flushed_lsn, lsn),
        applied_lsn: max(state.applied_lsn, lsn)
    }

    {[standby_status_update(state)], state}
  end

  defp maybe_ack_event(state, _event), do: {[], state}

  defp maybe_advance_flush_from_keepalive(state, wal_end) do
    if in_transaction?(state) do
      state
    else
      %{
        state
        | flushed_lsn: max(state.flushed_lsn, wal_end),
          applied_lsn: max(state.applied_lsn, wal_end)
      }
    end
  end

  defp in_transaction?(%State{converter_state: converter_state}) do
    not is_nil(Map.get(converter_state, :current_xid))
  end

  defp bump_received_lsn(%State{} = state, wal_end) do
    %{state | received_lsn: max(state.received_lsn, wal_end)}
  end

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

  defp disconnect_with_reason(%State{} = state, reason) do
    DuckFeeder.Telemetry.cdc_connection(:disconnecting, %{
      reason: reason,
      slot_name: state.slot_name,
      publication_name: state.publication_name,
      lag_bytes: lag_bytes(state),
      max_lag_bytes: state.max_lag_bytes
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
