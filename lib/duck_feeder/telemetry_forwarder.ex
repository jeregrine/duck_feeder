defmodule DuckFeeder.TelemetryForwarder do
  @moduledoc """
  Forwards selected Telemetry events into a DuckFeeder append stream.

  Default behavior is split-path:
  - non-`[:duck_feeder, ...]` events are appended as raw rows
  - `[:duck_feeder, ...]` events are summarized in a debounce window and appended
    as compact summary rows

  This avoids high-cardinality/self-recursive telemetry ingestion while still
  retaining operational signal.
  """

  use GenServer

  @duck_feeder_events [
    [:duck_feeder, :cdc, :event],
    [:duck_feeder, :cdc, :connection],
    [:duck_feeder, :cdc, :frame],
    [:duck_feeder, :cdc, :lag],
    [:duck_feeder, :cdc, :backpressure],
    [:duck_feeder, :batch, :flushed],
    [:duck_feeder, :batch, :processed],
    [:duck_feeder, :batch, :poison_row],
    [:duck_feeder, :service, :batch_queue],
    [:duck_feeder, :append_stream, :batch_queue],
    [:duck_feeder, :append_stream, :batch_dropped],
    [:duck_feeder, :service, :ack_checkpoint_lag]
  ]

  defmodule State do
    @enforce_keys [
      :stream,
      :table,
      :handler_id,
      :events,
      :append_fun,
      :observer_pid,
      :include_duck_feeder_events?,
      :summarize_duck_feeder?,
      :summary_debounce_ms,
      :summary_max_window_ms,
      :summary_suppress_own_events_ms
    ]

    defstruct [
      :stream,
      :table,
      :handler_id,
      :events,
      :append_fun,
      :observer_pid,
      :include_duck_feeder_events?,
      :summarize_duck_feeder?,
      :summary_debounce_ms,
      :summary_max_window_ms,
      :summary_suppress_own_events_ms,
      :summary_window_started_at_ms,
      :summary_window_started_at_iso,
      :summary_timer_ref,
      suppress_duck_feeder_until_ms: nil,
      summary_groups: %{}
    ]
  end

  @type option ::
          {:name, GenServer.name()}
          | {:stream, GenServer.server()}
          | {:table, String.t()}
          | {:events, [[atom()]]}
          | {:handler_id, String.t()}
          | {:observer_pid, pid()}
          | {:include_duck_feeder_events?, boolean()}
          | {:summarize_duck_feeder?, boolean()}
          | {:summary_debounce_ms, pos_integer()}
          | {:summary_max_window_ms, pos_integer()}
          | {:summary_suppress_own_events_ms, non_neg_integer()}
          | {:append_fun,
             (GenServer.server(), String.t(), map(), keyword() -> :ok | {:error, term()})}

  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts) do
    {name_opt, opts} = Keyword.pop(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name_opt)
  end

  @spec flush_summaries(GenServer.server()) :: :ok
  def flush_summaries(server), do: GenServer.call(server, :flush_summaries)

  @doc false
  def handle_telemetry(event, measurements, metadata, pid)
      when is_list(event) and is_map(measurements) and is_map(metadata) and is_pid(pid) do
    send(pid, {:duck_feeder_telemetry_forwarder_event, event, measurements, metadata})
    :ok
  end

  def handle_telemetry(_event, _measurements, _metadata, _pid), do: :ok

  @impl true
  def init(opts) do
    stream = Keyword.fetch!(opts, :stream)
    table = Keyword.get(opts, :table, "app_events")
    include_duck_feeder_events? = Keyword.get(opts, :include_duck_feeder_events?, true)
    summarize_duck_feeder? = Keyword.get(opts, :summarize_duck_feeder?, true)
    append_fun = Keyword.get(opts, :append_fun, &DuckFeeder.append_event/4)

    handler_id =
      Keyword.get_lazy(opts, :handler_id, fn ->
        "duck-feeder-telemetry-forwarder-#{System.unique_integer([:positive])}"
      end)

    events =
      opts
      |> Keyword.get(:events, [])
      |> normalize_events()
      |> maybe_include_duck_feeder_events(include_duck_feeder_events?)

    with {:ok, events} <- events,
         :ok <- ensure_events_non_empty(events),
         :ok <- ensure_append_fun(append_fun),
         {:ok, summary_debounce_ms} <-
           normalize_pos_integer(
             Keyword.get(opts, :summary_debounce_ms, 2_000),
             :summary_debounce_ms
           ),
         {:ok, summary_max_window_ms} <-
           normalize_pos_integer(
             Keyword.get(opts, :summary_max_window_ms, 30_000),
             :summary_max_window_ms
           ),
         {:ok, summary_suppress_own_events_ms} <-
           normalize_non_neg_integer(
             Keyword.get(opts, :summary_suppress_own_events_ms, 1_000),
             :summary_suppress_own_events_ms
           ),
         :ok <- attach_handler(handler_id, events) do
      {:ok,
       %State{
         stream: stream,
         table: table,
         handler_id: handler_id,
         events: events,
         append_fun: append_fun,
         observer_pid: Keyword.get(opts, :observer_pid),
         include_duck_feeder_events?: include_duck_feeder_events?,
         summarize_duck_feeder?: summarize_duck_feeder?,
         summary_debounce_ms: summary_debounce_ms,
         summary_max_window_ms: summary_max_window_ms,
         summary_suppress_own_events_ms: summary_suppress_own_events_ms
       }}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call(:flush_summaries, _from, %State{} = state) do
    {:reply, :ok, flush_summary_groups(state, :manual)}
  end

  @impl true
  def handle_info(
        {:duck_feeder_telemetry_forwarder_event, event, measurements, metadata},
        %State{} = state
      ) do
    if duck_feeder_event?(event) and state.summarize_duck_feeder? do
      now_ms = System.monotonic_time(:millisecond)

      if is_integer(state.suppress_duck_feeder_until_ms) and
           now_ms < state.suppress_duck_feeder_until_ms do
        {:noreply, state}
      else
        next_state =
          state
          |> add_summary_event(event, measurements, metadata)
          |> maybe_flush_for_max_window(now_ms)

        {:noreply, next_state}
      end
    else
      {:noreply, forward_raw_event(state, event, measurements, metadata)}
    end
  end

  def handle_info(:flush_summary_groups, %State{} = state) do
    {:noreply, flush_summary_groups(state, :debounce)}
  end

  def handle_info(_msg, %State{} = state), do: {:noreply, state}

  @impl true
  def terminate(_reason, %State{} = state) do
    if is_reference(state.summary_timer_ref), do: Process.cancel_timer(state.summary_timer_ref)
    :telemetry.detach(state.handler_id)
    :ok
  end

  defp attach_handler(handler_id, events) do
    case :telemetry.attach_many(handler_id, events, &__MODULE__.handle_telemetry/4, self()) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp maybe_include_duck_feeder_events({:ok, events}, true),
    do: {:ok, Enum.uniq(events ++ @duck_feeder_events)}

  defp maybe_include_duck_feeder_events({:ok, events}, false), do: {:ok, events}
  defp maybe_include_duck_feeder_events({:error, _reason} = error, _include?), do: error

  defp add_summary_event(%State{} = state, event, measurements, metadata) do
    event_name = event_name(event)

    group_meta = %{
      event: event_name,
      status: Map.get(metadata, :status),
      source: Map.get(metadata, :source),
      table_schema: Map.get(metadata, :table_schema, Map.get(metadata, :schema)),
      table_name: Map.get(metadata, :table_name, Map.get(metadata, :table))
    }

    key =
      {group_meta.event, group_meta.status, group_meta.source, group_meta.table_schema,
       group_meta.table_name}

    summary =
      Map.get(state.summary_groups, key, %{
        count: 0,
        metadata: group_meta,
        measurements: %{}
      })
      |> Map.update!(:count, &(&1 + 1))
      |> Map.update!(:measurements, &merge_measurements(&1, measurements))

    next_state =
      if is_integer(state.summary_window_started_at_ms) do
        %{state | summary_groups: Map.put(state.summary_groups, key, summary)}
      else
        %{
          state
          | summary_groups: Map.put(state.summary_groups, key, summary),
            summary_window_started_at_ms: System.monotonic_time(:millisecond),
            summary_window_started_at_iso: DateTime.utc_now() |> DateTime.to_iso8601()
        }
      end

    schedule_debounce_flush(next_state)
  end

  defp maybe_flush_for_max_window(
         %State{summary_window_started_at_ms: window_start, summary_max_window_ms: max_window_ms} =
           state,
         now_ms
       )
       when is_integer(window_start) do
    if now_ms - window_start >= max_window_ms do
      flush_summary_groups(state, :max_window)
    else
      state
    end
  end

  defp maybe_flush_for_max_window(%State{} = state, _now_ms), do: state

  defp schedule_debounce_flush(
         %State{summary_timer_ref: timer_ref, summary_debounce_ms: debounce_ms} = state
       ) do
    if is_reference(timer_ref), do: Process.cancel_timer(timer_ref)

    %{
      state
      | summary_timer_ref: Process.send_after(self(), :flush_summary_groups, debounce_ms)
    }
  end

  defp flush_summary_groups(%State{summary_groups: groups} = state, _reason)
       when map_size(groups) == 0 do
    clear_summary_window(state)
  end

  defp flush_summary_groups(%State{} = state, reason) do
    now_ms = System.monotonic_time(:millisecond)
    now_iso = DateTime.utc_now() |> DateTime.to_iso8601()
    started_iso = state.summary_window_started_at_iso || now_iso

    suppressed_until =
      if state.summary_suppress_own_events_ms > 0,
        do: now_ms + state.summary_suppress_own_events_ms,
        else: nil

    state = %{state | suppress_duck_feeder_until_ms: suppressed_until}

    {append_errors, _count} =
      state.summary_groups
      |> Map.values()
      |> Enum.map(&summary_row(&1, started_iso, now_iso, reason))
      |> Enum.reduce({[], 0}, fn row, {errors, count} ->
        case safe_append(state, row) do
          :ok -> {errors, count + 1}
          {:error, append_reason} -> {[append_reason | errors], count}
        end
      end)

    if append_errors != [] do
      notify_observer(
        state.observer_pid,
        {:duck_feeder_telemetry_forwarder_flush_errors, Enum.reverse(append_errors)}
      )
    end

    clear_summary_window(state)
  end

  defp clear_summary_window(%State{} = state) do
    if is_reference(state.summary_timer_ref), do: Process.cancel_timer(state.summary_timer_ref)

    %{
      state
      | summary_groups: %{},
        summary_window_started_at_ms: nil,
        summary_window_started_at_iso: nil,
        summary_timer_ref: nil
    }
  end

  defp forward_raw_event(%State{} = state, event, measurements, metadata) do
    row = %{
      "type" => "telemetry_event",
      "event" => event_name(event),
      "measurements" => normalize_term(measurements),
      "metadata" => normalize_term(metadata),
      "at" => DateTime.utc_now() |> DateTime.to_iso8601()
    }

    case safe_append(state, row) do
      :ok ->
        state

      {:error, reason} ->
        notify_observer(
          state.observer_pid,
          {:duck_feeder_telemetry_forwarder_append_error, reason, row}
        )

        state
    end
  end

  defp safe_append(%State{} = state, row) do
    try do
      case state.append_fun.(state.stream, state.table, row, []) do
        :ok -> :ok
        {:error, reason} -> {:error, reason}
        other -> {:error, {:unexpected_append_result, other}}
      end
    rescue
      error -> {:error, {:append_exception, error}}
    catch
      kind, reason -> {:error, {:append_throw, kind, reason}}
    end
  end

  defp summary_row(summary, started_iso, ended_iso, reason) do
    %{
      "type" => "duck_feeder_summary",
      "event" => summary.metadata.event,
      "status" => normalize_term(summary.metadata.status),
      "source" => normalize_term(summary.metadata.source),
      "table_schema" => normalize_term(summary.metadata.table_schema),
      "table_name" => normalize_term(summary.metadata.table_name),
      "count" => summary.count,
      "window_started_at" => started_iso,
      "window_ended_at" => ended_iso,
      "flush_reason" => Atom.to_string(reason),
      "measurements" => finalize_measurements(summary.measurements)
    }
  end

  defp merge_measurements(acc, incoming) when is_map(incoming) do
    Enum.reduce(incoming, acc, fn {key, value}, map_acc ->
      if is_number(value) do
        string_key = to_string(key)

        Map.update(
          map_acc,
          string_key,
          %{min: value, max: value, sum: value, last: value, count: 1},
          fn stats ->
            %{
              min: min(stats.min, value),
              max: max(stats.max, value),
              sum: stats.sum + value,
              last: value,
              count: stats.count + 1
            }
          end
        )
      else
        map_acc
      end
    end)
  end

  defp finalize_measurements(measurements) when is_map(measurements) do
    Enum.into(measurements, %{}, fn {key, value} ->
      average = if value.count > 0, do: value.sum / value.count, else: value.sum

      {key,
       %{
         "min" => value.min,
         "max" => value.max,
         "last" => value.last,
         "avg" => average,
         "count" => value.count
       }}
    end)
  end

  defp event_name(event) when is_list(event), do: Enum.map_join(event, ".", &to_string/1)

  defp duck_feeder_event?([:duck_feeder | _]), do: true
  defp duck_feeder_event?(_), do: false

  defp normalize_events(events) when is_list(events) do
    events
    |> Enum.reduce_while({:ok, []}, fn event, {:ok, acc} ->
      if valid_telemetry_event?(event) do
        {:cont, {:ok, [event | acc]}}
      else
        {:halt, {:error, {:invalid_telemetry_event, event}}}
      end
    end)
    |> case do
      {:ok, normalized} -> {:ok, Enum.reverse(normalized)}
      {:error, _reason} = error -> error
    end
  end

  defp normalize_events(_other), do: {:error, {:invalid_option, :events}}

  defp ensure_events_non_empty([]), do: {:error, {:invalid_option, :events, :empty}}
  defp ensure_events_non_empty(_events), do: :ok

  defp valid_telemetry_event?(event) when is_list(event) and event != [] do
    Enum.all?(event, &is_atom/1)
  end

  defp valid_telemetry_event?(_event), do: false

  defp ensure_append_fun(fun) when is_function(fun, 4), do: :ok
  defp ensure_append_fun(_other), do: {:error, {:invalid_option, :append_fun}}

  defp normalize_pos_integer(value, _key) when is_integer(value) and value > 0, do: {:ok, value}
  defp normalize_pos_integer(value, key), do: {:error, {:invalid_option, key, value}}

  defp normalize_non_neg_integer(value, _key) when is_integer(value) and value >= 0,
    do: {:ok, value}

  defp normalize_non_neg_integer(value, key), do: {:error, {:invalid_option, key, value}}

  defp notify_observer(pid, message) when is_pid(pid), do: send(pid, message)
  defp notify_observer(_pid, _message), do: :ok

  defp normalize_term(value), do: normalize_term(value, 4)

  defp normalize_term(_value, depth) when depth <= 0, do: "..."
  defp normalize_term(value, _depth) when is_nil(value), do: nil
  defp normalize_term(value, _depth) when is_boolean(value), do: value
  defp normalize_term(value, _depth) when is_number(value), do: value
  defp normalize_term(value, _depth) when is_binary(value), do: value
  defp normalize_term(value, _depth) when is_atom(value), do: Atom.to_string(value)

  defp normalize_term(%DateTime{} = value, _depth), do: DateTime.to_iso8601(value)
  defp normalize_term(%NaiveDateTime{} = value, _depth), do: NaiveDateTime.to_iso8601(value)
  defp normalize_term(%Date{} = value, _depth), do: Date.to_iso8601(value)
  defp normalize_term(%Time{} = value, _depth), do: Time.to_iso8601(value)

  defp normalize_term(value, depth) when is_list(value) do
    value
    |> Enum.take(100)
    |> Enum.map(&normalize_term(&1, depth - 1))
  end

  defp normalize_term(value, depth) when is_tuple(value) do
    value
    |> Tuple.to_list()
    |> normalize_term(depth - 1)
  end

  defp normalize_term(value, depth) when is_map(value) do
    value
    |> Enum.take(100)
    |> Enum.into(%{}, fn {key, nested_value} ->
      {to_string(key), normalize_term(nested_value, depth - 1)}
    end)
  end

  defp normalize_term(value, _depth), do: inspect(value)
end
