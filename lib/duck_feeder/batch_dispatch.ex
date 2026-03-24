defmodule DuckFeeder.BatchDispatch do
  @moduledoc false

  alias DuckFeeder.BatchQueue

  @type table :: {String.t(), String.t()}
  @type batch :: map()
  @type state :: map()

  @type event_callback :: (state(), atom(), map() -> state())
  @type dropped_callback :: (state(), table(), batch(), atom() -> state())
  @type overflow_callback :: (state(), table(), batch(), term() -> state())
  @type result_callback :: (state(), table(), term(), batch() -> state())
  @type completed_callback :: (state(), table(), term(), batch() -> state())
  @type task_crashed_callback :: (state(), table(), batch(), term(), term() -> state())

  @spec handle_incoming_batch(state(), table(), batch(), keyword()) ::
          {:noreply, state()} | {:stop, term(), state()}
  def handle_incoming_batch(state, table, batch, opts) when is_map(state) and is_list(opts) do
    on_event = Keyword.get(opts, :on_event, &passthrough_event/3)
    on_dropped = Keyword.get(opts, :on_dropped, &passthrough_dropped/4)
    on_overflow = Keyword.get(opts, :on_overflow, &passthrough_overflow/4)

    case BatchQueue.enqueue_or_start_batch(
           state,
           table,
           batch,
           on_event: on_event,
           on_dropped: on_dropped
         ) do
      {:ok, next_state} ->
        {:noreply, next_state}

      {:error, reason, next_state} ->
        overflow_state = on_overflow.(next_state, table, batch, reason)
        {:stop, reason, overflow_state}
    end
  end

  @spec handle_batch_result(state(), reference(), term(), keyword()) :: {:noreply, state()}
  def handle_batch_result(state, ref, result, opts) when is_map(state) and is_reference(ref) do
    on_event = Keyword.get(opts, :on_event, &passthrough_event/3)
    on_result = Keyword.get(opts, :on_result, &passthrough_result/4)
    on_completed = Keyword.get(opts, :on_completed, &passthrough_result/4)

    case Map.pop(state.inflight_batch_tasks, ref) do
      {nil, _inflight} ->
        {:noreply, state}

      {%{table: table, batch: batch}, next_inflight} ->
        Process.demonitor(ref, [:flush])

        next_state =
          state
          |> Map.put(:inflight_batch_tasks, next_inflight)
          |> on_result.(table, result, batch)
          |> BatchQueue.maybe_start_queued_batches(on_event: on_event)
          |> on_completed.(table, result, batch)

        {:noreply, next_state}
    end
  end

  @spec handle_batch_down(state(), reference(), term(), keyword()) ::
          {:noreply, state()} | {:stop, term(), state()}
  def handle_batch_down(state, ref, reason, opts) when is_map(state) and is_reference(ref) do
    on_task_crashed = Keyword.get(opts, :on_task_crashed, &passthrough_task_crashed/5)

    case Map.get(state.inflight_batch_tasks, ref) do
      nil ->
        {:noreply, state}

      %{table: table, batch: batch} ->
        if BatchQueue.normal_down_reason?(reason) do
          {:noreply, state}
        else
          {_task, next_inflight} = Map.pop(state.inflight_batch_tasks, ref)
          error = {:batch_task_crashed, reason}

          next_state =
            %{state | inflight_batch_tasks: next_inflight}
            |> on_task_crashed.(table, batch, reason, error)

          {:stop, error, next_state}
        end
    end
  end

  defp passthrough_event(state, _status, _metadata), do: state
  defp passthrough_dropped(state, _table, _batch, _reason), do: state
  defp passthrough_overflow(state, _table, _batch, _reason), do: state
  defp passthrough_result(state, _table, _result, _batch), do: state
  defp passthrough_task_crashed(state, _table, _batch, _reason, _error), do: state
end
