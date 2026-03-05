defmodule DuckFeeder.BatchQueue do
  @moduledoc """
  Shared bounded queue + task launcher for batch processing.

  Used by both `DuckFeeder.Service` and `DuckFeeder.AppendStream`.
  """

  @type table :: {String.t(), String.t()}
  @type batch :: map()

  @type inflight_task :: %{table: table(), batch: batch()}

  @type state :: %{
          required(:context) => map(),
          required(:batch_task_supervisor) => pid(),
          required(:max_inflight_batches) => pos_integer(),
          required(:max_pending_batches) => pos_integer(),
          required(:inflight_batch_tasks) => %{optional(reference()) => inflight_task()},
          required(:pending_batches) => :queue.queue({table(), batch()}),
          required(:pending_batch_count) => non_neg_integer(),
          optional(:overflow_strategy) => :fail | :drop_oldest
        }

  @type event_callback :: (state(), atom(), map() -> state())
  @type dropped_callback :: (state(), table(), batch(), atom() -> state())

  @spec enqueue_or_start_batch(state(), table(), batch(), keyword()) ::
          {:ok, state()} | {:error, term(), state()}
  def enqueue_or_start_batch(state, table, batch, opts \\ []) when is_map(state) do
    event_callback = Keyword.get(opts, :on_event, &passthrough_event/3)
    dropped_callback = Keyword.get(opts, :on_dropped, &passthrough_dropped/4)

    if map_size(state.inflight_batch_tasks) < state.max_inflight_batches do
      {:ok, start_batch_task(state, table, batch, :incoming, event_callback)}
    else
      if state.pending_batch_count >= state.max_pending_batches do
        handle_queue_overflow(state, table, batch, event_callback, dropped_callback)
      else
        queued_state =
          state
          |> Map.put(:pending_batches, :queue.in({table, batch}, state.pending_batches))
          |> Map.put(:pending_batch_count, state.pending_batch_count + 1)

        {:ok, event_callback.(queued_state, :enqueued, %{table: table})}
      end
    end
  end

  @spec maybe_start_queued_batches(state(), keyword()) :: state()
  def maybe_start_queued_batches(state, opts \\ []) when is_map(state) do
    event_callback = Keyword.get(opts, :on_event, &passthrough_event/3)
    do_maybe_start_queued_batches(state, event_callback)
  end

  @spec normal_down_reason?(term()) :: boolean()
  def normal_down_reason?(:normal), do: true
  def normal_down_reason?(:shutdown), do: true
  def normal_down_reason?({:shutdown, _reason}), do: true
  def normal_down_reason?(_reason), do: false

  defp do_maybe_start_queued_batches(state, event_callback) do
    if map_size(state.inflight_batch_tasks) >= state.max_inflight_batches do
      state
    else
      cond do
        state.pending_batch_count == 0 ->
          state

        true ->
          case :queue.out(state.pending_batches) do
            {{:value, {table, batch}}, next_queue} ->
              state
              |> Map.put(:pending_batches, next_queue)
              |> Map.put(:pending_batch_count, state.pending_batch_count - 1)
              |> start_batch_task(table, batch, :queued, event_callback)
              |> do_maybe_start_queued_batches(event_callback)

            {:empty, _queue} ->
              Map.put(state, :pending_batch_count, 0)
          end
      end
    end
  end

  defp start_batch_task(state, table, batch, source, event_callback) when is_atom(source) do
    task =
      Task.Supervisor.async_nolink(state.batch_task_supervisor, fn ->
        DuckFeeder.BatchProcessor.process_batch(state.context, table, batch)
      end)

    next_state =
      Map.put(
        state,
        :inflight_batch_tasks,
        Map.put(state.inflight_batch_tasks, task.ref, %{table: table, batch: batch})
      )

    event_callback.(next_state, :started, %{table: table, source: source})
  end

  defp handle_queue_overflow(
         %{overflow_strategy: :drop_oldest} = state,
         table,
         batch,
         event_callback,
         dropped_callback
       ) do
    case :queue.out(state.pending_batches) do
      {{:value, {dropped_table, dropped_batch}}, next_queue} ->
        next_state =
          state
          |> Map.put(:pending_batches, :queue.in({table, batch}, next_queue))
          |> dropped_callback.(dropped_table, dropped_batch, :drop_oldest)
          |> event_callback.(:dropped_oldest, %{
            table: table,
            dropped_table: dropped_table,
            reason: :drop_oldest
          })
          |> event_callback.(:enqueued, %{table: table, source: :drop_oldest})

        {:ok, next_state}

      {:empty, _queue} ->
        {:error, {:batch_queue_overflow, state.max_pending_batches}, state}
    end
  end

  defp handle_queue_overflow(state, _table, _batch, _event_callback, _dropped_callback) do
    {:error, {:batch_queue_overflow, state.max_pending_batches}, state}
  end

  defp passthrough_event(state, _status, _metadata), do: state
  defp passthrough_dropped(state, _table, _batch, _reason), do: state
end
