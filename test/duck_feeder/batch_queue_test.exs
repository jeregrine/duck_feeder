defmodule DuckFeeder.BatchQueueTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.BatchQueue

  setup do
    %{task_supervisor: start_supervised!({Task.Supervisor, name: nil})}
  end

  test "queues batches once inflight capacity is full and starts them when capacity frees", %{
    task_supervisor: task_supervisor
  } do
    state = new_state(task_supervisor)
    first_table = {"raw", "events"}
    second_table = {"raw", "logs"}
    first_batch = %{id: 1}
    second_batch = %{id: 2}

    assert {:ok, state} =
             BatchQueue.enqueue_or_start_batch(state, first_table, first_batch,
               on_event: &track_event/3
             )

    assert map_size(state.inflight_batch_tasks) == 1

    assert {:ok, state} =
             BatchQueue.enqueue_or_start_batch(state, second_table, second_batch,
               on_event: &track_event/3
             )

    assert state.pending_batch_count == 1
    assert :queue.to_list(state.pending_batches) == [{second_table, second_batch}]

    state =
      state
      |> Map.put(:inflight_batch_tasks, %{})
      |> BatchQueue.maybe_start_queued_batches(on_event: &track_event/3)

    assert state.pending_batch_count == 0
    assert :queue.to_list(state.pending_batches) == []
    assert map_size(state.inflight_batch_tasks) == 1

    assert Enum.reverse(state.events) == [
             {:started, %{source: :incoming, table: first_table}},
             {:enqueued, %{table: second_table}},
             {:started, %{source: :queued, table: second_table}}
           ]
  end

  test "drops the oldest pending batch when configured to do so", %{
    task_supervisor: task_supervisor
  } do
    old_table = {"raw", "events"}
    new_table = {"raw", "logs"}
    old_batch = %{id: 1}
    new_batch = %{id: 2}

    state =
      new_state(task_supervisor,
        inflight_batch_tasks: %{make_ref() => %{table: {"raw", "inflight"}, batch: %{id: 0}}},
        pending_batches: :queue.from_list([{old_table, old_batch}]),
        pending_batch_count: 1,
        overflow_strategy: :drop_oldest
      )

    assert {:ok, state} =
             BatchQueue.enqueue_or_start_batch(state, new_table, new_batch,
               on_event: &track_event/3,
               on_dropped: &track_drop/4
             )

    assert state.pending_batch_count == 1
    assert :queue.to_list(state.pending_batches) == [{new_table, new_batch}]
    assert state.drops == [{old_table, old_batch, :drop_oldest}]

    assert Enum.reverse(state.events) == [
             {:dropped_oldest,
              %{dropped_table: old_table, reason: :drop_oldest, table: new_table}},
             {:enqueued, %{source: :drop_oldest, table: new_table}}
           ]
  end

  defp new_state(task_supervisor, opts \\ []) do
    owner = self()

    %{
      context: %{
        batch_processor: fn _context, table, batch ->
          send(owner, {:batch_processed, table, batch})
          {:ok, :done}
        end
      },
      batch_task_supervisor: task_supervisor,
      max_inflight_batches: Keyword.get(opts, :max_inflight_batches, 1),
      max_pending_batches: Keyword.get(opts, :max_pending_batches, 1),
      inflight_batch_tasks: Keyword.get(opts, :inflight_batch_tasks, %{}),
      pending_batches: Keyword.get(opts, :pending_batches, :queue.new()),
      pending_batch_count: Keyword.get(opts, :pending_batch_count, 0),
      overflow_strategy: Keyword.get(opts, :overflow_strategy, :fail),
      events: [],
      drops: []
    }
  end

  defp track_event(state, event, metadata) do
    Map.update!(state, :events, &[{event, metadata} | &1])
  end

  defp track_drop(state, table, batch, reason) do
    Map.update!(state, :drops, &[{table, batch, reason} | &1])
  end
end
