defmodule DuckFeeder.BatchDispatchTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.BatchDispatch

  setup do
    %{task_supervisor: start_supervised!({Task.Supervisor, name: nil})}
  end

  test "handle_batch_result starts the next queued batch after a task completes", %{
    task_supervisor: task_supervisor
  } do
    owner = self()
    completed_ref = make_ref()
    completed_table = {"raw", "events"}
    queued_table = {"raw", "logs"}
    completed_batch = %{id: 1}
    queued_batch = %{id: 2}

    state = %{
      context: %{
        batch_processor: fn _context, table, batch ->
          send(owner, {:batch_processed, table, batch})
          {:ok, :done}
        end
      },
      batch_task_supervisor: task_supervisor,
      max_inflight_batches: 1,
      max_pending_batches: 1,
      inflight_batch_tasks: %{completed_ref => %{table: completed_table, batch: completed_batch}},
      pending_batches: :queue.from_list([{queued_table, queued_batch}]),
      pending_batch_count: 1,
      events: [],
      results: [],
      completed: []
    }

    assert {:noreply, state} =
             BatchDispatch.handle_batch_result(state, completed_ref, {:ok, :done},
               on_event: &track_event/3,
               on_result: &track_result/4,
               on_completed: &track_completed/4
             )

    assert state.pending_batch_count == 0
    assert :queue.to_list(state.pending_batches) == []
    assert map_size(state.inflight_batch_tasks) == 1
    refute Map.has_key?(state.inflight_batch_tasks, completed_ref)

    assert Enum.reverse(state.results) == [{completed_table, {:ok, :done}, completed_batch}]
    assert Enum.reverse(state.completed) == [{completed_table, {:ok, :done}, completed_batch}]

    assert Enum.any?(
             state.events,
             &match?({:started, %{source: :queued, table: ^queued_table}}, &1)
           )

    assert_receive {:batch_processed, ^queued_table, ^queued_batch}, 1_000
  end

  test "handle_batch_down stops on abnormal task exits", %{task_supervisor: task_supervisor} do
    ref = make_ref()
    table = {"raw", "events"}
    batch = %{id: 1}

    state = %{
      context: %{batch_processor: fn _context, _table, _batch -> :ok end},
      batch_task_supervisor: task_supervisor,
      max_inflight_batches: 1,
      max_pending_batches: 1,
      inflight_batch_tasks: %{ref => %{table: table, batch: batch}},
      pending_batches: :queue.new(),
      pending_batch_count: 0,
      crashes: []
    }

    assert {:stop, {:batch_task_crashed, :boom}, state} =
             BatchDispatch.handle_batch_down(state, ref, :boom, on_task_crashed: &track_crash/5)

    refute Map.has_key?(state.inflight_batch_tasks, ref)
    assert state.crashes == [{table, batch, :boom, {:batch_task_crashed, :boom}}]
  end

  defp track_event(state, event, metadata) do
    Map.update!(state, :events, &[{event, metadata} | &1])
  end

  defp track_result(state, table, result, batch) do
    Map.update!(state, :results, &[{table, result, batch} | &1])
  end

  defp track_completed(state, table, result, batch) do
    Map.update!(state, :completed, &[{table, result, batch} | &1])
  end

  defp track_crash(state, table, batch, reason, error) do
    Map.update!(state, :crashes, &[{table, batch, reason, error} | &1])
  end
end
