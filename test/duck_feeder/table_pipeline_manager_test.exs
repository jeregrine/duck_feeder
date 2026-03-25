defmodule DuckFeeder.TablePipelineManagerTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.{TablePipeline, TablePipelineManager}

  setup do
    %{pipeline_supervisor: start_supervised!({DynamicSupervisor, strategy: :one_for_one})}
  end

  test "ensure_started starts and reuses a table pipeline", %{
    pipeline_supervisor: pipeline_supervisor
  } do
    table = {"raw", "events"}

    assert {:ok, pid, pipelines} =
             TablePipelineManager.ensure_started(%{}, pipeline_supervisor, table, self(), %{
               max_rows: 25,
               max_bytes: 5_000,
               flush_interval_ms: 12_345
             })

    assert pipelines == %{table => pid}
    assert is_pid(pid)
    assert TablePipeline.stats(pid).flush_interval_ms == 12_345

    assert {:ok, ^pid, ^pipelines} =
             TablePipelineManager.ensure_started(
               pipelines,
               pipeline_supervisor,
               table,
               self(),
               %{flush_interval_ms: 999}
             )
  end

  test "ensure_started replaces dead cached pipeline pids", %{
    pipeline_supervisor: pipeline_supervisor
  } do
    table = {"raw", "events"}

    assert {:ok, pid, pipelines} =
             TablePipelineManager.ensure_started(%{}, pipeline_supervisor, table, self(), %{})

    ref = Process.monitor(pid)
    GenServer.stop(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 1_000

    assert {:ok, next_pid, next_pipelines} =
             TablePipelineManager.ensure_started(
               pipelines,
               pipeline_supervisor,
               table,
               self(),
               %{}
             )

    assert next_pid != pid
    assert next_pipelines == %{table => next_pid}
  end
end
