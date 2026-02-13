defmodule DuckFeeder.Reconciler.WorkerTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.Reconciler.Worker

  defmodule FakeReconciler do
    def reconcile(context, opts) do
      if pid = context[:test_pid], do: send(pid, {:fake_reconcile, context, opts})

      {:ok,
       %{
         checked: 2,
         committed: ["b1"],
         skipped: ["b2"],
         errors: []
       }}
    end
  end

  defmodule FakeReconcilerError do
    def reconcile(_context, _opts), do: {:error, :boom}
  end

  test "run_once executes reconcile and stores last result" do
    {:ok, worker} =
      Worker.start_link(
        context: %{meta_conn: :fake, test_pid: self()},
        reconciler_module: FakeReconciler,
        reconcile_opts: [states: [:uploaded]],
        interval_ms: 60_000,
        run_on_start?: false,
        observer_pid: self()
      )

    assert {:ok, %{checked: 2}} = Worker.run_once(worker)
    assert_receive {:fake_reconcile, %{meta_conn: :fake}, [states: [:uploaded]]}
    assert_receive {:duck_feeder_reconcile, {:ok, %{checked: 2}}}

    assert {:ok, %{checked: 2}} = Worker.last_result(worker)

    GenServer.stop(worker)
  end

  test "runs on schedule and reports errors" do
    {:ok, worker} =
      Worker.start_link(
        context: %{meta_conn: :fake},
        reconciler_module: FakeReconcilerError,
        interval_ms: 10,
        run_on_start?: true,
        observer_pid: self()
      )

    assert_receive {:duck_feeder_reconcile, {:error, :boom}}, 200

    assert {:error, :boom} = Worker.last_result(worker)

    GenServer.stop(worker)
  end
end
