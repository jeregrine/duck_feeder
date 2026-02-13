defmodule DuckFeeder.Reconciler.Worker do
  @moduledoc """
  Periodic reconciler worker.

  Runs `DuckFeeder.Reconciler.reconcile/2` on a fixed interval and exposes
  manual `run_once/1` plus `last_result/1` helpers.
  """

  use GenServer

  alias DuckFeeder.Reconciler

  defmodule State do
    @enforce_keys [:context, :interval_ms, :reconciler_module, :reconcile_opts]
    defstruct [
      :context,
      :interval_ms,
      :reconciler_module,
      :reconcile_opts,
      :observer_pid,
      :timer_ref,
      :last_result,
      :last_run_at
    ]
  end

  @type option ::
          {:name, GenServer.name()}
          | {:context, map()}
          | {:interval_ms, pos_integer()}
          | {:reconciler_module, module()}
          | {:reconcile_opts, keyword()}
          | {:observer_pid, pid()}
          | {:run_on_start?, boolean()}

  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts) do
    {name_opt, opts} = Keyword.pop(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name_opt)
  end

  @spec run_once(GenServer.server()) :: {:ok, map()} | {:error, term()}
  def run_once(server), do: GenServer.call(server, :run_once)

  @spec last_result(GenServer.server()) :: {:ok | :error, map() | term()} | nil
  def last_result(server), do: GenServer.call(server, :last_result)

  @impl true
  def init(opts) do
    state = %State{
      context: Keyword.fetch!(opts, :context),
      interval_ms: Keyword.get(opts, :interval_ms, 60_000),
      reconciler_module: Keyword.get(opts, :reconciler_module, Reconciler),
      reconcile_opts: Keyword.get(opts, :reconcile_opts, []),
      observer_pid: Keyword.get(opts, :observer_pid)
    }

    state =
      if Keyword.get(opts, :run_on_start?, true) do
        schedule_run(state, 0)
      else
        schedule_run(state, state.interval_ms)
      end

    {:ok, state}
  end

  @impl true
  def handle_call(:run_once, _from, %State{} = state) do
    {result, state} = run_reconcile(state)
    {:reply, result, state}
  end

  def handle_call(:last_result, _from, %State{} = state) do
    {:reply, state.last_result, state}
  end

  @impl true
  def handle_info(:reconcile_tick, %State{} = state) do
    {_result, state} = run_reconcile(state)
    {:noreply, schedule_run(state, state.interval_ms)}
  end

  defp run_reconcile(%State{} = state) do
    result = state.reconciler_module.reconcile(state.context, state.reconcile_opts)

    DuckFeeder.Telemetry.reconciler_run(result)

    if is_pid(state.observer_pid) do
      send(state.observer_pid, {:duck_feeder_reconcile, result})
    end

    state = %{state | last_result: result, last_run_at: DateTime.utc_now()}

    {result, state}
  end

  defp schedule_run(%State{timer_ref: timer_ref} = state, interval_ms) do
    if is_reference(timer_ref), do: Process.cancel_timer(timer_ref)
    %{state | timer_ref: Process.send_after(self(), :reconcile_tick, interval_ms)}
  end
end
