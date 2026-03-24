defmodule DuckFeeder.Runtime.StreamWorker do
  @moduledoc """
  Managed runtime worker that starts and monitors a stream pair
  (`service_pid` + `cdc_pid`).

  If either child exits unexpectedly, this worker exits and can be restarted by
  an external supervisor.
  """

  use GenServer

  alias DuckFeeder.Runtime

  defmodule State do
    @enforce_keys [:service_pid, :cdc_pid, :start_lsn, :source]
    defstruct [:service_pid, :cdc_pid, :start_lsn, :source, monitors: %{}]
  end

  @type option ::
          {:name, GenServer.name()}
          | {:meta_conn, term()}
          | {:source_name, String.t()}
          | {:storage_config, map() | nil}
          | {:runtime_opts, keyword()}
          | {:runtime_module, module()}

  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts) do
    {name_opt, opts} = Keyword.pop(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name_opt)
  end

  @spec stream_info(GenServer.server()) :: {:ok, map()}
  def stream_info(server), do: GenServer.call(server, :stream_info)

  @impl true
  def init(opts) do
    runtime_module = Keyword.get(opts, :runtime_module, Runtime)
    meta_conn = Keyword.fetch!(opts, :meta_conn)
    source_name = Keyword.fetch!(opts, :source_name)
    storage_config = Keyword.fetch!(opts, :storage_config)
    runtime_opts = Keyword.get(opts, :runtime_opts, [])

    case runtime_module.start_stream(meta_conn, source_name, storage_config, runtime_opts) do
      {:ok, %{service_pid: service_pid, cdc_pid: cdc_pid, start_lsn: start_lsn, source: source}} ->
        monitors = %{
          Process.monitor(service_pid) => service_pid,
          Process.monitor(cdc_pid) => cdc_pid
        }

        {:ok,
         %State{
           service_pid: service_pid,
           cdc_pid: cdc_pid,
           start_lsn: start_lsn,
           source: source,
           monitors: monitors
         }}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_call(:stream_info, _from, %State{} = state) do
    {:reply,
     {:ok,
      %{
        service_pid: state.service_pid,
        cdc_pid: state.cdc_pid,
        start_lsn: state.start_lsn,
        source: state.source
      }}, state}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, pid, reason}, %State{monitors: monitors} = state) do
    if Map.has_key?(monitors, ref) do
      {:stop, {:stream_child_down, pid, reason}, state}
    else
      {:noreply, state}
    end
  end

  @impl true
  def terminate(_reason, %State{} = state) do
    stop_if_alive(state.cdc_pid)
    stop_if_alive(state.service_pid)
    :ok
  end

  defp stop_if_alive(pid) when is_pid(pid) do
    if Process.alive?(pid), do: Process.exit(pid, :shutdown)
  end
end
