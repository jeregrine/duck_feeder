defmodule DuckFeeder.Runtime.Manager do
  @moduledoc """
  Dynamic runtime manager for running multiple source streams in one application.

  Starts one `DuckFeeder.Runtime.Supervisor` per source and tracks their pids.
  """

  use GenServer

  alias DuckFeeder.Runtime.Supervisor, as: RuntimeSupervisor

  defmodule State do
    @enforce_keys [:meta_conn, :storage_config, :runtime_supervisor_module, :base_opts]
    defstruct [
      :meta_conn,
      :storage_config,
      :runtime_supervisor_module,
      :base_opts,
      sources: %{},
      monitors: %{}
    ]
  end

  @type option ::
          {:name, GenServer.name()}
          | {:meta_conn, term()}
          | {:storage_config, map()}
          | {:runtime_supervisor_module, module()}
          | {:base_opts, keyword()}

  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts) do
    {name_opt, opts} = Keyword.pop(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name_opt)
  end

  @spec start_source(GenServer.server(), String.t(), keyword()) ::
          {:ok, pid()} | {:error, :already_started | term()}
  def start_source(server, source_name, opts \\ []) when is_binary(source_name) do
    GenServer.call(server, {:start_source, source_name, opts})
  end

  @spec stop_source(GenServer.server(), String.t()) :: :ok | {:error, :not_found}
  def stop_source(server, source_name) when is_binary(source_name) do
    GenServer.call(server, {:stop_source, source_name})
  end

  @spec list_sources(GenServer.server()) :: %{optional(String.t()) => pid()}
  def list_sources(server), do: GenServer.call(server, :list_sources)

  @impl true
  def init(opts) do
    {:ok,
     %State{
       meta_conn: Keyword.fetch!(opts, :meta_conn),
       storage_config: Keyword.fetch!(opts, :storage_config),
       runtime_supervisor_module:
         Keyword.get(opts, :runtime_supervisor_module, RuntimeSupervisor),
       base_opts: Keyword.get(opts, :base_opts, [])
     }}
  end

  @impl true
  def handle_call({:start_source, source_name, source_opts}, _from, %State{} = state) do
    case Map.get(state.sources, source_name) do
      pid when is_pid(pid) ->
        if Process.alive?(pid) do
          {:reply, {:error, :already_started}, state}
        else
          start_source_runtime(state, source_name, source_opts)
        end

      _ ->
        start_source_runtime(state, source_name, source_opts)
    end
  end

  def handle_call({:stop_source, source_name}, _from, %State{} = state) do
    case Map.get(state.sources, source_name) do
      pid when is_pid(pid) ->
        _ = GenServer.stop(pid)
        {:reply, :ok, drop_source(state, source_name)}

      _ ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call(:list_sources, _from, %State{} = state) do
    {:reply, state.sources, state}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, %State{} = state) do
    case Map.get(state.monitors, ref) do
      nil ->
        {:noreply, state}

      source_name ->
        next_state =
          state
          |> drop_source(source_name)
          |> then(fn s -> %{s | monitors: Map.delete(s.monitors, ref)} end)

        {:noreply, next_state}
    end
  end

  defp start_source_runtime(state, source_name, source_opts) do
    opts =
      state.base_opts
      |> Keyword.merge(source_opts)
      |> Keyword.merge(
        meta_conn: state.meta_conn,
        source_name: source_name,
        storage_config: state.storage_config
      )

    case state.runtime_supervisor_module.start_link(opts) do
      {:ok, pid} ->
        Process.unlink(pid)
        ref = Process.monitor(pid)

        next_state = %{
          state
          | sources: Map.put(state.sources, source_name, pid),
            monitors: Map.put(state.monitors, ref, source_name)
        }

        {:reply, {:ok, pid}, next_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  defp drop_source(%State{} = state, source_name) do
    source_pid = Map.get(state.sources, source_name)

    monitor_ref =
      Enum.find_value(state.monitors, fn {ref, name} ->
        if name == source_name, do: ref, else: nil
      end)

    if is_reference(monitor_ref), do: Process.demonitor(monitor_ref, [:flush])

    %{
      state
      | sources: Map.delete(state.sources, source_name),
        monitors:
          if(is_reference(monitor_ref),
            do: Map.delete(state.monitors, monitor_ref),
            else: state.monitors
          )
    }
    |> then(fn s ->
      # Keep behavior idempotent if pid already gone.
      if is_pid(source_pid) and Process.alive?(source_pid), do: s, else: s
    end)
  end
end
