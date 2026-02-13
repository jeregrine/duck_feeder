defmodule DuckFeeder.Ingest do
  @moduledoc """
  Ingest orchestrator for routing committed transactions into per-table pipelines.
  """

  use GenServer

  alias DuckFeeder.CDC.Router
  alias DuckFeeder.TablePipeline

  defmodule State do
    @enforce_keys [:designated_tables, :mapping, :pipeline_supervisor, :pipeline_opts, :sink_pid]
    defstruct [
      :designated_tables,
      :mapping,
      :pipeline_supervisor,
      :pipeline_opts,
      :sink_pid,
      pipelines: %{}
    ]

    @type t :: %__MODULE__{
            designated_tables: [map()],
            mapping: map(),
            pipeline_supervisor: pid(),
            pipeline_opts: map(),
            sink_pid: pid(),
            pipelines: %{optional({String.t(), String.t()}) => pid()}
          }
  end

  @type option ::
          {:name, GenServer.name()}
          | {:designated_tables, [map()]}
          | {:sink_pid, pid()}
          | {:pipeline_opts, map()}

  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts) do
    {name_opt, opts} = Keyword.pop(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name_opt)
  end

  @spec ingest_transaction(GenServer.server(), map()) :: :ok
  def ingest_transaction(server, transaction) when is_map(transaction) do
    GenServer.cast(server, {:ingest_transaction, transaction})
  end

  @spec flush_table(GenServer.server(), {String.t(), String.t()}) ::
          :empty | {:ok, map()} | {:error, term()}
  def flush_table(server, table) do
    GenServer.call(server, {:flush_table, table})
  end

  @spec table_pipeline(GenServer.server(), {String.t(), String.t()}) ::
          {:ok, pid()} | {:error, term()}
  def table_pipeline(server, table) do
    GenServer.call(server, {:table_pipeline, table})
  end

  @impl true
  def init(opts) do
    designated_tables = Keyword.get(opts, :designated_tables, [])
    mapping = Router.build_mapping(designated_tables)
    sink_pid = Keyword.get(opts, :sink_pid, self())
    pipeline_opts = Keyword.get(opts, :pipeline_opts, %{}) |> Map.new()

    {:ok, pipeline_supervisor} = DynamicSupervisor.start_link(strategy: :one_for_one)

    {:ok,
     %State{
       designated_tables: designated_tables,
       mapping: mapping,
       sink_pid: sink_pid,
       pipeline_opts: pipeline_opts,
       pipeline_supervisor: pipeline_supervisor
     }}
  end

  @impl true
  def handle_cast({:ingest_transaction, transaction}, state) do
    routed = Router.route_transaction(transaction, state.designated_tables)

    state =
      Enum.reduce(routed.routes, state, fn {table, changes}, acc_state ->
        {:ok, pipeline, next_state} = ensure_pipeline(acc_state, table)

        Enum.each(changes, fn change ->
          TablePipeline.append(pipeline, enrich_change(change, routed), routed.end_lsn)
        end)

        next_state
      end)

    {:noreply, state}
  end

  @impl true
  def handle_call({:flush_table, table}, _from, state) do
    case ensure_pipeline(state, table) do
      {:ok, pipeline, next_state} ->
        {:reply, TablePipeline.flush(pipeline), next_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:table_pipeline, table}, _from, state) do
    case ensure_pipeline(state, table) do
      {:ok, pipeline, next_state} -> {:reply, {:ok, pipeline}, next_state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  defp ensure_pipeline(%State{pipelines: pipelines} = state, table) do
    case Map.get(pipelines, table) do
      pid when is_pid(pid) ->
        if Process.alive?(pid) do
          {:ok, pid, state}
        else
          start_pipeline(state, table)
        end

      _ ->
        start_pipeline(state, table)
    end
  end

  defp start_pipeline(%State{} = state, table) do
    opts = pipeline_start_opts(state, table)

    case DynamicSupervisor.start_child(state.pipeline_supervisor, {TablePipeline, opts}) do
      {:ok, pid} ->
        {:ok, pid, %{state | pipelines: Map.put(state.pipelines, table, pid)}}

      {:error, {:already_started, pid}} ->
        {:ok, pid, %{state | pipelines: Map.put(state.pipelines, table, pid)}}

      {:error, reason} ->
        {:error, {:pipeline_start_failed, table, reason}}
    end
  end

  defp pipeline_start_opts(state, table) do
    [
      table: table,
      sink_pid: state.sink_pid,
      max_rows: Map.get(state.pipeline_opts, :max_rows, 10_000),
      max_bytes: Map.get(state.pipeline_opts, :max_bytes, 128 * 1_024 * 1_024),
      flush_interval_ms: Map.get(state.pipeline_opts, :flush_interval_ms, 5_000)
    ]
  end

  defp enrich_change(change, transaction) do
    change
    |> Map.put(:_commit_lsn, transaction.end_lsn)
    |> Map.put(:_xid, transaction.xid)
    |> Map.put(:_op, op_code(change[:op]))
    |> Map.put(:_ingest_ts, DateTime.utc_now())
  end

  defp op_code(:insert), do: "I"
  defp op_code(:update), do: "U"
  defp op_code(:delete), do: "D"
  defp op_code(:truncate), do: "T"
  defp op_code(other), do: to_string(other)
end
