defmodule DuckFeeder.AppendStream do
  @moduledoc """
  Generic append-event stream pipeline.

  Reuses DuckFeeder batching/writer/upload/commit flow for non-CDC producers
  (e.g. telemetry, logs, error streams) by appending rows directly to target
  DuckLake tables.
  """

  use GenServer

  alias DuckFeeder.{BatchProcessor, TablePipeline}
  alias DuckFeeder.CDC.Lsn

  defmodule State do
    @enforce_keys [
      :pipeline_supervisor,
      :pipeline_opts,
      :context,
      :designated_table_by_target,
      :default_target_schema,
      :observer_pid,
      :lsn_counter
    ]
    defstruct [
      :pipeline_supervisor,
      :pipeline_opts,
      :context,
      :designated_table_by_target,
      :default_target_schema,
      :observer_pid,
      :lsn_counter,
      pipelines: %{}
    ]
  end

  @type option ::
          {:name, GenServer.name()}
          | {:designated_tables, [map()]}
          | {:meta_conn, term()}
          | {:storage, map()}
          | {:writer, map()}
          | {:meta_module, module()}
          | {:object_prefix, String.t()}
          | {:pipeline_opts, map()}
          | {:observer_pid, pid()}
          | {:committer_module, module()}
          | {:committer_opts, keyword()}
          | {:default_target_schema, String.t()}
          | {:start_lsn, String.t()}

  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts) do
    {name_opt, opts} = Keyword.pop(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name_opt)
  end

  @spec append(GenServer.server(), String.t() | {String.t(), String.t()}, map(), keyword()) ::
          :ok | {:error, term()}
  def append(server, table, row, opts \\ []) when is_map(row) and is_list(opts) do
    GenServer.call(server, {:append, table, row, opts})
  end

  @spec flush_table(GenServer.server(), String.t() | {String.t(), String.t()}) ::
          :empty | {:ok, map()} | {:error, term()}
  def flush_table(server, table) do
    GenServer.call(server, {:flush_table, table})
  end

  @impl true
  def init(opts) do
    designated_tables = Keyword.fetch!(opts, :designated_tables)
    {:ok, pipeline_supervisor} = DynamicSupervisor.start_link(strategy: :one_for_one)

    start_lsn = Keyword.get(opts, :start_lsn, "0/0")

    state =
      %State{
        pipeline_supervisor: pipeline_supervisor,
        pipeline_opts: Keyword.get(opts, :pipeline_opts, %{}) |> Map.new(),
        designated_table_by_target: designated_table_mapping(designated_tables),
        default_target_schema: Keyword.get(opts, :default_target_schema, "raw"),
        observer_pid: Keyword.get(opts, :observer_pid, self()),
        lsn_counter: Lsn.parse!(start_lsn),
        context:
          %{
            meta_conn: Keyword.fetch!(opts, :meta_conn),
            designated_table_by_target: designated_table_mapping(designated_tables),
            writer: Keyword.get(opts, :writer, %{}),
            storage: Keyword.fetch!(opts, :storage),
            object_prefix: Keyword.get(opts, :object_prefix, "duck_feeder_append")
          }
          |> maybe_put_optional(:meta_module, Keyword.get(opts, :meta_module))
          |> maybe_put_optional(:committer_module, Keyword.get(opts, :committer_module))
          |> maybe_put_optional(:committer_opts, Keyword.get(opts, :committer_opts))
      }

    {:ok, state}
  end

  @impl true
  def handle_call({:append, table_ref, row, opts}, _from, %State{} = state) do
    with {:ok, table} <- normalize_table(table_ref, state.default_target_schema),
         :ok <- ensure_known_table(state.designated_table_by_target, table),
         {:ok, pipeline, next_state} <- ensure_pipeline(state, table),
         {:ok, lsn, next_state} <- resolve_lsn(next_state, opts) do
      :ok = TablePipeline.append(pipeline, row, lsn)
      {:reply, :ok, next_state}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:flush_table, table_ref}, _from, %State{} = state) do
    with {:ok, table} <- normalize_table(table_ref, state.default_target_schema),
         {:ok, pipeline, next_state} <- ensure_pipeline(state, table) do
      {:reply, TablePipeline.flush(pipeline), next_state}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_info({:duck_feeder_batch, table, batch}, %State{} = state) do
    result = BatchProcessor.process_batch(state.context, table, batch)

    if is_pid(state.observer_pid) do
      send(state.observer_pid, {:duck_feeder_append_batch_processed, table, result, batch})
      send(state.observer_pid, {:duck_feeder_batch_processed, table, result, batch})
    end

    {:noreply, state}
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
    opts =
      [
        table: table,
        sink_pid: self(),
        max_rows: Map.get(state.pipeline_opts, :max_rows, 10_000),
        max_bytes: Map.get(state.pipeline_opts, :max_bytes, 128 * 1_024 * 1_024),
        flush_interval_ms: Map.get(state.pipeline_opts, :flush_interval_ms, 5_000)
      ]

    case DynamicSupervisor.start_child(state.pipeline_supervisor, {TablePipeline, opts}) do
      {:ok, pid} ->
        {:ok, pid, %{state | pipelines: Map.put(state.pipelines, table, pid)}}

      {:error, {:already_started, pid}} ->
        {:ok, pid, %{state | pipelines: Map.put(state.pipelines, table, pid)}}

      {:error, reason} ->
        {:error, {:pipeline_start_failed, table, reason}}
    end
  end

  defp resolve_lsn(state, opts) do
    case Keyword.get(opts, :lsn) do
      lsn when is_binary(lsn) ->
        lsn_int = Lsn.parse!(lsn)
        {:ok, lsn, %{state | lsn_counter: max(state.lsn_counter, lsn_int)}}

      nil ->
        next = state.lsn_counter + 1
        {:ok, Lsn.to_string(next), %{state | lsn_counter: next}}

      other ->
        {:error, {:invalid_lsn, other}}
    end
  end

  defp ensure_known_table(mapping, table) do
    if Map.has_key?(mapping, table) do
      :ok
    else
      {:error, {:unknown_target_table, table}}
    end
  end

  defp normalize_table({schema, table}, _default_schema)
       when is_binary(schema) and schema != "" and is_binary(table) and table != "" do
    {:ok, {schema, table}}
  end

  defp normalize_table(table, default_schema) when is_binary(table) and table != "" do
    {:ok, {default_schema, table}}
  end

  defp normalize_table(other, _default_schema), do: {:error, {:invalid_table, other}}

  defp designated_table_mapping(designated_tables) do
    Enum.reduce(designated_tables, %{}, fn designated_table, acc ->
      id = Map.fetch!(designated_table, :id)

      target =
        {Map.fetch!(designated_table, :target_schema),
         Map.fetch!(designated_table, :target_table)}

      Map.put(acc, target, id)
    end)
  end

  defp maybe_put_optional(context, _key, nil), do: context
  defp maybe_put_optional(context, key, value), do: Map.put(context, key, value)
end
