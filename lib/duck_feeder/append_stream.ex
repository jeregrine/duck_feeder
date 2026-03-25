defmodule DuckFeeder.AppendStream do
  @moduledoc """
  Generic append-event stream pipeline.

  Reuses DuckFeeder batching and downstream sink flow for non-CDC producers
  (e.g. telemetry, logs, error streams) by appending rows directly to target
  DuckDB-managed tables.

  Flow:

      append/4
        |
        v
      TablePipeline per target table
        |
        v
      {:duck_feeder_batch, table, batch}
        |
        v
      async bounded queue/tasks
        |
        v
      Sink

  Overload policy:
  - `overflow_strategy: :fail` (default, fail-closed)
  - `overflow_strategy: :drop_oldest` (lossy mode for availability-first streams)
  """

  use GenServer

  alias DuckFeeder.{RuntimeSupport, TablePipeline, TablePipelineManager}
  alias DuckFeeder.CDC.Lsn

  defmodule State do
    @enforce_keys [
      :pipeline_supervisor,
      :pipeline_opts,
      :context,
      :default_target_schema,
      :observer_pid,
      :lsn_counter,
      :batch_task_supervisor,
      :max_inflight_batches,
      :max_pending_batches,
      :overflow_strategy
    ]
    defstruct [
      :pipeline_supervisor,
      :pipeline_opts,
      :context,
      :default_target_schema,
      :observer_pid,
      :lsn_counter,
      :batch_task_supervisor,
      :max_inflight_batches,
      :max_pending_batches,
      :overflow_strategy,
      pipelines: %{},
      inflight_batch_tasks: %{},
      pending_batches: :queue.new(),
      pending_batch_count: 0
    ]

    @type inflight_task :: %{table: {String.t(), String.t()}, batch: map()}

    @type t :: %__MODULE__{
            pipeline_supervisor: pid(),
            pipeline_opts: map(),
            context: map(),
            default_target_schema: String.t(),
            observer_pid: pid(),
            lsn_counter: non_neg_integer(),
            batch_task_supervisor: pid(),
            max_inflight_batches: pos_integer(),
            max_pending_batches: pos_integer(),
            overflow_strategy: :fail | :drop_oldest,
            pipelines: %{optional({String.t(), String.t()}) => pid()},
            inflight_batch_tasks: %{optional(reference()) => inflight_task()},
            pending_batches: :queue.queue({{String.t(), String.t()}, map()}),
            pending_batch_count: non_neg_integer()
          }
  end

  @type option ::
          {:name, GenServer.name()}
          | {:designated_tables, [map()]}
          | {:meta_conn, term()}
          | {:duckdb, map()}
          | {:meta_module, module()}
          | {:object_prefix, String.t()}
          | {:pipeline_opts, map()}
          | {:observer_pid, pid()}
          | {:default_target_schema, String.t()}
          | {:start_lsn, String.t()}
          | {:max_inflight_batches, pos_integer()}
          | {:max_pending_batches, pos_integer()}
          | {:overflow_strategy, :fail | :drop_oldest}
          | {:batch_processor, (map(), {String.t(), String.t()}, map() -> term())}

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
    start_lsn = Keyword.get(opts, :start_lsn, "0/0")

    with {:ok, common} <-
           RuntimeSupport.resolve_common_init(
             designated_tables,
             opts,
             checkpoint_prefix: Keyword.get(opts, :object_prefix, "duck_feeder_append")
           ),
         {:ok, lsn_counter} <- Lsn.parse(start_lsn),
         {:ok, overflow_strategy} <-
           normalize_overflow_strategy(Keyword.get(opts, :overflow_strategy, :fail)),
         {:ok, pipeline_supervisor} <- DynamicSupervisor.start_link(strategy: :one_for_one),
         {:ok, batch_task_supervisor} <- Task.Supervisor.start_link(strategy: :one_for_one) do
      {:ok,
       %State{
         pipeline_supervisor: pipeline_supervisor,
         pipeline_opts: Keyword.get(opts, :pipeline_opts, %{}) |> Map.new(),
         context: common.context,
         default_target_schema: Keyword.get(opts, :default_target_schema, "raw"),
         observer_pid: common.observer_pid,
         lsn_counter: lsn_counter,
         batch_task_supervisor: batch_task_supervisor,
         max_inflight_batches: common.max_inflight_batches,
         max_pending_batches: common.max_pending_batches,
         overflow_strategy: overflow_strategy
       }}
    else
      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_call({:append, table_ref, row, opts}, _from, %State{} = state) do
    with {:ok, table} <- normalize_table(table_ref, state.default_target_schema),
         :ok <- ensure_known_table(state.context, table),
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
    DuckFeeder.BatchDispatch.handle_incoming_batch(
      state,
      table,
      batch,
      on_event: &emit_batch_queue_telemetry/3,
      on_dropped: &notify_dropped_batch/4,
      on_overflow: &handle_batch_queue_overflow/4
    )
  end

  def handle_info({ref, result}, %State{} = state) when is_reference(ref) do
    DuckFeeder.BatchDispatch.handle_batch_result(
      state,
      ref,
      result,
      on_event: &emit_batch_queue_telemetry/3,
      on_result: &notify_batch_result/4,
      on_completed: &handle_batch_completed/4
    )
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %State{} = state)
      when is_reference(ref) do
    DuckFeeder.BatchDispatch.handle_batch_down(
      state,
      ref,
      reason,
      on_task_crashed: &handle_batch_task_crashed/5
    )
  end

  defp ensure_pipeline(%State{} = state, table) do
    case TablePipelineManager.ensure_started(
           state.pipelines,
           state.pipeline_supervisor,
           table,
           self(),
           state.pipeline_opts
         ) do
      {:ok, pid, pipelines} -> {:ok, pid, %{state | pipelines: pipelines}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp handle_batch_queue_overflow(%State{} = state, table, batch, reason) do
    overflow_state =
      emit_batch_queue_telemetry(state, :overflow, %{
        table: table,
        reason: reason
      })

    send(
      overflow_state.observer_pid,
      {:duck_feeder_append_batch_queue_overflow, table, batch, reason}
    )

    send(
      overflow_state.observer_pid,
      {:duck_feeder_batch_queue_overflow, table, batch, reason}
    )

    overflow_state
  end

  defp handle_batch_completed(%State{} = state, table, result, batch) do
    _ = batch

    emit_batch_queue_telemetry(state, :completed, %{
      table: table,
      result: DuckFeeder.Telemetry.batch_result_status(result)
    })
  end

  defp handle_batch_task_crashed(%State{} = state, table, batch, reason, error) do
    send(
      state.observer_pid,
      {:duck_feeder_append_batch_processed, table, {:error, error}, batch}
    )

    send(state.observer_pid, {:duck_feeder_batch_processed, table, {:error, error}, batch})

    emit_batch_queue_telemetry(state, :task_crashed, %{
      table: table,
      reason: reason
    })
  end

  defp notify_batch_result(%State{observer_pid: observer_pid} = state, table, result, batch) do
    send(observer_pid, {:duck_feeder_append_batch_processed, table, result, batch})
    send(observer_pid, {:duck_feeder_batch_processed, table, result, batch})
    state
  end

  defp notify_dropped_batch(
         %State{observer_pid: observer_pid} = state,
         dropped_table,
         dropped_batch,
         reason
       ) do
    DuckFeeder.Telemetry.append_stream_batch_dropped(
      %{dropped_count: 1},
      %{table: dropped_table, reason: reason}
    )

    send(
      observer_pid,
      {:duck_feeder_append_batch_dropped, dropped_table, dropped_batch, reason}
    )

    send(observer_pid, {:duck_feeder_batch_dropped, dropped_table, dropped_batch, reason})

    state
  end

  defp emit_batch_queue_telemetry(%State{} = state, status, metadata)
       when is_atom(status) and is_map(metadata) do
    metadata =
      metadata
      |> Map.put(:status, status)
      |> DuckFeeder.Telemetry.put_table_metadata()

    DuckFeeder.Telemetry.append_stream_batch_queue(
      DuckFeeder.Telemetry.batch_queue_measurements(state),
      metadata
    )

    state
  end

  defp resolve_lsn(state, opts) do
    case Keyword.get(opts, :lsn) do
      lsn when is_binary(lsn) ->
        case Lsn.parse(lsn) do
          {:ok, lsn_int} ->
            {:ok, lsn, %{state | lsn_counter: max(state.lsn_counter, lsn_int)}}

          {:error, reason} ->
            {:error, {:invalid_lsn, reason}}
        end

      nil ->
        next = state.lsn_counter + 1
        {:ok, Lsn.to_string(next), %{state | lsn_counter: next}}

      other ->
        {:error, {:invalid_lsn, other}}
    end
  end

  defp ensure_known_table(context, table) when is_map(context) and is_tuple(table) do
    if Map.get(context, :designated_tables_by_target, %{}) |> Map.has_key?(table) do
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

  defp normalize_overflow_strategy(strategy) when strategy in [:fail, :drop_oldest],
    do: {:ok, strategy}

  defp normalize_overflow_strategy(other),
    do: {:error, {:invalid_option, :overflow_strategy, other}}
end
