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

  alias DuckFeeder.{BatchQueue, StreamSupport, TablePipeline}
  alias DuckFeeder.CDC.Lsn

  defmodule State do
    @enforce_keys [
      :pipeline_supervisor,
      :pipeline_opts,
      :context,
      :designated_table_by_target,
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
      :designated_table_by_target,
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
            designated_table_by_target: %{optional({String.t(), String.t()}) => String.t()},
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
          | {:sink_module, module()}
          | {:meta_module, module()}
          | {:object_prefix, String.t()}
          | {:pipeline_opts, map()}
          | {:observer_pid, pid()}
          | {:default_target_schema, String.t()}
          | {:start_lsn, String.t()}
          | {:max_inflight_batches, pos_integer()}
          | {:max_pending_batches, pos_integer()}
          | {:overflow_strategy, :fail | :drop_oldest}
          | {:poison_row_mode, :fail | :drop}
          | {:poison_row_sink, pid() | (map() -> term()) | {module(), atom(), [term()]}}

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

    with {:ok, sink_module} <- StreamSupport.resolve_sink_module_option(opts),
         {:ok, duckdb} <- StreamSupport.resolve_duckdb(opts, sink_module),
         {:ok, lsn_counter} <- Lsn.parse(start_lsn),
         {:ok, max_inflight_batches} <-
           StreamSupport.normalize_positive_integer(
             Keyword.get(opts, :max_inflight_batches, 1),
             :max_inflight_batches
           ),
         {:ok, max_pending_batches} <-
           StreamSupport.normalize_positive_integer(
             Keyword.get(opts, :max_pending_batches, 1_000),
             :max_pending_batches
           ),
         {:ok, overflow_strategy} <-
           normalize_overflow_strategy(Keyword.get(opts, :overflow_strategy, :fail)),
         {:ok, pipeline_supervisor} <- DynamicSupervisor.start_link(strategy: :one_for_one),
         {:ok, batch_task_supervisor} <- Task.Supervisor.start_link(strategy: :one_for_one) do
      object_prefix = Keyword.get(opts, :object_prefix, "duck_feeder_append")

      designated_table_by_target =
        StreamSupport.designated_table_mapping(designated_tables, object_prefix)

      context =
        %{
          meta_conn: Keyword.fetch!(opts, :meta_conn),
          designated_table_by_target: designated_table_by_target,
          designated_table_config_by_target:
            StreamSupport.designated_table_config_mapping(designated_tables),
          object_prefix: object_prefix,
          sink_module: sink_module
        }
        |> StreamSupport.maybe_put_optional(:duckdb, duckdb)
        |> StreamSupport.maybe_put_optional(:meta_module, Keyword.get(opts, :meta_module))
        |> StreamSupport.maybe_put_optional(:poison_row_mode, Keyword.get(opts, :poison_row_mode))
        |> StreamSupport.maybe_put_optional(:poison_row_sink, Keyword.get(opts, :poison_row_sink))

      {:ok,
       %State{
         pipeline_supervisor: pipeline_supervisor,
         pipeline_opts: Keyword.get(opts, :pipeline_opts, %{}) |> Map.new(),
         context: context,
         designated_table_by_target: designated_table_by_target,
         default_target_schema: Keyword.get(opts, :default_target_schema, "raw"),
         observer_pid: Keyword.get(opts, :observer_pid, self()),
         lsn_counter: lsn_counter,
         batch_task_supervisor: batch_task_supervisor,
         max_inflight_batches: max_inflight_batches,
         max_pending_batches: max_pending_batches,
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
    case BatchQueue.enqueue_or_start_batch(
           state,
           table,
           batch,
           on_event: &emit_batch_queue_telemetry/3,
           on_dropped: &notify_dropped_batch/4
         ) do
      {:ok, next_state} ->
        {:noreply, next_state}

      {:error, reason, next_state} ->
        overflow_state =
          emit_batch_queue_telemetry(next_state, :overflow, %{
            table: table,
            reason: reason
          })

        if is_pid(overflow_state.observer_pid) do
          send(
            overflow_state.observer_pid,
            {:duck_feeder_append_batch_queue_overflow, table, batch, reason}
          )

          send(
            overflow_state.observer_pid,
            {:duck_feeder_batch_queue_overflow, table, batch, reason}
          )
        end

        {:stop, reason, overflow_state}
    end
  end

  def handle_info({ref, result}, %State{inflight_batch_tasks: inflight} = state)
      when is_reference(ref) do
    case Map.pop(inflight, ref) do
      {nil, _inflight} ->
        {:noreply, state}

      {%{table: table, batch: batch}, next_inflight} ->
        Process.demonitor(ref, [:flush])

        next_state =
          state
          |> Map.put(:inflight_batch_tasks, next_inflight)
          |> notify_batch_result(table, result, batch)
          |> BatchQueue.maybe_start_queued_batches(on_event: &emit_batch_queue_telemetry/3)
          |> emit_batch_queue_telemetry(:completed, %{
            table: table,
            result: StreamSupport.batch_result_status(result)
          })

        {:noreply, next_state}
    end
  end

  def handle_info(
        {:DOWN, ref, :process, _pid, reason},
        %State{inflight_batch_tasks: inflight} = state
      )
      when is_reference(ref) do
    case Map.get(inflight, ref) do
      nil ->
        {:noreply, state}

      %{table: table, batch: batch} ->
        if BatchQueue.normal_down_reason?(reason) do
          {:noreply, state}
        else
          {_task, next_inflight} = Map.pop(inflight, ref)
          error = {:batch_task_crashed, reason}

          next_state = %{state | inflight_batch_tasks: next_inflight}

          if is_pid(next_state.observer_pid) do
            send(
              next_state.observer_pid,
              {:duck_feeder_append_batch_processed, table, {:error, error}, batch}
            )

            send(
              next_state.observer_pid,
              {:duck_feeder_batch_processed, table, {:error, error}, batch}
            )
          end

          next_state =
            emit_batch_queue_telemetry(next_state, :task_crashed, %{
              table: table,
              reason: reason
            })

          {:stop, error, next_state}
        end
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

  defp notify_batch_result(%State{observer_pid: observer_pid} = state, table, result, batch) do
    if is_pid(observer_pid) do
      send(observer_pid, {:duck_feeder_append_batch_processed, table, result, batch})
      send(observer_pid, {:duck_feeder_batch_processed, table, result, batch})
    end

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

    if is_pid(observer_pid) do
      send(
        observer_pid,
        {:duck_feeder_append_batch_dropped, dropped_table, dropped_batch, reason}
      )

      send(observer_pid, {:duck_feeder_batch_dropped, dropped_table, dropped_batch, reason})
    end

    state
  end

  defp emit_batch_queue_telemetry(%State{} = state, status, metadata)
       when is_atom(status) and is_map(metadata) do
    metadata =
      metadata
      |> Map.put(:status, status)
      |> StreamSupport.maybe_put_table_metadata()

    DuckFeeder.Telemetry.append_stream_batch_queue(
      StreamSupport.batch_queue_measurements(state),
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

  defp normalize_overflow_strategy(strategy) when strategy in [:fail, :drop_oldest],
    do: {:ok, strategy}

  defp normalize_overflow_strategy(other),
    do: {:error, {:invalid_option, :overflow_strategy, other}}
end
