defmodule DuckFeeder.Service do
  @moduledoc """
  End-to-end CDC service orchestrator.

  Main data flow:

      {:duck_feeder_cdc_event, event}
                 |
                 v
           CDC.Pipeline
                 |
                 v
              Ingest
                 |
                 v
          TablePipeline(s)
                 |
                 v
      {:duck_feeder_batch, table, batch}
                 |
                 v
         async bounded queue/tasks
                 |
                 v
            BatchProcessor
                 |
                 v
          committed checkpoint_lsn
                 |
                 v
      {:duck_feeder_ack_lsn, checkpoint_lsn} -> CDC.Connection

  The service is intentionally fail-closed for CDC integrity: queue overflow or
  task crash stops the process so supervision can restart from durable metadata
  state.
  """

  use GenServer

  alias DuckFeeder.{BatchQueue, Ingest, TablePipeline}
  alias DuckFeeder.CDC.{Lsn, Pipeline}

  defmodule State do
    @enforce_keys [
      :cdc_pipeline_pid,
      :ingest_pid,
      :context,
      :observer_pid,
      :batch_task_supervisor,
      :max_inflight_batches,
      :max_pending_batches
    ]

    defstruct [
      :cdc_pipeline_pid,
      :ingest_pid,
      :context,
      :observer_pid,
      :batch_task_supervisor,
      :max_inflight_batches,
      :max_pending_batches,
      :cdc_pid,
      :latest_checkpoint_lsn,
      :latest_acked_lsn,
      snapshot_lsn_counter: 0,
      inflight_batch_tasks: %{},
      pending_batches: :queue.new(),
      pending_batch_count: 0
    ]

    @type inflight_task :: %{table: {String.t(), String.t()}, batch: map()}

    @type t :: %__MODULE__{
            cdc_pipeline_pid: pid(),
            ingest_pid: pid(),
            context: map(),
            observer_pid: pid(),
            batch_task_supervisor: pid(),
            max_inflight_batches: pos_integer(),
            max_pending_batches: pos_integer(),
            cdc_pid: pid() | nil,
            latest_checkpoint_lsn: String.t() | nil,
            latest_acked_lsn: String.t() | nil,
            snapshot_lsn_counter: non_neg_integer(),
            inflight_batch_tasks: %{optional(reference()) => inflight_task()},
            pending_batches: :queue.queue({{String.t(), String.t()}, map()}),
            pending_batch_count: non_neg_integer()
          }
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
          | {:max_tx_changes, pos_integer()}
          | {:observer_pid, pid()}
          | {:committer_module, module()}
          | {:committer_opts, keyword()}
          | {:snapshot_lsn_start, String.t()}
          | {:max_inflight_batches, pos_integer()}
          | {:max_pending_batches, pos_integer()}
          | {:poison_row_mode, :fail | :drop}
          | {:poison_row_sink, pid() | (map() -> term()) | {module(), atom(), [term()]}}

  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts) do
    {name_opt, opts} = Keyword.pop(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name_opt)
  end

  @spec push_event(GenServer.server(), DuckFeeder.CDC.Event.t()) ::
          :buffering | {:committed, map()} | {:error, term()}
  def push_event(server, event), do: GenServer.call(server, {:push_event, event})

  @spec in_transaction?(GenServer.server()) :: boolean()
  def in_transaction?(server), do: GenServer.call(server, :in_transaction?)

  @spec attach_cdc(GenServer.server(), pid()) :: :ok | {:error, term()}
  def attach_cdc(server, cdc_pid) when is_pid(cdc_pid) do
    GenServer.call(server, {:attach_cdc, cdc_pid})
  end

  @spec ingest_snapshot_row(GenServer.server(), map(), map()) :: :ok | {:error, term()}
  def ingest_snapshot_row(server, designated_table, row)
      when is_map(designated_table) and is_map(row) do
    GenServer.call(server, {:ingest_snapshot_row, designated_table, row})
  end

  @impl true
  def init(opts) do
    designated_tables = Keyword.fetch!(opts, :designated_tables)

    {:ok, ingest_pid} =
      Ingest.start_link(
        designated_tables: designated_tables,
        sink_pid: self(),
        pipeline_opts: Keyword.get(opts, :pipeline_opts, %{})
      )

    {:ok, cdc_pipeline_pid} =
      Pipeline.start_link(
        ingest_pid: ingest_pid,
        max_tx_changes: Keyword.get(opts, :max_tx_changes)
      )

    {:ok, batch_task_supervisor} = Task.Supervisor.start_link(strategy: :one_for_one)

    context =
      %{
        meta_conn: Keyword.fetch!(opts, :meta_conn),
        designated_table_by_target: designated_table_mapping(designated_tables),
        writer: Keyword.get(opts, :writer, %{}),
        storage: Keyword.fetch!(opts, :storage),
        object_prefix: Keyword.get(opts, :object_prefix, "duck_feeder")
      }
      |> maybe_put_optional(:meta_module, Keyword.get(opts, :meta_module))
      |> maybe_put_optional(:committer_module, Keyword.get(opts, :committer_module))
      |> maybe_put_optional(:committer_opts, Keyword.get(opts, :committer_opts))
      |> maybe_put_optional(:poison_row_mode, Keyword.get(opts, :poison_row_mode))
      |> maybe_put_optional(:poison_row_sink, Keyword.get(opts, :poison_row_sink))

    observer_pid = Keyword.get(opts, :observer_pid, self())

    with {:ok, snapshot_lsn_counter} <- snapshot_lsn_counter(opts),
         {:ok, max_inflight_batches} <-
           normalize_positive_integer(
             Keyword.get(opts, :max_inflight_batches, 1),
             :max_inflight_batches
           ),
         {:ok, max_pending_batches} <-
           normalize_positive_integer(
             Keyword.get(opts, :max_pending_batches, 1_000),
             :max_pending_batches
           ) do
      {:ok,
       %State{
         cdc_pipeline_pid: cdc_pipeline_pid,
         ingest_pid: ingest_pid,
         context: context,
         observer_pid: observer_pid,
         batch_task_supervisor: batch_task_supervisor,
         max_inflight_batches: max_inflight_batches,
         max_pending_batches: max_pending_batches,
         snapshot_lsn_counter: snapshot_lsn_counter
       }}
    else
      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_call({:push_event, event}, _from, %State{cdc_pipeline_pid: cdc_pipeline_pid} = state) do
    {:reply, Pipeline.push_event(cdc_pipeline_pid, event), state}
  end

  def handle_call(:in_transaction?, _from, %State{cdc_pipeline_pid: cdc_pipeline_pid} = state) do
    {:reply, Pipeline.in_transaction?(cdc_pipeline_pid), state}
  end

  def handle_call({:attach_cdc, cdc_pid}, _from, %State{} = state) when is_pid(cdc_pid) do
    next_state =
      state
      |> Map.put(:cdc_pid, cdc_pid)
      |> maybe_replay_checkpoint_ack()
      |> emit_ack_checkpoint_lag_telemetry(:attach_cdc)

    {:reply, :ok, next_state}
  end

  def handle_call({:attach_cdc, other}, _from, %State{} = state) do
    {:reply, {:error, {:invalid_cdc_pid, other}}, state}
  end

  def handle_call({:ingest_snapshot_row, designated_table, row}, _from, %State{} = state) do
    with {:ok, table} <- snapshot_target_relation(designated_table),
         {:ok, pipeline} <- Ingest.table_pipeline(state.ingest_pid, table),
         {lsn, next_counter} <- next_snapshot_lsn(state.snapshot_lsn_counter),
         {:ok, snapshot_row} <- build_snapshot_row(designated_table, row, lsn),
         :ok <- TablePipeline.append(pipeline, snapshot_row, lsn) do
      {:reply, :ok, %{state | snapshot_lsn_counter: next_counter}}
    else
      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_info(
        {:duck_feeder_cdc_event, event},
        %State{cdc_pipeline_pid: cdc_pipeline_pid} = state
      ) do
    result = Pipeline.push_event(cdc_pipeline_pid, event)

    if is_pid(state.observer_pid) do
      send(state.observer_pid, {:duck_feeder_cdc_event_processed, result, event})
    end

    case result do
      {:error, reason} ->
        {:stop, {:cdc_pipeline_push_failed, reason}, state}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:duck_feeder_batch, table, batch}, %State{} = state) do
    case BatchQueue.enqueue_or_start_batch(
           state,
           table,
           batch,
           on_event: &emit_batch_queue_telemetry/3
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
          |> maybe_ack_checkpoint(result)
          |> BatchQueue.maybe_start_queued_batches(on_event: &emit_batch_queue_telemetry/3)
          |> emit_batch_queue_telemetry(:completed, %{
            table: table,
            result: batch_result_status(result)
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

  defp notify_batch_result(%State{observer_pid: observer_pid} = state, table, result, batch) do
    if is_pid(observer_pid) do
      send(observer_pid, {:duck_feeder_batch_processed, table, result, batch})
    end

    state
  end

  defp maybe_ack_checkpoint(%State{} = state, {:ok, result}) when is_map(result) do
    checkpoint_lsn = Map.get(result, :checkpoint_lsn)

    if is_binary(checkpoint_lsn) do
      state
      |> maybe_update_checkpoint_lsn(checkpoint_lsn)
      |> maybe_send_checkpoint_ack(checkpoint_lsn)
      |> emit_ack_checkpoint_lag_telemetry(:batch_commit)
    else
      state
    end
  end

  defp maybe_ack_checkpoint(state, _result), do: state

  defp maybe_update_checkpoint_lsn(%State{} = state, checkpoint_lsn)
       when is_binary(checkpoint_lsn),
       do: %{state | latest_checkpoint_lsn: checkpoint_lsn}

  defp maybe_update_checkpoint_lsn(%State{} = state, _checkpoint_lsn), do: state

  defp maybe_send_checkpoint_ack(
         %State{cdc_pid: cdc_pid} = state,
         checkpoint_lsn
       )
       when is_binary(checkpoint_lsn) and is_pid(cdc_pid) do
    send(cdc_pid, {:duck_feeder_ack_lsn, checkpoint_lsn})
    %{state | latest_acked_lsn: checkpoint_lsn}
  end

  defp maybe_send_checkpoint_ack(%State{} = state, _checkpoint_lsn), do: state

  defp maybe_replay_checkpoint_ack(
         %State{cdc_pid: cdc_pid, latest_checkpoint_lsn: checkpoint_lsn} = state
       )
       when is_pid(cdc_pid) and is_binary(checkpoint_lsn) do
    send(cdc_pid, {:duck_feeder_ack_lsn, checkpoint_lsn})
    %{state | latest_acked_lsn: checkpoint_lsn}
  end

  defp maybe_replay_checkpoint_ack(%State{} = state), do: state

  defp emit_batch_queue_telemetry(%State{} = state, status, metadata)
       when is_atom(status) and is_map(metadata) do
    measurements = %{
      pending_batch_count: state.pending_batch_count,
      inflight_batch_count: map_size(state.inflight_batch_tasks),
      max_pending_batches: state.max_pending_batches,
      max_inflight_batches: state.max_inflight_batches
    }

    metadata =
      metadata
      |> Map.put(:status, status)
      |> maybe_put_table_metadata()

    DuckFeeder.Telemetry.service_batch_queue(measurements, metadata)
    state
  end

  defp emit_ack_checkpoint_lag_telemetry(
         %State{latest_checkpoint_lsn: checkpoint_lsn} = state,
         source
       )
       when is_binary(checkpoint_lsn) and is_atom(source) do
    {measurements, metadata} =
      checkpoint_ack_lag_measurements(
        checkpoint_lsn,
        state.latest_acked_lsn,
        source,
        is_pid(state.cdc_pid)
      )

    DuckFeeder.Telemetry.service_ack_checkpoint_lag(measurements, metadata)
    state
  end

  defp emit_ack_checkpoint_lag_telemetry(%State{} = state, _source), do: state

  defp checkpoint_ack_lag_measurements(checkpoint_lsn, ack_lsn, source, cdc_attached?) do
    {lag_bytes, lag_known?, status} =
      with {:ok, checkpoint_int} <- Lsn.parse(checkpoint_lsn),
           {:ok, ack_int} <- parse_optional_lsn(ack_lsn) do
        {max(checkpoint_int - ack_int, 0), true, :ok}
      else
        {:error, :missing_ack_lsn} -> {0, false, :missing_ack_lsn}
        {:error, _reason} -> {0, false, :invalid_lsn}
      end

    measurements = %{
      lag_bytes: lag_bytes,
      lag_known: if(lag_known?, do: 1, else: 0),
      cdc_attached: if(cdc_attached?, do: 1, else: 0)
    }

    metadata = %{
      source: source,
      status: status,
      checkpoint_lsn: checkpoint_lsn,
      ack_lsn: ack_lsn
    }

    {measurements, metadata}
  end

  defp parse_optional_lsn(lsn) when is_binary(lsn), do: Lsn.parse(lsn)
  defp parse_optional_lsn(nil), do: {:error, :missing_ack_lsn}
  defp parse_optional_lsn(_other), do: {:error, :invalid_ack_lsn}

  defp batch_result_status({:ok, _result}), do: :ok
  defp batch_result_status({:error, _reason}), do: :error
  defp batch_result_status(_result), do: :unknown

  defp maybe_put_table_metadata(metadata) when is_map(metadata) do
    case Map.get(metadata, :table) do
      {schema, table} when is_binary(schema) and is_binary(table) ->
        metadata
        |> Map.put(:table_schema, schema)
        |> Map.put(:table_name, table)

      _ ->
        metadata
    end
  end

  defp designated_table_mapping(designated_tables) do
    designated_tables
    |> Enum.reduce(%{}, fn designated_table, acc ->
      id = Map.fetch!(designated_table, :id)

      target =
        {Map.fetch!(designated_table, :target_schema),
         Map.fetch!(designated_table, :target_table)}

      Map.put(acc, target, id)
    end)
  end

  defp snapshot_target_relation(designated_table) when is_map(designated_table) do
    with {:ok, target_schema} <- fetch_map_string(designated_table, :target_schema),
         {:ok, target_table} <- fetch_map_string(designated_table, :target_table) do
      {:ok, {target_schema, target_table}}
    end
  end

  defp build_snapshot_row(designated_table, row, lsn)
       when is_map(designated_table) and is_map(row) and is_binary(lsn) do
    with {:ok, designated_table_id} <- fetch_map_value(designated_table, :id),
         {:ok, source_schema} <- fetch_map_string(designated_table, :source_schema),
         {:ok, source_table} <- fetch_map_string(designated_table, :source_table),
         {:ok, target_schema} <- fetch_map_string(designated_table, :target_schema),
         {:ok, target_table} <- fetch_map_string(designated_table, :target_table) do
      snapshot_payload = snapshot_payload(row, lsn, source_schema, source_table)

      {:ok,
       Map.merge(snapshot_payload, %{
         designated_table_id: designated_table_id,
         target_relation: {target_schema, target_table}
       })}
    end
  end

  defp snapshot_payload(row, lsn, source_schema, source_table) when is_map(row) do
    if pre_tagged_snapshot_row?(row) do
      %{
        _op: fetch_map_value_or_default(row, :_op, "R") |> to_string(),
        _commit_lsn: fetch_map_value_or_default(row, :_commit_lsn, lsn),
        _xid: fetch_map_value_or_default(row, :_xid, nil),
        _source_ts: fetch_map_value_or_default(row, :_source_ts, nil),
        _ingest_ts: fetch_map_value_or_default(row, :_ingest_ts, DateTime.utc_now()),
        _relation_schema: fetch_map_value_or_default(row, :_relation_schema, source_schema),
        _relation_table: fetch_map_value_or_default(row, :_relation_table, source_table),
        _record: snapshot_record_from_tagged_row(row),
        _old_record: fetch_map_value_or_default(row, :_old_record, %{}) |> normalize_row_map()
      }
    else
      %{
        _op: "I",
        _commit_lsn: lsn,
        _xid: nil,
        _source_ts: nil,
        _ingest_ts: DateTime.utc_now(),
        _relation_schema: source_schema,
        _relation_table: source_table,
        _record: stringify_keys(row),
        _old_record: %{}
      }
    end
  end

  defp pre_tagged_snapshot_row?(row) when is_map(row) do
    has_metadata_key?(row, :_op) or has_metadata_key?(row, :_record) or
      has_metadata_key?(row, :_commit_lsn)
  end

  defp snapshot_record_from_tagged_row(row) when is_map(row) do
    case fetch_map_value_or_default(row, :_record, nil) do
      record when is_map(record) ->
        stringify_keys(record)

      _ ->
        row
        |> Enum.reject(fn {key, _value} -> snapshot_metadata_key?(key) end)
        |> Map.new()
        |> stringify_keys()
    end
  end

  defp normalize_row_map(map) when is_map(map), do: stringify_keys(map)
  defp normalize_row_map(_other), do: %{}

  defp has_metadata_key?(map, key) when is_map(map) and is_atom(key) do
    Map.has_key?(map, key) or Map.has_key?(map, Atom.to_string(key))
  end

  defp snapshot_metadata_key?(key) when is_atom(key),
    do: snapshot_metadata_key?(Atom.to_string(key))

  defp snapshot_metadata_key?(key) when is_binary(key) do
    key in [
      "_op",
      "_commit_lsn",
      "_xid",
      "_source_ts",
      "_ingest_ts",
      "_relation_schema",
      "_relation_table",
      "_record",
      "_old_record",
      "designated_table_id",
      "target_relation"
    ]
  end

  defp snapshot_metadata_key?(_key), do: false

  defp next_snapshot_lsn(counter) when is_integer(counter) and counter >= 0 do
    next_counter = counter + 1
    {Lsn.to_string(next_counter), next_counter}
  end

  defp snapshot_lsn_counter(opts) when is_list(opts) do
    case Keyword.get(opts, :snapshot_lsn_start, "0/0") do
      lsn when is_binary(lsn) -> Lsn.parse(lsn)
      other -> {:error, {:invalid_snapshot_lsn_start, other}}
    end
  end

  defp stringify_keys(map) when is_map(map) do
    map
    |> Enum.map(fn {k, v} -> {to_string(k), v} end)
    |> Map.new()
  end

  defp fetch_map_string(map, key) when is_map(map) and is_atom(key) do
    case Map.get(map, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, {:invalid_designated_table_field, key}}
    end
  end

  defp fetch_map_value(map, key) when is_map(map) do
    case Map.get(map, key) do
      nil -> {:error, {:invalid_designated_table_field, key}}
      value -> {:ok, value}
    end
  end

  defp fetch_map_value_or_default(map, key, default) when is_map(map) and is_atom(key) do
    Map.get(map, key, Map.get(map, Atom.to_string(key), default))
  end

  defp normalize_positive_integer(value, _key) when is_integer(value) and value > 0,
    do: {:ok, value}

  defp normalize_positive_integer(value, key), do: {:error, {:invalid_option, key, value}}

  defp maybe_put_optional(context, _key, nil), do: context
  defp maybe_put_optional(context, key, value), do: Map.put(context, key, value)
end
