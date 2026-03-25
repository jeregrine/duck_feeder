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
         Sink (dedup + DuckDB write)
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

  alias DuckFeeder.{DesignatedTable, Ingest, RuntimeSupport, TablePipeline}
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
          | {:duckdb, map()}
          | {:meta_module, module()}
          | {:pipeline_opts, map()}
          | {:max_tx_changes, pos_integer()}
          | {:observer_pid, pid()}
          | {:snapshot_lsn_start, String.t()}
          | {:max_inflight_batches, pos_integer()}
          | {:max_pending_batches, pos_integer()}
          | {:batch_processor, (map(), {String.t(), String.t()}, map() -> term())}

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

    with {:ok, common} <- RuntimeSupport.resolve_common_init(designated_tables, opts),
         {:ok, snapshot_lsn_counter} <- snapshot_lsn_counter(opts) do
      {:ok,
       %State{
         cdc_pipeline_pid: cdc_pipeline_pid,
         ingest_pid: ingest_pid,
         context: common.context,
         observer_pid: common.observer_pid,
         batch_task_supervisor: batch_task_supervisor,
         max_inflight_batches: common.max_inflight_batches,
         max_pending_batches: common.max_pending_batches,
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

    send(state.observer_pid, {:duck_feeder_cdc_event_processed, result, event})

    case result do
      {:error, reason} ->
        {:stop, {:cdc_pipeline_push_failed, reason}, state}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:duck_feeder_batch, table, batch}, %State{} = state) do
    DuckFeeder.BatchDispatch.handle_incoming_batch(
      state,
      table,
      batch,
      on_event: &emit_batch_queue_telemetry/3,
      on_overflow: &handle_batch_queue_overflow/4
    )
  end

  def handle_info({ref, result}, %State{} = state) when is_reference(ref) do
    DuckFeeder.BatchDispatch.handle_batch_result(
      state,
      ref,
      result,
      on_event: &emit_batch_queue_telemetry/3,
      on_result: &handle_batch_result/4,
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

  defp handle_batch_queue_overflow(%State{} = state, table, batch, reason) do
    overflow_state =
      emit_batch_queue_telemetry(state, :overflow, %{
        table: table,
        reason: reason
      })

    send(
      overflow_state.observer_pid,
      {:duck_feeder_batch_queue_overflow, table, batch, reason}
    )

    overflow_state
  end

  defp handle_batch_result(%State{} = state, table, result, batch) do
    state
    |> notify_batch_result(table, result, batch)
    |> maybe_ack_checkpoint(result)
  end

  defp handle_batch_completed(%State{} = state, table, result, batch) do
    _ = batch

    emit_batch_queue_telemetry(state, :completed, %{
      table: table,
      result: DuckFeeder.Telemetry.batch_result_status(result)
    })
  end

  defp handle_batch_task_crashed(%State{} = state, table, batch, reason, error) do
    send(state.observer_pid, {:duck_feeder_batch_processed, table, {:error, error}, batch})

    emit_batch_queue_telemetry(state, :task_crashed, %{
      table: table,
      reason: reason
    })
  end

  defp notify_batch_result(%State{observer_pid: observer_pid} = state, table, result, batch) do
    send(observer_pid, {:duck_feeder_batch_processed, table, result, batch})
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
    metadata =
      metadata
      |> Map.put(:status, status)
      |> DuckFeeder.Telemetry.put_table_metadata()

    DuckFeeder.Telemetry.service_batch_queue(
      DuckFeeder.Telemetry.batch_queue_measurements(state),
      metadata
    )

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

  defp snapshot_target_relation(designated_table) when is_map(designated_table) do
    designated_table = DesignatedTable.normalize(designated_table)

    with {:ok, target_schema} <- fetch_designated_table_string(designated_table, :target_schema),
         {:ok, target_table} <- fetch_designated_table_string(designated_table, :target_table) do
      {:ok, {target_schema, target_table}}
    end
  end

  defp build_snapshot_row(designated_table, row, lsn)
       when is_map(designated_table) and is_map(row) and is_binary(lsn) do
    designated_table = DesignatedTable.normalize(designated_table)

    with {:ok, source_schema} <- fetch_designated_table_string(designated_table, :source_schema),
         {:ok, source_table} <- fetch_designated_table_string(designated_table, :source_table),
         {:ok, target_schema} <- fetch_designated_table_string(designated_table, :target_schema),
         {:ok, target_table} <- fetch_designated_table_string(designated_table, :target_table) do
      snapshot_payload = snapshot_payload(row, lsn, source_schema, source_table)

      {:ok,
       Map.merge(snapshot_payload, %{
         checkpoint_key: DesignatedTable.checkpoint_key(designated_table),
         target_relation: {target_schema, target_table}
       })}
    end
  end

  defp snapshot_payload(row, lsn, source_schema, source_table) when is_map(row) do
    row = normalize_tagged_snapshot_row(row)

    if tagged_snapshot_row?(row) do
      %{
        _op: Map.get(row, :_op, "R") |> to_string(),
        _commit_lsn: Map.get(row, :_commit_lsn, lsn),
        _xid: Map.get(row, :_xid),
        _source_ts: Map.get(row, :_source_ts),
        _ingest_ts: Map.get(row, :_ingest_ts, DateTime.utc_now()),
        _relation_schema: Map.get(row, :_relation_schema, source_schema),
        _relation_table: Map.get(row, :_relation_table, source_table),
        _record: snapshot_record_from_tagged_row(row),
        _old_record: Map.get(row, :_old_record, %{}) |> normalize_row_map()
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

  defp tagged_snapshot_row?(row) when is_map(row) do
    Map.has_key?(row, :_op) or Map.has_key?(row, :_record) or Map.has_key?(row, :_commit_lsn)
  end

  defp snapshot_record_from_tagged_row(row) when is_map(row) do
    case Map.get(row, :_record) do
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
      "checkpoint_key",
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

  defp normalize_tagged_snapshot_row(row) when is_map(row) do
    Enum.reduce(row, %{}, fn
      {key, value}, acc when is_atom(key) ->
        Map.put(acc, key, value)

      {key, value}, acc when is_binary(key) ->
        case tagged_snapshot_metadata_key(key) do
          nil -> Map.put(acc, key, value)
          metadata_key -> Map.put(acc, metadata_key, value)
        end

      {key, value}, acc ->
        Map.put(acc, key, value)
    end)
  end

  defp fetch_designated_table_string(map, key) when is_map(map) and is_atom(key) do
    case Map.get(map, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, {:invalid_designated_table_field, key}}
    end
  end

  defp tagged_snapshot_metadata_key("_op"), do: :_op
  defp tagged_snapshot_metadata_key("_commit_lsn"), do: :_commit_lsn
  defp tagged_snapshot_metadata_key("_xid"), do: :_xid
  defp tagged_snapshot_metadata_key("_source_ts"), do: :_source_ts
  defp tagged_snapshot_metadata_key("_ingest_ts"), do: :_ingest_ts
  defp tagged_snapshot_metadata_key("_relation_schema"), do: :_relation_schema
  defp tagged_snapshot_metadata_key("_relation_table"), do: :_relation_table
  defp tagged_snapshot_metadata_key("_record"), do: :_record
  defp tagged_snapshot_metadata_key("_old_record"), do: :_old_record
  defp tagged_snapshot_metadata_key(_key), do: nil
end
