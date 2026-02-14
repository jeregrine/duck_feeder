defmodule DuckFeeder.Service do
  @moduledoc """
  End-to-end service wiring:

  CDC events -> transaction buffer -> ingest router/pipelines -> batch processor.
  """

  use GenServer

  alias DuckFeeder.{BatchProcessor, Ingest, TablePipeline}
  alias DuckFeeder.CDC.{Lsn, Pipeline}

  defmodule State do
    @enforce_keys [:cdc_pipeline_pid, :ingest_pid, :context, :observer_pid]
    defstruct [:cdc_pipeline_pid, :ingest_pid, :context, :observer_pid, snapshot_lsn_counter: 0]

    @type t :: %__MODULE__{
            cdc_pipeline_pid: pid(),
            ingest_pid: pid(),
            context: map(),
            observer_pid: pid(),
            snapshot_lsn_counter: non_neg_integer()
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

    observer_pid = Keyword.get(opts, :observer_pid, self())

    {:ok,
     %State{
       cdc_pipeline_pid: cdc_pipeline_pid,
       ingest_pid: ingest_pid,
       context: context,
       observer_pid: observer_pid
     }}
  end

  @impl true
  def handle_call({:push_event, event}, _from, %State{cdc_pipeline_pid: cdc_pipeline_pid} = state) do
    {:reply, Pipeline.push_event(cdc_pipeline_pid, event), state}
  end

  def handle_call(:in_transaction?, _from, %State{cdc_pipeline_pid: cdc_pipeline_pid} = state) do
    {:reply, Pipeline.in_transaction?(cdc_pipeline_pid), state}
  end

  def handle_call({:ingest_snapshot_row, designated_table, row}, _from, %State{} = state) do
    with {:ok, table} <- snapshot_target_relation(designated_table),
         {:ok, pipeline} <- Ingest.table_pipeline(state.ingest_pid, table),
         {lsn, next_counter} <- next_snapshot_lsn(state.snapshot_lsn_counter),
         {:ok, snapshot_row} <- build_snapshot_row(designated_table, row, lsn) do
      :ok = TablePipeline.append(pipeline, snapshot_row, lsn)
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

    {:noreply, state}
  end

  def handle_info({:duck_feeder_batch, table, batch}, %State{} = state) do
    result = BatchProcessor.process_batch(state.context, table, batch)

    if is_pid(state.observer_pid) do
      send(state.observer_pid, {:duck_feeder_batch_processed, table, result, batch})
    end

    {:noreply, state}
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
      {:ok,
       %{
         _op: "I",
         _commit_lsn: lsn,
         _xid: nil,
         _source_ts: nil,
         _ingest_ts: DateTime.utc_now(),
         _relation_schema: source_schema,
         _relation_table: source_table,
         _record: stringify_keys(row),
         _old_record: %{},
         designated_table_id: designated_table_id,
         target_relation: {target_schema, target_table}
       }}
    end
  end

  defp next_snapshot_lsn(counter) when is_integer(counter) and counter >= 0 do
    next_counter = counter + 1
    {Lsn.to_string(next_counter), next_counter}
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

  defp maybe_put_optional(context, _key, nil), do: context
  defp maybe_put_optional(context, key, value), do: Map.put(context, key, value)
end
