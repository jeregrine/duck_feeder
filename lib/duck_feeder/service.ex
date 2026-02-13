defmodule DuckFeeder.Service do
  @moduledoc """
  End-to-end service wiring:

  CDC events -> transaction buffer -> ingest router/pipelines -> batch processor.
  """

  use GenServer

  alias DuckFeeder.{BatchProcessor, Ingest}
  alias DuckFeeder.CDC.Pipeline

  defmodule State do
    @enforce_keys [:cdc_pipeline_pid, :ingest_pid, :context, :observer_pid]
    defstruct [:cdc_pipeline_pid, :ingest_pid, :context, :observer_pid]

    @type t :: %__MODULE__{
            cdc_pipeline_pid: pid(),
            ingest_pid: pid(),
            context: map(),
            observer_pid: pid()
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

  @impl true
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

  defp maybe_put_optional(context, _key, nil), do: context
  defp maybe_put_optional(context, key, value), do: Map.put(context, key, value)
end
