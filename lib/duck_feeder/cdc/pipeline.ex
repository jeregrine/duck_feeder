defmodule DuckFeeder.CDC.Pipeline do
  @moduledoc """
  CDC event pipeline.

  Accepts normalized CDC events, buffers transaction boundaries, and forwards
  committed transactions to `DuckFeeder.Ingest`.
  """

  use GenServer

  alias DuckFeeder.CDC.{Event, TransactionBuffer}
  alias DuckFeeder.Ingest

  defmodule State do
    @enforce_keys [:ingest_pid, :buffer]
    defstruct [:ingest_pid, :buffer]

    @type t :: %__MODULE__{
            ingest_pid: pid(),
            buffer: TransactionBuffer.State.t()
          }
  end

  @type option ::
          {:name, GenServer.name()} | {:ingest_pid, pid()} | {:max_tx_changes, pos_integer()}

  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts) do
    {name_opt, opts} = Keyword.pop(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name_opt)
  end

  @spec push_event(GenServer.server(), Event.t()) ::
          :buffering | {:committed, map()} | {:error, term()}
  def push_event(server, event) do
    GenServer.call(server, {:push_event, event})
  end

  @spec in_transaction?(GenServer.server()) :: boolean()
  def in_transaction?(server), do: GenServer.call(server, :in_transaction?)

  @impl true
  def init(opts) do
    ingest_pid = Keyword.fetch!(opts, :ingest_pid)

    buffer =
      TransactionBuffer.new(max_changes: Keyword.get(opts, :max_tx_changes))

    {:ok, %State{ingest_pid: ingest_pid, buffer: buffer}}
  end

  @impl true
  def handle_call({:push_event, event}, _from, %State{buffer: buffer} = state) do
    case TransactionBuffer.handle_event(buffer, event) do
      {:buffering, next_buffer} ->
        DuckFeeder.Telemetry.cdc_event(event_type(event), :buffering)
        {:reply, :buffering, %{state | buffer: next_buffer}}

      {:ok, transaction, next_buffer} ->
        case Ingest.ingest_transaction(state.ingest_pid, transaction) do
          :ok ->
            DuckFeeder.Telemetry.cdc_event(event_type(event), :committed)
            {:reply, {:committed, transaction}, %{state | buffer: next_buffer}}

          {:error, reason} ->
            DuckFeeder.Telemetry.cdc_event(event_type(event), :error)
            {:reply, {:error, {:ingest_failed, reason}}, %{state | buffer: next_buffer}}
        end

      {:error, reason} ->
        DuckFeeder.Telemetry.cdc_event(event_type(event), :error)
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call(:in_transaction?, _from, %State{buffer: buffer} = state) do
    {:reply, TransactionBuffer.in_transaction?(buffer), state}
  end

  defp event_type(%{__struct__: module}), do: module
  defp event_type(other), do: other
end
