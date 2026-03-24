defmodule DuckFeeder.TablePipeline do
  @moduledoc """
  Per-table micro-batch pipeline.

  Buffers routed CDC rows and emits flush batches based on row/byte/time thresholds.
  """

  use GenServer

  alias DuckFeeder.Ingest.BatchBuffer

  defmodule State do
    @enforce_keys [:table, :buffer, :flush_interval_ms]
    defstruct [:table, :buffer, :flush_interval_ms, :sink_pid, :on_flush, :timer_ref]

    @type on_flush_callback :: function()

    @type t :: %__MODULE__{
            table: {String.t(), String.t()},
            buffer: BatchBuffer.t(),
            flush_interval_ms: pos_integer(),
            sink_pid: pid() | nil,
            on_flush: on_flush_callback() | nil,
            timer_ref: reference() | nil
          }
  end

  @type option ::
          {:name, GenServer.name()}
          | {:table, {String.t(), String.t()}}
          | {:max_rows, pos_integer()}
          | {:max_bytes, pos_integer()}
          | {:flush_interval_ms, pos_integer()}
          | {:sink_pid, pid()}
          | {:on_flush, function()}

  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts) do
    {name_opt, opts} = Keyword.pop(opts, :name)

    GenServer.start_link(__MODULE__, opts, name: name_opt)
  end

  @spec append(GenServer.server(), map(), String.t(), keyword()) :: :ok | {:error, term()}
  def append(server, row, commit_lsn, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 5_000)
    GenServer.call(server, {:append, row, commit_lsn, opts}, timeout)
  catch
    :exit, reason -> {:error, {:table_pipeline_append_failed, reason}}
  end

  @spec flush(GenServer.server()) :: :empty | {:ok, BatchBuffer.batch()}
  def flush(server) do
    GenServer.call(server, :flush)
  end

  @spec stats(GenServer.server()) :: map()
  def stats(server) do
    GenServer.call(server, :stats)
  end

  @impl true
  def init(opts) do
    table = Keyword.fetch!(opts, :table)
    flush_interval_ms = Keyword.get(opts, :flush_interval_ms, 5_000)

    buffer =
      BatchBuffer.new(
        max_rows: Keyword.get(opts, :max_rows, 10_000),
        max_bytes: Keyword.get(opts, :max_bytes, 128 * 1_024 * 1_024),
        flush_interval_ms: flush_interval_ms
      )

    state =
      %State{
        table: table,
        buffer: buffer,
        flush_interval_ms: flush_interval_ms,
        sink_pid: Keyword.get(opts, :sink_pid),
        on_flush: Keyword.get(opts, :on_flush)
      }
      |> schedule_flush_tick()

    {:ok, state}
  end

  @impl true
  def handle_cast({:append, row, commit_lsn, opts}, %State{} = state) do
    {_result, next_state} = process_append(state, row, commit_lsn, opts)
    {:noreply, next_state}
  end

  @impl true
  def handle_call({:append, row, commit_lsn, opts}, _from, %State{} = state) do
    {result, next_state} = process_append(state, row, commit_lsn, opts)
    {:reply, result, next_state}
  end

  @impl true
  def handle_call(:flush, _from, %State{buffer: buffer} = state) do
    case BatchBuffer.flush(buffer) do
      {:empty, _buffer} ->
        {:reply, :empty, state}

      {:ok, batch, next_buffer} ->
        emit_flush(state, batch)
        {:reply, {:ok, batch}, %{state | buffer: next_buffer}}
    end
  end

  def handle_call(:stats, _from, %State{buffer: buffer} = state) do
    stats = %{
      table: state.table,
      row_count: buffer.row_count,
      byte_count: buffer.byte_count,
      lsn_start: buffer.lsn_start,
      lsn_end: buffer.lsn_end,
      flush_interval_ms: state.flush_interval_ms
    }

    {:reply, stats, state}
  end

  @impl true
  def handle_info(:flush_tick, %State{buffer: buffer} = state) do
    state =
      if BatchBuffer.due_flush?(buffer) do
        case BatchBuffer.flush(buffer) do
          {:ok, batch, next_buffer} ->
            emit_flush(state, batch)
            %{state | buffer: next_buffer}

          {:empty, _} ->
            state
        end
      else
        state
      end

    {:noreply, schedule_flush_tick(state)}
  end

  defp process_append(%State{buffer: buffer} = state, row, commit_lsn, opts) do
    case BatchBuffer.append(buffer, row, commit_lsn, opts) do
      {:ok, next_buffer} ->
        {:ok, %{state | buffer: next_buffer}}

      {:flush, batch, next_buffer} ->
        emit_flush(state, batch)
        {:ok, %{state | buffer: next_buffer}}
    end
  rescue
    exception ->
      {{:error, {:table_pipeline_append_exception, exception}}, state}
  catch
    kind, reason ->
      {{:error, {:table_pipeline_append_throw, kind, reason}}, state}
  end

  defp emit_flush(%State{table: table, sink_pid: sink_pid, on_flush: on_flush}, batch) do
    DuckFeeder.Telemetry.batch_flushed(table, batch)

    if is_pid(sink_pid) do
      send(sink_pid, {:duck_feeder_batch, table, batch})
    end

    run_on_flush(on_flush, table, batch)
    :ok
  end

  defp run_on_flush(on_flush, table, batch) when is_function(on_flush, 2) do
    on_flush.(table, batch)
  end

  defp run_on_flush(on_flush, table, batch) when is_function(on_flush, 1) do
    on_flush.({table, batch})
  end

  defp run_on_flush(_on_flush, _table, _batch), do: :ok

  defp schedule_flush_tick(%State{flush_interval_ms: interval, timer_ref: timer_ref} = state) do
    if is_reference(timer_ref), do: Process.cancel_timer(timer_ref)

    %{state | timer_ref: Process.send_after(self(), :flush_tick, interval)}
  end
end
