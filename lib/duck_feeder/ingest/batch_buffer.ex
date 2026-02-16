defmodule DuckFeeder.Ingest.BatchBuffer do
  @moduledoc """
  In-memory micro-batch buffer with row/byte/time thresholds.
  """

  @enforce_keys [:max_rows, :max_bytes, :flush_interval_ms]
  defstruct max_rows: nil,
            max_bytes: nil,
            flush_interval_ms: nil,
            rows: [],
            row_count: 0,
            byte_count: 0,
            lsn_start: nil,
            lsn_end: nil,
            opened_at_mono_ms: nil

  @type t :: %__MODULE__{
          max_rows: pos_integer(),
          max_bytes: pos_integer(),
          flush_interval_ms: pos_integer(),
          rows: [map()],
          row_count: non_neg_integer(),
          byte_count: non_neg_integer(),
          lsn_start: String.t() | nil,
          lsn_end: String.t() | nil,
          opened_at_mono_ms: non_neg_integer() | nil
        }

  @type batch :: %{
          rows: [map()],
          row_count: non_neg_integer(),
          byte_count: non_neg_integer(),
          lsn_start: String.t() | nil,
          lsn_end: String.t() | nil
        }

  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    %__MODULE__{
      max_rows: Keyword.get(opts, :max_rows, 10_000),
      max_bytes: Keyword.get(opts, :max_bytes, 128 * 1_024 * 1_024),
      flush_interval_ms: Keyword.get(opts, :flush_interval_ms, 5_000)
    }
  end

  @spec empty?(t()) :: boolean()
  def empty?(%__MODULE__{row_count: row_count}), do: row_count == 0

  @spec append(t(), map(), String.t(), keyword()) :: {:ok, t()} | {:flush, batch(), t()}
  def append(%__MODULE__{} = state, row, commit_lsn, opts \\ []) when is_binary(commit_lsn) do
    row_size = Keyword.get(opts, :row_size, estimate_row_size(row))
    now = Keyword.get(opts, :now_mono_ms, monotonic_ms())

    next_state =
      state
      |> maybe_set_opened_at(now)
      |> put_lsn_start_if_nil(commit_lsn)
      |> Map.put(:lsn_end, commit_lsn)
      |> Map.update!(:rows, &[row | &1])
      |> Map.update!(:row_count, &(&1 + 1))
      |> Map.update!(:byte_count, &(&1 + row_size))

    if threshold_flush?(next_state) do
      {:flush, to_batch(next_state), reset_state(next_state)}
    else
      {:ok, next_state}
    end
  end

  @spec due_flush?(t(), non_neg_integer()) :: boolean()
  def due_flush?(%__MODULE__{} = state, now_mono_ms \\ monotonic_ms()) do
    not empty?(state) and
      not is_nil(state.opened_at_mono_ms) and
      now_mono_ms - state.opened_at_mono_ms >= state.flush_interval_ms
  end

  @spec flush(t()) :: {:empty, t()} | {:ok, batch(), t()}
  def flush(%__MODULE__{} = state) do
    if empty?(state) do
      {:empty, state}
    else
      {:ok, to_batch(state), reset_state(state)}
    end
  end

  defp threshold_flush?(state) do
    state.row_count >= state.max_rows or state.byte_count >= state.max_bytes
  end

  defp to_batch(state) do
    %{
      rows: Enum.reverse(state.rows),
      row_count: state.row_count,
      byte_count: state.byte_count,
      lsn_start: state.lsn_start,
      lsn_end: state.lsn_end
    }
  end

  defp reset_state(state) do
    %{
      state
      | rows: [],
        row_count: 0,
        byte_count: 0,
        lsn_start: nil,
        lsn_end: nil,
        opened_at_mono_ms: nil
    }
  end

  defp maybe_set_opened_at(%__MODULE__{opened_at_mono_ms: nil} = state, now),
    do: %{state | opened_at_mono_ms: now}

  defp maybe_set_opened_at(state, _now), do: state

  defp put_lsn_start_if_nil(%__MODULE__{lsn_start: nil} = state, lsn),
    do: %{state | lsn_start: lsn}

  defp put_lsn_start_if_nil(state, _lsn), do: state

  @max_collection_sample 16
  @max_depth 4

  defp estimate_row_size(row), do: estimate_term_size(row, 0)

  defp estimate_term_size(_term, depth) when depth >= @max_depth, do: 32

  defp estimate_term_size(value, _depth) when is_binary(value), do: byte_size(value)
  defp estimate_term_size(value, _depth) when is_integer(value), do: 8
  defp estimate_term_size(value, _depth) when is_float(value), do: 8
  defp estimate_term_size(value, _depth) when is_boolean(value), do: 1
  defp estimate_term_size(nil, _depth), do: 1

  defp estimate_term_size(value, _depth) when is_atom(value), do: 4

  defp estimate_term_size(value, _depth) when is_struct(value) do
    _ = value
    32
  end

  defp estimate_term_size(value, depth) when is_map(value) do
    entries = Enum.take(value, @max_collection_sample)
    entry_count = length(entries)

    sampled_bytes =
      Enum.reduce(entries, 0, fn {k, v}, acc ->
        acc + estimate_term_size(k, depth + 1) + estimate_term_size(v, depth + 1)
      end)

    per_entry =
      sampled_bytes
      |> safe_div(max(entry_count, 1))
      |> min(64)
      |> max(8)

    16 + map_size(value) * per_entry
  end

  defp estimate_term_size(value, depth) when is_list(value) do
    {sampled_bytes, sampled_count, truncated?} =
      Enum.reduce_while(value, {0, 0, false}, fn item, {acc, count, _truncated?} ->
        if count < @max_collection_sample do
          {:cont, {acc + estimate_term_size(item, depth + 1), count + 1, false}}
        else
          {:halt, {acc, count, true}}
        end
      end)

    per_item =
      sampled_bytes
      |> safe_div(max(sampled_count, 1))
      |> min(64)
      |> max(4)

    base = 8 + sampled_count * per_item
    if truncated?, do: base + per_item * 8, else: base
  end

  defp estimate_term_size(value, depth) when is_tuple(value) do
    value
    |> Tuple.to_list()
    |> estimate_term_size(depth + 1)
    |> Kernel.+(8)
  end

  defp estimate_term_size(_value, _depth), do: 16

  defp safe_div(_value, 0), do: 0
  defp safe_div(value, divisor), do: div(value, divisor)

  defp monotonic_ms, do: System.monotonic_time(:millisecond)
end
