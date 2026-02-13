defmodule DuckFeeder.Telemetry do
  @moduledoc """
  Telemetry emission helpers.
  """

  @prefix [:duck_feeder]

  @spec execute([atom()], map(), map()) :: :ok
  def execute(event_suffix, measurements, metadata)
      when is_list(event_suffix) and is_map(measurements) and is_map(metadata) do
    :telemetry.execute(@prefix ++ event_suffix, measurements, metadata)
  end

  @spec batch_flushed({String.t(), String.t()}, map()) :: :ok
  def batch_flushed({schema, table}, batch) do
    execute(
      [:batch, :flushed],
      %{row_count: Map.get(batch, :row_count, 0), byte_count: Map.get(batch, :byte_count, 0)},
      %{schema: schema, table: table, lsn_start: batch[:lsn_start], lsn_end: batch[:lsn_end]}
    )
  end

  @spec batch_processed({String.t(), String.t()}, {:ok, map()} | {:error, term()}) :: :ok
  def batch_processed({schema, table}, {:ok, result}) do
    execute(
      [:batch, :processed],
      %{success: 1, error: 0},
      %{schema: schema, table: table, result: result}
    )
  end

  def batch_processed({schema, table}, {:error, reason}) do
    execute(
      [:batch, :processed],
      %{success: 0, error: 1},
      %{schema: schema, table: table, reason: reason}
    )
  end

  @spec cdc_event(atom() | String.t(), :buffering | :committed | :error) :: :ok
  def cdc_event(event_type, status) do
    execute(
      [:cdc, :event],
      %{count: 1},
      %{event_type: event_type, status: status}
    )
  end
end
