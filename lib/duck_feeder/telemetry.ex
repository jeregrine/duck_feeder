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

  @spec cdc_event(atom() | String.t(), :buffering | :committed | :error) :: :ok
  def cdc_event(event_type, status) do
    execute(
      [:cdc, :event],
      %{count: 1},
      %{event_type: event_type, status: status}
    )
  end

  @spec cdc_connection(atom(), map()) :: :ok
  def cdc_connection(status, metadata \\ %{}) when is_atom(status) and is_map(metadata) do
    execute(
      [:cdc, :connection],
      %{count: 1},
      Map.put(metadata, :status, status)
    )
  end

  @spec cdc_frame(atom(), atom(), map()) :: :ok
  def cdc_frame(frame_type, outcome, metadata \\ %{})
      when is_atom(frame_type) and is_atom(outcome) and is_map(metadata) do
    execute(
      [:cdc, :frame],
      %{count: 1},
      metadata
      |> Map.put(:frame_type, frame_type)
      |> Map.put(:outcome, outcome)
    )
  end

  @spec cdc_lag(map(), map()) :: :ok
  def cdc_lag(measurements, metadata \\ %{}) when is_map(measurements) and is_map(metadata) do
    execute(
      [:cdc, :lag],
      Map.put_new(measurements, :count, 1),
      metadata
    )
  end

  @spec cdc_backpressure(map(), map()) :: :ok
  def cdc_backpressure(measurements, metadata \\ %{})
      when is_map(measurements) and is_map(metadata) do
    execute(
      [:cdc, :backpressure],
      Map.put_new(measurements, :count, 1),
      metadata
    )
  end

  @spec service_batch_queue(map(), map()) :: :ok
  def service_batch_queue(measurements, metadata \\ %{})
      when is_map(measurements) and is_map(metadata) do
    execute(
      [:service, :batch_queue],
      Map.put_new(measurements, :count, 1),
      metadata
    )
  end

  @spec append_stream_batch_queue(map(), map()) :: :ok
  def append_stream_batch_queue(measurements, metadata \\ %{})
      when is_map(measurements) and is_map(metadata) do
    execute(
      [:append_stream, :batch_queue],
      Map.put_new(measurements, :count, 1),
      metadata
    )
  end

  @spec append_stream_batch_dropped(map(), map()) :: :ok
  def append_stream_batch_dropped(measurements, metadata \\ %{})
      when is_map(measurements) and is_map(metadata) do
    execute(
      [:append_stream, :batch_dropped],
      Map.put_new(measurements, :count, 1),
      metadata
    )
  end

  @spec service_ack_checkpoint_lag(map(), map()) :: :ok
  def service_ack_checkpoint_lag(measurements, metadata \\ %{})
      when is_map(measurements) and is_map(metadata) do
    execute(
      [:service, :ack_checkpoint_lag],
      Map.put_new(measurements, :count, 1),
      metadata
    )
  end
end
