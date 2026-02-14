defmodule DuckFeeder.TelemetryTest do
  use ExUnit.Case, async: false

  alias DuckFeeder.{BatchProcessor, Ingest, TablePipeline}
  alias DuckFeeder.CDC.{Event, Pipeline}

  test "emits batch flushed telemetry" do
    handler_id = "duck-feeder-test-batch-flushed-#{System.unique_integer([:positive])}"

    :ok =
      :telemetry.attach(
        handler_id,
        [:duck_feeder, :batch, :flushed],
        &__MODULE__.handle_event/4,
        self()
      )

    on_exit(fn ->
      :telemetry.detach(handler_id)
    end)

    {:ok, pipeline} =
      TablePipeline.start_link(
        table: {"raw", "users"},
        max_rows: 1,
        max_bytes: 10_000,
        flush_interval_ms: 60_000,
        sink_pid: self()
      )

    :ok = TablePipeline.append(pipeline, %{"id" => 1}, "0/10")

    assert_receive {:telemetry, [:duck_feeder, :batch, :flushed], measurements, metadata}, 300
    assert measurements.row_count == 1
    assert metadata.schema == "raw"
    assert metadata.table == "users"
  end

  test "emits cdc event telemetry" do
    handler_id = "duck-feeder-test-cdc-event-#{System.unique_integer([:positive])}"

    :ok =
      :telemetry.attach(
        handler_id,
        [:duck_feeder, :cdc, :event],
        &__MODULE__.handle_event/4,
        self()
      )

    on_exit(fn ->
      :telemetry.detach(handler_id)
    end)

    {:ok, ingest} =
      Ingest.start_link(
        designated_tables: [],
        sink_pid: self(),
        pipeline_opts: %{max_rows: 10, max_bytes: 10_000, flush_interval_ms: 60_000}
      )

    {:ok, cdc_pipeline} = Pipeline.start_link(ingest_pid: ingest)

    assert :buffering =
             Pipeline.push_event(
               cdc_pipeline,
               %Event.Relation{id: 1, schema: "public", table: "users"}
             )

    assert_receive {:telemetry, [:duck_feeder, :cdc, :event], %{count: 1}, metadata}, 300
    assert metadata.status == :buffering
  end

  test "emits batch processed telemetry" do
    handler_id = "duck-feeder-test-batch-processed-#{System.unique_integer([:positive])}"

    :ok =
      :telemetry.attach(
        handler_id,
        [:duck_feeder, :batch, :processed],
        &__MODULE__.handle_event/4,
        self()
      )

    on_exit(fn ->
      :telemetry.detach(handler_id)
    end)

    context = %{
      meta_conn: :fake,
      designated_table_by_target: %{},
      writer: %{},
      storage: %{provider: :s3, bucket: "bucket"}
    }

    assert {:error, {:unknown_target_table, {"raw", "users"}}} =
             BatchProcessor.process_batch(
               context,
               {"raw", "users"},
               %{rows: [], lsn_start: "0/1", lsn_end: "0/2"}
             )

    assert_receive {:telemetry, [:duck_feeder, :batch, :processed], %{error: 1, success: 0},
                    metadata},
                   300

    assert metadata.schema == "raw"
    assert metadata.table == "users"
  end

  test "emits cdc connection/frame/lag/backpressure telemetry" do
    handler_id = "duck-feeder-test-cdc-conn-#{System.unique_integer([:positive])}"

    :ok =
      :telemetry.attach_many(
        handler_id,
        [
          [:duck_feeder, :cdc, :connection],
          [:duck_feeder, :cdc, :frame],
          [:duck_feeder, :cdc, :lag],
          [:duck_feeder, :cdc, :backpressure]
        ],
        &__MODULE__.handle_event/4,
        self()
      )

    on_exit(fn ->
      :telemetry.detach(handler_id)
    end)

    DuckFeeder.Telemetry.cdc_connection(:stream_starting, %{slot_name: "slot"})
    DuckFeeder.Telemetry.cdc_frame(:keepalive, :ack_requested, %{wal_end: 1})
    DuckFeeder.Telemetry.cdc_lag(%{lag_bytes: 42}, %{source: :test})

    DuckFeeder.Telemetry.cdc_backpressure(%{lag_bytes: 84, threshold_bytes: 40}, %{
      status: :entered
    })

    assert_receive {:telemetry, [:duck_feeder, :cdc, :connection], %{count: 1}, metadata}, 300
    assert metadata.status == :stream_starting
    assert metadata.slot_name == "slot"

    assert_receive {:telemetry, [:duck_feeder, :cdc, :frame], %{count: 1}, frame_meta}, 300
    assert frame_meta.frame_type == :keepalive
    assert frame_meta.outcome == :ack_requested

    assert_receive {:telemetry, [:duck_feeder, :cdc, :lag], %{count: 1, lag_bytes: 42}, lag_meta},
                   300

    assert lag_meta.source == :test

    assert_receive {:telemetry, [:duck_feeder, :cdc, :backpressure], backpressure_measurements,
                    backpressure_meta},
                   300

    assert backpressure_measurements.threshold_bytes == 40
    assert backpressure_meta.status == :entered
  end

  test "emits reconciler run telemetry" do
    handler_id = "duck-feeder-test-reconciler-run-#{System.unique_integer([:positive])}"

    :ok =
      :telemetry.attach(
        handler_id,
        [:duck_feeder, :reconciler, :run],
        &__MODULE__.handle_event/4,
        self()
      )

    on_exit(fn ->
      :telemetry.detach(handler_id)
    end)

    DuckFeeder.Telemetry.reconciler_run(
      {:ok, %{checked: 3, committed: ["b1"], skipped: [], errors: []}}
    )

    assert_receive {:telemetry, [:duck_feeder, :reconciler, :run], measurements, metadata}, 300
    assert measurements.checked == 3
    assert measurements.committed == 1
    assert metadata.status == :ok
  end

  def handle_event(event, measurements, metadata, pid) do
    send(pid, {:telemetry, event, measurements, metadata})
  end
end
