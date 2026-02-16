defmodule DuckFeeder.TelemetryTest do
  use ExUnit.Case, async: false

  alias DuckFeeder.{AppendStream, BatchProcessor, Ingest, Service, TablePipeline}
  alias DuckFeeder.CDC.{Event, Pipeline}

  defmodule QueueMeta do
    def build_batch_id(_designated_table_id, _lsn_start, _lsn_end, _indexes),
      do: "batch-telemetry"

    def insert_batch(_conn, attrs) do
      Process.put({:designated_table_id, attrs.batch_id}, attrs.designated_table_id)
      {:ok, %{batch_id: attrs.batch_id, inserted?: true, state: :pending}}
    end

    def transition_batch(_conn, batch_id, to_state, _opts \\ []),
      do: {:ok, %{batch_id: batch_id, from: :pending, to: to_state}}

    def put_batch_file(_conn, _attrs), do: {:ok, 1}

    def commit_uploaded_batch(_conn, batch_id) do
      {:ok,
       %{
         batch_id: batch_id,
         designated_table_id: Process.get({:designated_table_id, batch_id}),
         checkpoint_lsn: "0/120"
       }}
    end
  end

  defmodule QueueWriter do
    @behaviour DuckFeeder.Writer.Adapter

    @impl true
    def write_batch(_config, %{rows: rows}, _opts) do
      path =
        Path.join(
          System.tmp_dir!(),
          "duck_feeder_telemetry_#{System.unique_integer([:positive])}.jsonl"
        )

      File.write!(path, "{}\n")

      {:ok,
       %{
         local_path: path,
         row_count: length(rows),
         file_size_bytes: File.stat!(path).size,
         format: :jsonl
       }}
    end

    @impl true
    def cleanup(_config, %{local_path: path}) do
      _ = File.rm(path)
      :ok
    end
  end

  defmodule SlowStorage do
    @behaviour DuckFeeder.Storage.Adapter

    @impl true
    def put_file(_config, _local_path, _object_ref, _opts) do
      Process.sleep(250)
      {:ok, %{etag: "etag-telemetry", version_id: nil, size: 11}}
    end

    @impl true
    def head_object(_config, _object_ref), do: {:ok, %{}}

    @impl true
    def delete_object(_config, _object_ref), do: :ok
  end

  defmodule FastStorage do
    @behaviour DuckFeeder.Storage.Adapter

    @impl true
    def put_file(_config, _local_path, _object_ref, _opts),
      do: {:ok, %{etag: "etag-fast", version_id: nil, size: 11}}

    @impl true
    def head_object(_config, _object_ref), do: {:ok, %{}}

    @impl true
    def delete_object(_config, _object_ref), do: :ok
  end

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

  test "emits service/append queue telemetry and service ack checkpoint lag" do
    handler_id = "duck-feeder-test-queue-telemetry-#{System.unique_integer([:positive])}"

    :ok =
      :telemetry.attach_many(
        handler_id,
        [
          [:duck_feeder, :service, :batch_queue],
          [:duck_feeder, :append_stream, :batch_queue],
          [:duck_feeder, :service, :ack_checkpoint_lag]
        ],
        &__MODULE__.handle_event/4,
        self()
      )

    on_exit(fn ->
      :telemetry.detach(handler_id)
    end)

    designated_tables = [
      %{
        id: 1,
        source_schema: "public",
        source_table: "events",
        target_schema: "raw",
        target_table: "events"
      }
    ]

    {:ok, service} =
      Service.start_link(
        designated_tables: designated_tables,
        meta_module: QueueMeta,
        meta_conn: :fake,
        writer: %{adapter: QueueWriter},
        storage: %{provider: :s3, bucket: "bucket", adapter: FastStorage},
        observer_pid: self(),
        max_inflight_batches: 1,
        max_pending_batches: 1
      )

    assert :ok = Service.attach_cdc(service, self())

    send(service, {
      :duck_feeder_batch,
      {"raw", "events"},
      %{rows: [%{"id" => "1"}], row_count: 1, lsn_start: "0/100", lsn_end: "0/120"}
    })

    assert_receive {:telemetry, [:duck_feeder, :service, :batch_queue], queue_measurements,
                    queue_metadata},
                   1_000

    assert queue_measurements.max_pending_batches == 1
    assert queue_metadata.status in [:started, :completed]

    assert_receive {:telemetry, [:duck_feeder, :service, :ack_checkpoint_lag], ack_measurements,
                    ack_metadata},
                   1_000

    assert ack_measurements.lag_known == 1
    assert ack_measurements.lag_bytes == 0
    assert ack_metadata.status == :ok
    assert ack_metadata.checkpoint_lsn == "0/120"

    GenServer.stop(service)

    previous = Process.flag(:trap_exit, true)

    on_exit(fn ->
      Process.flag(:trap_exit, previous)
    end)

    {:ok, stream} =
      AppendStream.start_link(
        designated_tables: [%{id: 1, target_schema: "raw", target_table: "events"}],
        meta_module: QueueMeta,
        meta_conn: :fake,
        writer: %{adapter: QueueWriter},
        storage: %{provider: :s3, bucket: "bucket", adapter: SlowStorage},
        observer_pid: self(),
        max_inflight_batches: 1,
        max_pending_batches: 1
      )

    batch = %{rows: [%{"kind" => "a"}], row_count: 1, lsn_start: "0/1", lsn_end: "0/1"}

    send(stream, {:duck_feeder_batch, {"raw", "events"}, batch})

    send(
      stream,
      {:duck_feeder_batch, {"raw", "events"}, %{batch | lsn_start: "0/2", lsn_end: "0/2"}}
    )

    send(
      stream,
      {:duck_feeder_batch, {"raw", "events"}, %{batch | lsn_start: "0/3", lsn_end: "0/3"}}
    )

    assert_receive {:telemetry, [:duck_feeder, :append_stream, :batch_queue], append_measurements,
                    append_metadata},
                   1_000

    assert append_measurements.max_pending_batches == 1

    assert append_metadata.status in [:started, :enqueued, :overflow, :completed]

    assert_receive {:telemetry, [:duck_feeder, :append_stream, :batch_queue], _measurements,
                    %{status: :overflow}},
                   1_000

    assert_receive {:EXIT, ^stream, {:batch_queue_overflow, 1}}, 1_000
  end

  test "emits append stream batch dropped telemetry" do
    handler_id = "duck-feeder-test-append-dropped-#{System.unique_integer([:positive])}"

    :ok =
      :telemetry.attach(
        handler_id,
        [:duck_feeder, :append_stream, :batch_dropped],
        &__MODULE__.handle_event/4,
        self()
      )

    on_exit(fn ->
      :telemetry.detach(handler_id)
    end)

    DuckFeeder.Telemetry.append_stream_batch_dropped(%{dropped_count: 1}, %{
      table: {"raw", "events"},
      reason: :drop_oldest
    })

    assert_receive {:telemetry, [:duck_feeder, :append_stream, :batch_dropped], measurements,
                    metadata},
                   300

    assert measurements.count == 1
    assert measurements.dropped_count == 1
    assert metadata.reason == :drop_oldest
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
