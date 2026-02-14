defmodule DuckFeeder.AppendStreamTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.AppendStream

  defmodule FakeMeta do
    def build_batch_id(_designated_table_id, _lsn_start, _lsn_end, _indexes), do: "batch-append"

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

  defmodule FakeWriter do
    @behaviour DuckFeeder.Writer.Adapter

    @impl true
    def write_batch(_config, %{rows: rows}, _opts) do
      path =
        Path.join(
          System.tmp_dir!(),
          "duck_feeder_append_#{System.unique_integer([:positive])}.jsonl"
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

  defmodule FakeStorage do
    @behaviour DuckFeeder.Storage.Adapter

    @impl true
    def put_file(_config, _local_path, _object_ref, _opts),
      do: {:ok, %{etag: "etag-append", version_id: nil, size: 11}}

    @impl true
    def head_object(_config, _object_ref), do: {:ok, %{}}

    @impl true
    def delete_object(_config, _object_ref), do: :ok
  end

  test "appends event rows and processes batch through writer/upload/commit" do
    designated_tables = [
      %{id: 1, target_schema: "raw", target_table: "events"}
    ]

    {:ok, stream} =
      AppendStream.start_link(
        designated_tables: designated_tables,
        meta_module: FakeMeta,
        meta_conn: :fake_conn,
        writer: %{adapter: FakeWriter},
        storage: %{provider: :s3, bucket: "bucket", adapter: FakeStorage},
        pipeline_opts: %{max_rows: 1, max_bytes: 10_000, flush_interval_ms: 60_000},
        observer_pid: self()
      )

    assert :ok = AppendStream.append(stream, "events", %{"kind" => "telemetry", "value" => 1})

    assert_receive {:duck_feeder_append_batch_processed, {"raw", "events"}, {:ok, result}, batch},
                   1_000

    assert result.status == :committed
    assert batch.row_count == 1
  end

  test "returns error for unknown target table" do
    {:ok, stream} =
      AppendStream.start_link(
        designated_tables: [%{id: 1, target_schema: "raw", target_table: "events"}],
        meta_module: FakeMeta,
        meta_conn: :fake_conn,
        writer: %{adapter: FakeWriter},
        storage: %{provider: :s3, bucket: "bucket", adapter: FakeStorage}
      )

    assert {:error, {:unknown_target_table, {"raw", "missing"}}} =
             AppendStream.append(stream, "missing", %{"kind" => "log"})
  end

  test "supports explicit flush for append stream table" do
    {:ok, stream} =
      AppendStream.start_link(
        designated_tables: [%{id: 1, target_schema: "raw", target_table: "events"}],
        meta_module: FakeMeta,
        meta_conn: :fake_conn,
        writer: %{adapter: FakeWriter},
        storage: %{provider: :s3, bucket: "bucket", adapter: FakeStorage},
        pipeline_opts: %{max_rows: 100, max_bytes: 10_000, flush_interval_ms: 60_000},
        observer_pid: self()
      )

    assert :ok = AppendStream.append(stream, "events", %{"kind" => "error", "message" => "boom"})
    assert {:ok, batch} = AppendStream.flush_table(stream, "events")
    assert batch.row_count == 1

    assert_receive {:duck_feeder_append_batch_processed, {"raw", "events"}, {:ok, _result},
                    _batch},
                   1_000
  end
end
