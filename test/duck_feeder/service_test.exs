defmodule DuckFeeder.ServiceTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.CDC.Event
  alias DuckFeeder.Service

  defmodule FakeMeta do
    def build_batch_id(_designated_table_id, _lsn_start, _lsn_end, _indexes), do: "batch-service"

    def insert_batch(_conn, attrs) do
      Process.put({:designated_table_id, attrs.batch_id}, attrs.designated_table_id)
      {:ok, %{batch_id: attrs.batch_id, inserted?: true, state: :pending}}
    end

    def transition_batch(_conn, batch_id, to_state, _opts \\ []) do
      {:ok, %{batch_id: batch_id, from: :pending, to: to_state}}
    end

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
          "duck_feeder_service_#{System.unique_integer([:positive])}.jsonl"
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
      do: {:ok, %{etag: "etag-service", version_id: nil, size: 11}}

    @impl true
    def head_object(_config, _object_ref), do: {:ok, %{}}

    @impl true
    def delete_object(_config, _object_ref), do: :ok
  end

  test "runs end-to-end from CDC event to processed batch" do
    designated_tables = [
      %{
        id: 1,
        source_schema: "public",
        source_table: "users",
        target_schema: "raw",
        target_table: "users"
      }
    ]

    {:ok, service} =
      Service.start_link(
        designated_tables: designated_tables,
        meta_module: FakeMeta,
        meta_conn: :fake_conn,
        writer: %{adapter: FakeWriter},
        storage: %{provider: :s3, bucket: "bucket", adapter: FakeStorage},
        pipeline_opts: %{max_rows: 1, max_bytes: 10_000, flush_interval_ms: 60_000},
        observer_pid: self()
      )

    assert :buffering =
             Service.push_event(service, %Event.Relation{id: 1, schema: "public", table: "users"})

    assert :buffering =
             Service.push_event(service, %Event.Begin{xid: 700, final_lsn: "0/100"})

    assert :buffering =
             Service.push_event(service, %Event.Insert{relation_id: 1, record: %{"id" => "1"}})

    assert {:committed, %{xid: 700}} =
             Service.push_event(service, %Event.Commit{xid: 700, end_lsn: "0/120"})

    assert_receive {:duck_feeder_batch_processed, {"raw", "users"}, {:ok, result}, batch}, 1_000

    assert result.status == :committed
    assert result.batch_id == "batch-service"
    assert batch.row_count == 1

    refute Service.in_transaction?(service)
  end

  test "returns CDC validation errors" do
    {:ok, service} =
      Service.start_link(
        designated_tables: [],
        meta_module: FakeMeta,
        meta_conn: :fake_conn,
        writer: %{adapter: FakeWriter},
        storage: %{provider: :s3, bucket: "bucket", adapter: FakeStorage},
        observer_pid: self()
      )

    assert :buffering =
             Service.push_event(service, %Event.Relation{id: 1, schema: "public", table: "users"})

    assert {:error, :change_outside_transaction} =
             Service.push_event(service, %Event.Insert{relation_id: 1, record: %{}})
  end
end
