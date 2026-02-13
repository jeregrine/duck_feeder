defmodule DuckFeeder.BatchProcessorTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.BatchProcessor

  defmodule FakeMeta do
    def build_batch_id(_designated_table_id, _lsn_start, _lsn_end, _indexes), do: "batch-test"

    def insert_batch(_conn, attrs) do
      notify({:meta_insert_batch, attrs})
      Process.put({:designated_table_id, attrs.batch_id}, attrs.designated_table_id)
      {:ok, %{batch_id: attrs.batch_id, inserted?: true, state: :pending}}
    end

    def transition_batch(_conn, batch_id, to_state, opts \\ []) do
      notify({:meta_transition_batch, batch_id, to_state, opts})
      {:ok, %{batch_id: batch_id, from: :pending, to: to_state}}
    end

    def put_batch_file(_conn, attrs) do
      notify({:meta_put_batch_file, attrs})
      {:ok, 1}
    end

    def commit_uploaded_batch(_conn, batch_id) do
      notify({:meta_commit_uploaded_batch, batch_id})

      {:ok,
       %{
         batch_id: batch_id,
         designated_table_id: Process.get({:designated_table_id, batch_id}),
         checkpoint_lsn: "0/11"
       }}
    end

    def get_source_id_for_batch(batch_id), do: Process.get({:designated_table_id, batch_id})

    defp notify(message) do
      if pid = Process.get(:test_pid), do: send(pid, message)
    end
  end

  defmodule FakeWriter do
    @behaviour DuckFeeder.Writer.Adapter

    @impl true
    def write_batch(_config, %{rows: rows}, _opts) do
      if pid = Process.get(:test_pid), do: send(pid, {:writer_write_batch, rows})

      path =
        Path.join(
          System.tmp_dir!(),
          "duck_feeder_batch_processor_#{System.unique_integer([:positive])}.jsonl"
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
      if pid = Process.get(:test_pid), do: send(pid, {:writer_cleanup, path})
      File.rm(path)
      :ok
    end
  end

  defmodule FakeStorage do
    @behaviour DuckFeeder.Storage.Adapter

    @impl true
    def put_file(_config, _local_path, object_ref, _opts) do
      if pid = Process.get(:test_pid), do: send(pid, {:storage_put_file, object_ref})
      {:ok, %{etag: "etag-1", version_id: nil, size: 10}}
    end

    @impl true
    def head_object(_config, _object_ref), do: {:ok, %{}}

    @impl true
    def delete_object(_config, _object_ref), do: :ok
  end

  defmodule FakeStorageFail do
    @behaviour DuckFeeder.Storage.Adapter

    @impl true
    def put_file(_config, _local_path, _object_ref, _opts), do: {:error, :upload_failed}

    @impl true
    def head_object(_config, _object_ref), do: {:ok, %{}}

    @impl true
    def delete_object(_config, _object_ref), do: :ok
  end

  defmodule FakeCommitter do
    @behaviour DuckFeeder.DuckLake.Committer

    @impl true
    def commit_batch(_meta_conn, batch_id, opts) do
      if pid = Process.get(:test_pid), do: send(pid, {:committer_commit_batch, batch_id, opts})

      {:ok,
       %{
         batch_id: batch_id,
         designated_table_id: opts[:meta_module] |> apply(:get_source_id_for_batch, [batch_id]),
         checkpoint_lsn: "0/11"
       }}
    end
  end

  setup do
    Process.put(:test_pid, self())
    :ok
  end

  test "processes batch end-to-end" do
    context = %{
      meta_module: FakeMeta,
      meta_conn: :fake_conn,
      designated_table_by_target: %{{"raw", "users"} => 7},
      writer: %{adapter: FakeWriter},
      storage: %{provider: :s3, bucket: "bucket", adapter: FakeStorage},
      object_prefix: "cdc"
    }

    batch = %{rows: [%{"id" => 1}], lsn_start: "0/10", lsn_end: "0/11", row_count: 1}

    assert {:ok, result} = BatchProcessor.process_batch(context, {"raw", "users"}, batch)

    assert result.status == :committed
    assert result.batch_id == "batch-test"
    assert result.designated_table_id == 7
    assert result.object_key =~ "cdc/raw.users/"

    assert_received {:meta_insert_batch, _}
    assert_received {:meta_transition_batch, "batch-test", :encoded, []}
    assert_received {:storage_put_file, %{bucket: "bucket"}}
    assert_received {:meta_put_batch_file, _}
    assert_received {:meta_transition_batch, "batch-test", :uploaded, []}
    assert_received {:meta_commit_uploaded_batch, "batch-test"}
    assert_received {:writer_cleanup, _path}
  end

  test "supports custom committer module" do
    context = %{
      meta_module: FakeMeta,
      meta_conn: :fake_conn,
      designated_table_by_target: %{{"raw", "users"} => 7},
      writer: %{adapter: FakeWriter},
      storage: %{provider: :s3, bucket: "bucket", adapter: FakeStorage},
      committer_module: FakeCommitter,
      object_prefix: "cdc"
    }

    batch = %{rows: [%{"id" => 1}], lsn_start: "0/10", lsn_end: "0/11", row_count: 1}

    assert {:ok, result} = BatchProcessor.process_batch(context, {"raw", "users"}, batch)
    assert result.status == :committed

    assert_received {:committer_commit_batch, "batch-test", opts}
    assert opts[:meta_module] == FakeMeta

    refute_received {:meta_commit_uploaded_batch, "batch-test"}
  end

  test "marks batch failed when upload fails" do
    context = %{
      meta_module: FakeMeta,
      meta_conn: :fake_conn,
      designated_table_by_target: %{{"raw", "users"} => 7},
      writer: %{adapter: FakeWriter},
      storage: %{provider: :s3, bucket: "bucket", adapter: FakeStorageFail},
      object_prefix: "cdc"
    }

    batch = %{rows: [%{"id" => 1}], lsn_start: "0/10", lsn_end: "0/11", row_count: 1}

    assert {:error, :upload_failed} =
             BatchProcessor.process_batch(context, {"raw", "users"}, batch)

    assert_received {:meta_transition_batch, "batch-test", :failed, opts}
    assert Keyword.get(opts, :error_message)
    assert_received {:writer_cleanup, _path}
  end

  test "returns error for unknown designated table target" do
    context = %{
      meta_module: FakeMeta,
      meta_conn: :fake_conn,
      designated_table_by_target: %{},
      writer: %{adapter: FakeWriter},
      storage: %{provider: :s3, bucket: "bucket", adapter: FakeStorage},
      object_prefix: "cdc"
    }

    batch = %{rows: [], lsn_start: "0/10", lsn_end: "0/11", row_count: 0}

    assert {:error, {:unknown_target_table, {"raw", "users"}}} =
             BatchProcessor.process_batch(context, {"raw", "users"}, batch)
  end
end
