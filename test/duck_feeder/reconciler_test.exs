defmodule DuckFeeder.ReconcilerTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.Reconciler

  defmodule FakeMeta do
    def list_stale_batches(_conn, _opts) do
      {:ok,
       [
         %{batch_id: "b1", state: "uploaded"},
         %{batch_id: "b2", state: "failed"},
         %{batch_id: "b3", state: "uploaded"}
       ]}
    end

    def commit_uploaded_batch(_conn, "b1"), do: {:ok, %{batch_id: "b1"}}
    def commit_uploaded_batch(_conn, "b3"), do: {:error, :commit_failed}
  end

  defmodule FakeMetaCleanup do
    def list_stale_batches(_conn, _opts), do: {:ok, [%{batch_id: "b2", state: "failed"}]}

    def list_batch_files(_conn, "b2") do
      {:ok,
       [
         %{object_key: "raw/users/file-1.parquet"},
         %{object_key: "raw/users/file-2.parquet"}
       ]}
    end

    def transition_batch(_conn, "b2", :pending, error_message: nil),
      do: {:ok, %{batch_id: "b2", from: :failed, to: :pending}}
  end

  defmodule FakeMetaCleanupNoFiles do
    def list_stale_batches(_conn, _opts), do: {:ok, [%{batch_id: "b4", state: "failed"}]}
    def list_batch_files(_conn, "b4"), do: {:ok, []}

    def transition_batch(_conn, "b4", :pending, error_message: nil),
      do: {:ok, %{batch_id: "b4", from: :failed, to: :pending}}
  end

  defmodule FakeMetaEncoded do
    def list_stale_batches(_conn, _opts), do: {:ok, [%{batch_id: "b-encoded", state: "encoded"}]}

    def list_batch_files(_conn, "b-encoded") do
      {:ok,
       [
         %{object_key: "raw/users/file-encoded.parquet"}
       ]}
    end

    def transition_batch(_conn, "b-encoded", :pending, error_message: nil),
      do: {:ok, %{batch_id: "b-encoded", from: :encoded, to: :pending}}
  end

  defmodule FakeMetaVerify do
    def list_stale_batches(_conn, _opts), do: {:ok, [%{batch_id: "b1", state: "uploaded"}]}

    def list_batch_files(_conn, "b1") do
      {:ok,
       [
         %{object_key: "raw/users/file-1.parquet"},
         %{object_key: "raw/users/file-2.parquet"}
       ]}
    end

    def commit_uploaded_batch(_conn, "b1") do
      if pid = Process.get(:test_pid), do: send(pid, {:meta_commit_uploaded_batch, "b1"})
      {:ok, %{batch_id: "b1"}}
    end

    def transition_batch(_conn, "b1", :failed, opts) do
      if pid = Process.get(:test_pid), do: send(pid, {:meta_transition_failed, opts})
      {:ok, %{batch_id: "b1", from: :uploaded, to: :failed}}
    end
  end

  defmodule FakeStorage do
    def delete_object(_config, key) do
      if pid = Process.get(:test_pid), do: send(pid, {:storage_delete_object, key})
      :ok
    end

    def head_object(_config, key) do
      if pid = Process.get(:test_pid), do: send(pid, {:storage_head_object, key})
      {:ok, %{etag: "etag"}}
    end
  end

  defmodule FakeMetaStopOnError do
    def list_stale_batches(_conn, _opts) do
      {:ok, [%{batch_id: "e1", state: "uploaded"}, %{batch_id: "e2", state: "uploaded"}]}
    end

    def commit_uploaded_batch(_conn, "e1"), do: {:error, :first_failed}

    def commit_uploaded_batch(_conn, "e2") do
      if pid = Process.get(:test_pid), do: send(pid, {:meta_commit_uploaded_batch, "e2"})
      {:ok, %{batch_id: "e2"}}
    end
  end

  defmodule FakeStorageHeadFail do
    def head_object(_config, "raw/users/file-1.parquet") do
      if pid = Process.get(:test_pid),
        do: send(pid, {:storage_head_object, "raw/users/file-1.parquet"})

      {:ok, %{etag: "etag"}}
    end

    def head_object(_config, "raw/users/file-2.parquet") do
      if pid = Process.get(:test_pid),
        do: send(pid, {:storage_head_object, "raw/users/file-2.parquet"})

      {:error, :not_found}
    end

    def delete_object(_config, _key), do: :ok
  end

  setup do
    Process.put(:test_pid, self())
    :ok
  end

  test "reconciles stale batches" do
    assert {:ok, summary} =
             Reconciler.reconcile(%{meta_conn: :fake, meta_module: FakeMeta})

    assert summary.checked == 3
    assert summary.committed == ["b1"]
    assert summary.retried == []
    assert summary.skipped == ["b2"]
    assert summary.errors == [{"b3", :commit_failed}]
  end

  test "can cleanup failed uploaded files and retry failed batches" do
    assert {:ok, summary} =
             Reconciler.reconcile(
               %{
                 meta_conn: :fake,
                 meta_module: FakeMetaCleanup,
                 storage_module: FakeStorage,
                 storage: %{provider: :s3, bucket: "bucket"}
               },
               cleanup_failed_uploads?: true
             )

    assert summary.checked == 1
    assert summary.retried == ["b2"]
    assert summary.errors == []

    assert_received {:storage_delete_object, "raw/users/file-1.parquet"}
    assert_received {:storage_delete_object, "raw/users/file-2.parquet"}
  end

  test "retries encoded batches and cleans known files by default" do
    assert {:ok, summary} =
             Reconciler.reconcile(%{
               meta_conn: :fake,
               meta_module: FakeMetaEncoded,
               storage_module: FakeStorage,
               storage: %{provider: :s3, bucket: "bucket"}
             })

    assert summary.checked == 1
    assert summary.retried == ["b-encoded"]
    assert summary.errors == []

    assert_received {:storage_delete_object, "raw/users/file-encoded.parquet"}
  end

  test "returns error for encoded cleanup when storage is missing" do
    assert {:ok, summary} =
             Reconciler.reconcile(%{
               meta_conn: :fake,
               meta_module: FakeMetaEncoded
             })

    assert summary.checked == 1
    assert summary.retried == []
    assert summary.errors == [{"b-encoded", :missing_storage_for_encoded_cleanup}]
  end

  test "allows failed cleanup with no files by default" do
    assert {:ok, summary} =
             Reconciler.reconcile(
               %{
                 meta_conn: :fake,
                 meta_module: FakeMetaCleanupNoFiles,
                 storage_module: FakeStorage,
                 storage: %{provider: :s3, bucket: "bucket"}
               },
               cleanup_failed_uploads?: true
             )

    assert summary.checked == 1
    assert summary.retried == ["b4"]
    assert summary.errors == []
  end

  test "returns error when failed cleanup requires files and none are recorded" do
    assert {:ok, summary} =
             Reconciler.reconcile(
               %{
                 meta_conn: :fake,
                 meta_module: FakeMetaCleanupNoFiles,
                 storage_module: FakeStorage,
                 storage: %{provider: :s3, bucket: "bucket"}
               },
               cleanup_failed_uploads?: true,
               require_failed_batch_files?: true
             )

    assert summary.checked == 1
    assert summary.retried == []
    assert summary.errors == [{"b4", {:missing_batch_files, "b4"}}]
  end

  test "can verify uploaded objects before committing" do
    assert {:ok, summary} =
             Reconciler.reconcile(
               %{
                 meta_conn: :fake,
                 meta_module: FakeMetaVerify,
                 storage_module: FakeStorage,
                 storage: %{provider: :s3, bucket: "bucket"}
               },
               verify_uploaded_objects?: true
             )

    assert summary.checked == 1
    assert summary.committed == ["b1"]
    assert summary.errors == []

    assert_received {:storage_head_object, "raw/users/file-1.parquet"}
    assert_received {:storage_head_object, "raw/users/file-2.parquet"}
    assert_received {:meta_commit_uploaded_batch, "b1"}
  end

  test "marks uploaded batch failed when object verification fails" do
    assert {:ok, summary} =
             Reconciler.reconcile(
               %{
                 meta_conn: :fake,
                 meta_module: FakeMetaVerify,
                 storage_module: FakeStorageHeadFail,
                 storage: %{provider: :s3, bucket: "bucket"}
               },
               verify_uploaded_objects?: true
             )

    assert summary.checked == 1
    assert summary.committed == []

    assert summary.errors ==
             [{"b1", {:missing_uploaded_object, "raw/users/file-2.parquet", :not_found}}]

    assert_received {:storage_head_object, "raw/users/file-1.parquet"}
    assert_received {:storage_head_object, "raw/users/file-2.parquet"}
    assert_received {:meta_transition_failed, [error_message: _error_message]}

    refute_received {:meta_commit_uploaded_batch, "b1"}
  end

  test "limits reconcile run with max_batches" do
    assert {:ok, summary} =
             Reconciler.reconcile(%{meta_conn: :fake, meta_module: FakeMeta}, max_batches: 2)

    assert summary.checked == 2
    assert summary.committed == ["b1"]
    assert summary.skipped == ["b2"]
    assert summary.errors == []
  end

  test "can stop after first batch error" do
    assert {:ok, summary} =
             Reconciler.reconcile(
               %{meta_conn: :fake, meta_module: FakeMetaStopOnError},
               stop_on_error?: true
             )

    assert summary.checked == 1
    assert summary.errors == [{"e1", :first_failed}]
    assert summary.committed == []

    refute_received {:meta_commit_uploaded_batch, "e2"}
  end

  test "returns error for invalid max_batches" do
    assert {:error, {:invalid_max_batches, 0}} =
             Reconciler.reconcile(%{meta_conn: :fake, meta_module: FakeMeta}, max_batches: 0)
  end
end
