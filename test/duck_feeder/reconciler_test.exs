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
end
