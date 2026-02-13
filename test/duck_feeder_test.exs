defmodule DuckFeederTest do
  use ExUnit.Case, async: true

  defmodule FakeAdapter do
    @behaviour DuckFeeder.Storage.Adapter

    @impl true
    def put_file(_config, local_path, object_ref, _opts) do
      {:ok,
       %{
         etag: object_ref.key,
         version_id: nil,
         size: byte_size(local_path)
       }}
    end

    @impl true
    def head_object(_config, object_ref), do: {:ok, object_ref}

    @impl true
    def delete_object(_config, _object_ref), do: :ok
  end

  test "builds object key with prefix" do
    config = %{provider: :s3, bucket: "bucket", prefix: "/prod/events/"}

    assert {:ok, %{bucket: "bucket", key: "prod/events/table=users/part-1.parquet"}} =
             DuckFeeder.Storage.object_ref(config, "/table=users/part-1.parquet")
  end

  test "dispatches through adapter override" do
    config = %{provider: :gcs, bucket: "bucket", prefix: "p", adapter: FakeAdapter}

    assert {:ok, %{etag: "p/a.parquet"}} = DuckFeeder.put_file(config, "/tmp/file", "a.parquet")

    assert {:ok, %{bucket: "bucket", key: "p/a.parquet"}} =
             DuckFeeder.head_object(config, "a.parquet")

    assert :ok = DuckFeeder.delete_object(config, "a.parquet")
  end

  test "returns unsupported provider" do
    config = %{provider: :azure, bucket: "bucket"}

    assert {:error, {:unsupported_provider, :azure}} =
             DuckFeeder.put_file(config, "/tmp/file", "a.parquet")
  end
end
