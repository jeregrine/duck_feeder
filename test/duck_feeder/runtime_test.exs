defmodule DuckFeeder.RuntimeTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.Runtime

  defmodule FakeMeta do
    def get_source(_conn, "source-a") do
      {:ok, %{id: 10, name: "source-a"}}
    end

    def get_source(_conn, other), do: {:error, {:source_not_found, other}}

    def list_designated_tables(_conn, source_id: 10) do
      {:ok,
       [
         %{
           id: 1,
           source_id: 10,
           source_schema: "public",
           source_table: "users",
           target_schema: "raw",
           target_table: "users",
           mode: "cdc_changelog",
           primary_keys: ["id"],
           partition_config: %{}
         }
       ]}
    end

    def list_designated_tables(_conn, _opts), do: {:ok, []}
  end

  test "builds service options from metadata" do
    storage = %{provider: :s3, bucket: "bucket", adapter: DuckFeeder.Storage.S3}

    assert {:ok, opts} =
             Runtime.service_options(:meta_conn, "source-a", storage,
               meta_module: FakeMeta,
               observer_pid: self(),
               object_prefix: "prefix"
             )

    assert opts[:designated_tables] != []
    assert opts[:storage] == storage
    assert opts[:object_prefix] == "prefix"
    assert opts[:meta_module] == FakeMeta
  end

  test "starts service from metadata" do
    storage = %{provider: :s3, bucket: "bucket", adapter: DuckFeeder.Storage.S3}

    assert {:ok, pid} =
             Runtime.start_service(:meta_conn, "source-a", storage,
               meta_module: FakeMeta,
               name: nil,
               observer_pid: self()
             )

    assert is_pid(pid)
    assert Process.alive?(pid)

    GenServer.stop(pid)
  end

  test "returns error when source is missing" do
    storage = %{provider: :s3, bucket: "bucket", adapter: DuckFeeder.Storage.S3}

    assert {:error, {:source_not_found, "missing"}} =
             Runtime.service_options(:meta_conn, "missing", storage, meta_module: FakeMeta)
  end
end
