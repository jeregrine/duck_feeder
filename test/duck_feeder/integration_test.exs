defmodule DuckFeeder.IntegrationTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.Integration

  test "builds runtime supervisor child spec" do
    storage = %{provider: :s3, bucket: "bucket", adapter: DuckFeeder.Storage.S3}

    child_spec =
      Integration.runtime_child_spec(:meta_conn, "source-a", storage,
        name: :duck_runtime,
        start_reconciler?: true,
        runtime_opts: [bootstrap_replication?: false]
      )

    assert child_spec.id == DuckFeeder.Runtime.Supervisor
    assert {DuckFeeder.Runtime.Supervisor, :start_link, [opts]} = child_spec.start
    assert opts[:name] == :duck_runtime
    assert opts[:meta_conn] == :meta_conn
    assert opts[:source_name] == "source-a"
    assert opts[:start_reconciler?] == true
  end

  test "builds child spec from runtime config" do
    config = %{
      source: %{
        postgres_url: "postgres://source",
        slot_name: "duck_slot",
        publication_name: "duck_pub",
        designated_tables: []
      },
      storage: %{
        provider: :s3,
        bucket: "bucket",
        access_key_id: "key",
        secret_access_key: "secret"
      },
      metadata: %{postgres_url: "postgres://meta"}
    }

    assert {:ok, child_spec} =
             Integration.runtime_child_spec_from_config(:meta_conn, config,
               source_name: "source-a",
               start_reconciler?: true
             )

    assert {DuckFeeder.Runtime.Supervisor, :start_link, [opts]} = child_spec.start
    assert opts[:source_name] == "source-a"
    assert opts[:storage_config][:provider] == :s3
    assert opts[:storage_config][:bucket] == "bucket"
    assert opts[:start_reconciler?] == true
  end
end
