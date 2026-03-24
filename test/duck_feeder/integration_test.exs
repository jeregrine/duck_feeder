defmodule DuckFeeder.IntegrationTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.Integration

  test "builds runtime supervisor child spec" do
    duckdb = %{path: "/tmp/source-a.duckdb"}

    child_spec =
      Integration.runtime_child_spec(:meta_conn, "source-a", duckdb,
        name: :duck_runtime,
        runtime_opts: [bootstrap_replication?: false]
      )

    assert child_spec.id == DuckFeeder.Runtime.Supervisor
    assert {DuckFeeder.Runtime.Supervisor, :start_link, [opts]} = child_spec.start
    assert opts[:name] == :duck_runtime
    assert opts[:meta_conn] == :meta_conn
    assert opts[:source_name] == "source-a"
    assert opts[:duckdb] == duckdb
  end

  test "builds child spec from runtime config" do
    config = %{
      source: %{
        postgres_url: "postgres://source",
        slot_name: "duck_slot",
        publication_name: "duck_pub",
        designated_tables: []
      },
      duckdb: %{
        path: "/tmp/source-a.duckdb",
        catalog: "lake"
      },
      metadata: %{postgres_url: "postgres://meta"}
    }

    assert {:ok, child_spec} =
             Integration.runtime_child_spec_from_config(:meta_conn, config,
               source_name: "source-a"
             )

    assert {DuckFeeder.Runtime.Supervisor, :start_link, [opts]} = child_spec.start
    assert opts[:source_name] == "source-a"
    assert opts[:duckdb][:path] == "/tmp/source-a.duckdb"
    assert opts[:duckdb][:catalog] == "lake"
  end
end
