defmodule DuckFeeder.Runtime.BuildRuntimeSourceTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.Runtime

  test "build_runtime_source preserves validated source fields while adding runtime connection info" do
    source = %{
      postgres_url: "postgres://source",
      slot_name: "slot-a",
      publication_name: "pub-a",
      designated_tables: [%{target_schema: "raw", target_table: "users"}],
      custom: :keep_me
    }

    built = Runtime.build_runtime_source("source-a", source)

    assert built.name == "source-a"
    assert built.postgres_url == "postgres://source"
    assert built.connection_info == %{postgres_url: "postgres://source"}
    assert built.snapshot_handoff_source_key == "source-a"
    assert built.designated_tables == [%{target_schema: "raw", target_table: "users"}]
    assert built.custom == :keep_me
  end
end
