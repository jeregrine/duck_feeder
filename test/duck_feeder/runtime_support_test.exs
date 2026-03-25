defmodule DuckFeeder.RuntimeSupportTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.{Meta, RuntimeSupport}

  test "resolve_common_init defaults meta_module to DuckFeeder.Meta" do
    designated_tables = [
      %{target_schema: "raw", target_table: "events"}
    ]

    assert {:ok, common} =
             RuntimeSupport.resolve_common_init(
               designated_tables,
               [meta_conn: self(), duckdb: %{conn: self()}],
               checkpoint_prefix: "duck_feeder_append"
             )

    assert common.context.meta_module == Meta
  end
end
