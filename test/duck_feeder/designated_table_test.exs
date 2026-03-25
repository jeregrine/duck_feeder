defmodule DuckFeeder.DesignatedTableTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.DesignatedTable

  test "by_target normalizes string-key designated tables and assigns checkpoint keys" do
    designated_tables = [
      %{
        "source_schema" => "public",
        "source_table" => "users",
        "target_schema" => "raw",
        "target_table" => "users"
      }
    ]

    assert %{{"raw", "users"} => table} = DesignatedTable.by_target(designated_tables, "source-a")

    assert table.source_schema == "public"
    assert table.target_schema == "raw"
    assert table.target_table == "users"
    assert table.checkpoint_key == "source-a:raw.users"
  end
end
