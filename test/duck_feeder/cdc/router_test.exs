defmodule DuckFeeder.CDC.RouterTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.CDC.Router

  test "routes only designated table changes" do
    transaction = %{
      xid: 10,
      begin_lsn: "0/10",
      end_lsn: "0/20",
      changes: [
        %{op: :insert, relation: {"public", "users"}, record: %{"id" => "1"}},
        %{op: :delete, relation: {"public", "orders"}, old_record: %{"id" => "9"}}
      ]
    }

    designated_tables = [
      %{
        checkpoint_key: "source-a:raw.users",
        source_schema: "public",
        source_table: "users",
        target_schema: "raw",
        target_table: "users"
      }
    ]

    routed = Router.route_transaction(transaction, designated_tables)

    assert routed.xid == 10
    assert Map.keys(routed.routes) == [{"raw", "users"}]

    assert [change] = routed.routes[{"raw", "users"}]
    assert change.op == :insert
    assert change.checkpoint_key == "source-a:raw.users"
    assert change.target_relation == {"raw", "users"}
  end

  test "routes designated table changes from a precomputed mapping" do
    transaction = %{
      xid: 11,
      begin_lsn: "0/11",
      end_lsn: "0/21",
      changes: [
        %{op: :insert, relation: {"public", "users"}, record: %{"id" => "1"}}
      ]
    }

    mapping =
      Router.build_mapping([
        %{
          checkpoint_key: "source-a:raw.users",
          source_schema: "public",
          source_table: "users",
          target_schema: "raw",
          target_table: "users"
        }
      ])

    routed = Router.route_transaction(transaction, mapping)

    assert [change] = routed.routes[{"raw", "users"}]
    assert change.checkpoint_key == "source-a:raw.users"
  end

  test "build_mapping raises when required keys are missing" do
    assert_raise ArgumentError, ~r/missing designated table key/, fn ->
      Router.build_mapping([
        %{source_schema: "public", source_table: "users", target_schema: "raw"}
      ])
    end
  end
end
