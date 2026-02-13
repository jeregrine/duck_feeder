defmodule DuckFeeder.CDC.LogicalReplication.ConverterTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.CDC.Event
  alias DuckFeeder.CDC.LogicalReplication.Converter
  alias DuckFeeder.CDC.LogicalReplication.Messages

  test "converts begin/commit and row changes" do
    relation = %Messages.Relation{
      id: 10,
      namespace: "public",
      name: "users",
      replica_identity: :all_columns,
      columns: [
        %Messages.Relation.Column{flags: [:key], name: "id", type_oid: 23, type_modifier: -1},
        %Messages.Relation.Column{flags: [], name: "name", type_oid: 25, type_modifier: -1}
      ]
    }

    state = Converter.new()

    assert {:ok, %Event.Relation{id: 10}, state} = Converter.convert(relation, state)

    assert {:ok, %Event.Begin{xid: 7, final_lsn: "0/10"}, state} =
             Converter.convert(
               %Messages.Begin{
                 xid: 7,
                 final_lsn: 16,
                 commit_timestamp: DateTime.from_naive!(~N[2026-01-01 00:00:00], "Etc/UTC")
               },
               state
             )

    assert {:ok, %Event.Insert{relation_id: 10, record: %{"id" => "1", "name" => "alice"}}, state} =
             Converter.convert(
               %Messages.Insert{relation_id: 10, tuple_data: ["1", "alice"], bytes: 6},
               state
             )

    assert {:ok,
            %Event.Update{
              relation_id: 10,
              old_record: %{"id" => "1", "name" => "alice"},
              record: %{"id" => "1", "name" => "alice"}
            }, state} =
             Converter.convert(
               %Messages.Update{
                 relation_id: 10,
                 old_tuple_data: ["1", "alice"],
                 changed_key_tuple_data: nil,
                 tuple_data: ["1", :unchanged_toast],
                 bytes: 3
               },
               state
             )

    assert {:ok, %Event.Delete{relation_id: 10, old_record: %{"id" => "1", "name" => "alice"}},
            state} =
             Converter.convert(
               %Messages.Delete{
                 relation_id: 10,
                 old_tuple_data: ["1", "alice"],
                 changed_key_tuple_data: nil,
                 bytes: 3
               },
               state
             )

    assert {:ok, %Event.Commit{xid: 7, end_lsn: "0/20"}, _state} =
             Converter.convert(
               %Messages.Commit{
                 flags: [],
                 lsn: 16,
                 end_lsn: 32,
                 commit_timestamp: DateTime.from_naive!(~N[2026-01-01 00:00:01], "Etc/UTC")
               },
               state
             )
  end

  test "returns relation and transaction errors" do
    state = Converter.new()

    assert {:error, {:unknown_relation, 10}} =
             Converter.convert(
               %Messages.Insert{relation_id: 10, tuple_data: ["1"], bytes: 1},
               state
             )

    relation = %Messages.Relation{
      id: 10,
      namespace: "public",
      name: "users",
      replica_identity: :default,
      columns: [
        %Messages.Relation.Column{flags: [:key], name: "id", type_oid: 23, type_modifier: -1}
      ]
    }

    assert {:ok, %Event.Relation{}, state} = Converter.convert(relation, state)

    assert {:error, {:replica_identity_not_full, {"public", "users"}}} =
             Converter.convert(
               %Messages.Update{
                 relation_id: 10,
                 old_tuple_data: nil,
                 changed_key_tuple_data: nil,
                 tuple_data: ["1"],
                 bytes: 1
               },
               state
             )

    assert {:error, :commit_without_begin} =
             Converter.convert(
               %Messages.Commit{
                 flags: [],
                 lsn: 0,
                 end_lsn: 10,
                 commit_timestamp: DateTime.utc_now()
               },
               state
             )
  end
end
