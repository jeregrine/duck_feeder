defmodule DuckFeeder.CDC.MessageMapperTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.CDC.{Event, MessageMapper}

  test "maps begin message" do
    msg = %{type: :begin, xid: 1, final_lsn: "0/10"}

    assert {:ok, %Event.Begin{xid: 1, final_lsn: "0/10"}} = MessageMapper.map_message(msg)
  end

  test "maps commit message from tuple form" do
    msg = {:commit, %{xid: 1, end_lsn: "0/11"}}

    assert {:ok, %Event.Commit{xid: 1, end_lsn: "0/11"}} = MessageMapper.map_message(msg)
  end

  test "maps relation and row mutation messages" do
    assert {:ok, %Event.Relation{id: 10}} =
             MessageMapper.map_message(%{
               type: :relation,
               id: 10,
               schema: "public",
               table: "users"
             })

    assert {:ok, %Event.Insert{relation_id: 10, record: %{"id" => "1"}}} =
             MessageMapper.map_message(%{type: :insert, relation_id: 10, record: %{"id" => "1"}})

    assert {:ok, %Event.Update{relation_id: 10}} =
             MessageMapper.map_message(%{
               type: :update,
               relation_id: 10,
               record: %{"id" => "1"},
               old_record: %{"id" => "1"}
             })

    assert {:ok, %Event.Delete{relation_id: 10}} =
             MessageMapper.map_message(%{
               type: :delete,
               relation_id: 10,
               old_record: %{"id" => "1"}
             })
  end

  test "ignores keepalive messages" do
    assert {:ignore, %{type: :keepalive}} = MessageMapper.map_message(%{type: :keepalive})
  end

  test "returns validation errors" do
    assert {:error, {:invalid_field, :xid, nil}} =
             MessageMapper.map_message(%{type: :begin, final_lsn: "0/10"})

    assert {:error, {:unsupported_message, :bad}} = MessageMapper.map_message(:bad)
  end
end
