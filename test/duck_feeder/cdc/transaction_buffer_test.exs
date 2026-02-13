defmodule DuckFeeder.CDC.TransactionBufferTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.CDC.Event
  alias DuckFeeder.CDC.TransactionBuffer

  test "buffers events and emits committed transaction" do
    ts = DateTime.utc_now()

    state = TransactionBuffer.new()

    {:buffering, state} =
      TransactionBuffer.handle_event(state, %Event.Relation{
        id: 1,
        schema: "public",
        table: "users"
      })

    {:buffering, state} =
      TransactionBuffer.handle_event(state, %Event.Begin{
        xid: 100,
        final_lsn: "0/16B6A98",
        timestamp: ts
      })

    {:buffering, state} =
      TransactionBuffer.handle_event(state, %Event.Insert{relation_id: 1, record: %{"id" => "1"}})

    {:ok, transaction, state} =
      TransactionBuffer.handle_event(
        state,
        %Event.Commit{xid: 100, end_lsn: "0/16B6AF0", timestamp: ts}
      )

    assert transaction.xid == 100
    assert transaction.begin_lsn == "0/16B6A98"
    assert transaction.end_lsn == "0/16B6AF0"
    assert transaction.change_count == 1

    assert [change] = transaction.changes
    assert change.op == :insert
    assert change.relation == {"public", "users"}
    assert change.record == %{"id" => "1"}

    refute TransactionBuffer.in_transaction?(state)
  end

  test "rejects change outside transaction" do
    state = TransactionBuffer.new()

    {:buffering, state} =
      TransactionBuffer.handle_event(state, %Event.Relation{id: 1, schema: "s", table: "t"})

    assert {:error, :change_outside_transaction} =
             TransactionBuffer.handle_event(state, %Event.Insert{relation_id: 1, record: %{}})
  end

  test "rejects xid mismatch on commit" do
    state = TransactionBuffer.new()

    {:buffering, state} =
      TransactionBuffer.handle_event(state, %Event.Begin{xid: 10, final_lsn: "0/1"})

    assert {:error, {:xid_mismatch, 10, 11}} =
             TransactionBuffer.handle_event(state, %Event.Commit{xid: 11, end_lsn: "0/2"})
  end

  test "enforces max change limit" do
    state = TransactionBuffer.new(max_changes: 1)

    {:buffering, state} =
      TransactionBuffer.handle_event(state, %Event.Relation{
        id: 1,
        schema: "public",
        table: "users"
      })

    {:buffering, state} =
      TransactionBuffer.handle_event(state, %Event.Begin{xid: 100, final_lsn: "0/1"})

    {:buffering, state} =
      TransactionBuffer.handle_event(state, %Event.Insert{relation_id: 1, record: %{"id" => "1"}})

    assert {:error, {:exceeded_max_changes, 1}} =
             TransactionBuffer.handle_event(state, %Event.Insert{
               relation_id: 1,
               record: %{"id" => "2"}
             })
  end

  test "truncate emits one change per relation" do
    state = TransactionBuffer.new()

    {:buffering, state} =
      TransactionBuffer.handle_event(state, %Event.Relation{
        id: 1,
        schema: "public",
        table: "users"
      })

    {:buffering, state} =
      TransactionBuffer.handle_event(state, %Event.Relation{
        id: 2,
        schema: "public",
        table: "orders"
      })

    {:buffering, state} =
      TransactionBuffer.handle_event(state, %Event.Begin{xid: 200, final_lsn: "0/10"})

    {:buffering, state} =
      TransactionBuffer.handle_event(state, %Event.Truncate{relation_ids: [1, 2]})

    {:ok, transaction, _state} =
      TransactionBuffer.handle_event(state, %Event.Commit{xid: 200, end_lsn: "0/11"})

    assert transaction.change_count == 2

    assert Enum.map(transaction.changes, & &1.relation) ==
             [{"public", "users"}, {"public", "orders"}]

    assert Enum.all?(transaction.changes, &(&1.op == :truncate))
  end
end
