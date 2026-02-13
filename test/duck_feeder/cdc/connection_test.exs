defmodule DuckFeeder.CDC.ConnectionTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.CDC.{Connection, Event, Lsn}
  alias DuckFeeder.CDC.Connection.State

  test "initializes state and builds START_REPLICATION stream query" do
    assert {:ok, %State{} = state} =
             Connection.init(
               slot_name: "duck_slot",
               publication_name: "duck_pub",
               start_lsn: "0/10",
               event_sink: self(),
               status_interval_ms: 0
             )

    assert state.received_lsn == Lsn.parse!("0/10")

    assert {:stream, query, [], %State{} = next_state} = Connection.handle_connect(state)

    assert query =~ "START_REPLICATION SLOT \"duck_slot\" LOGICAL 0/10"
    assert query =~ "publication_names 'duck_pub'"
    assert next_state.step == :streaming
  end

  test "acknowledges primary keepalive reply requests" do
    {:ok, state} =
      Connection.init(
        slot_name: "duck_slot",
        publication_name: "duck_pub",
        start_lsn: "0/0",
        event_sink: self(),
        status_interval_ms: 0
      )

    {:stream, _query, [], state} = Connection.handle_connect(state)

    keepalive = <<?k, 32::64, 0::64-signed, 1::8>>

    assert {:noreply, [ack], %State{} = state} = Connection.handle_data(keepalive, state)

    <<?r, write::64, flush::64, apply::64, _timestamp::64-signed, 0::8>> = ack

    assert write == 33
    assert flush == 33
    assert apply == 33
    assert state.received_lsn == 32
    assert state.applied_lsn == 32
  end

  test "decodes xlog data, emits events, and acknowledges commit" do
    {:ok, state} =
      Connection.init(
        slot_name: "duck_slot",
        publication_name: "duck_pub",
        start_lsn: "0/0",
        event_sink: self(),
        status_interval_ms: 0
      )

    {:stream, _query, [], state} = Connection.handle_connect(state)

    relation_payload =
      <<
        ?R,
        1::32,
        "public",
        0,
        "users",
        0,
        ?f,
        2::16,
        1::8,
        "id",
        0,
        23::32,
        -1::32-signed,
        0::8,
        "name",
        0,
        25::32,
        -1::32-signed
      >>

    assert {:noreply, [], %State{} = state} =
             Connection.handle_data(xlog(90, 100, relation_payload), state)

    assert_receive {:duck_feeder_cdc_event,
                    %Event.Relation{id: 1, schema: "public", table: "users"}}

    begin_payload = <<?B, 100::64, 0::64-signed, 500::32>>

    assert {:noreply, [], %State{} = state} =
             Connection.handle_data(xlog(100, 101, begin_payload), state)

    assert_receive {:duck_feeder_cdc_event, %Event.Begin{xid: 500}}

    insert_payload = <<?I, 1::32, ?N, 2::16, encode_tuple(["1", "duck"])::binary>>

    assert {:noreply, [], %State{} = state} =
             Connection.handle_data(xlog(101, 102, insert_payload), state)

    assert_receive {:duck_feeder_cdc_event,
                    %Event.Insert{relation_id: 1, record: %{"id" => "1", "name" => "duck"}}}

    commit_payload = <<?C, 0::8, 100::64, 120::64, 1_000_000::64-signed>>

    assert {:noreply, [ack], %State{} = state} =
             Connection.handle_data(xlog(102, 120, commit_payload), state)

    assert_receive {:duck_feeder_cdc_event, %Event.Commit{xid: 500, end_lsn: "0/78"}}

    <<?r, write::64, flush::64, apply::64, _timestamp::64-signed, 0::8>> = ack

    assert write == 121
    assert flush == 121
    assert apply == 121
    assert state.applied_lsn == 120
  end

  test "disconnects on conversion errors" do
    {:ok, state} =
      Connection.init(
        slot_name: "duck_slot",
        publication_name: "duck_pub",
        start_lsn: "0/0",
        event_sink: self(),
        status_interval_ms: 0
      )

    {:stream, _query, [], state} = Connection.handle_connect(state)

    bad_insert_payload = <<?I, 9::32, ?N, 1::16, encode_tuple(["1"])::binary>>

    assert {:disconnect, {:logical_replication_convert_failed, {:unknown_relation, 9}}} =
             Connection.handle_data(xlog(0, 1, bad_insert_payload), state)
  end

  test "disconnects when max lag is exceeded" do
    {:ok, state} =
      Connection.init(
        slot_name: "duck_slot",
        publication_name: "duck_pub",
        start_lsn: "0/0",
        event_sink: self(),
        max_lag_bytes: 1,
        status_interval_ms: 0
      )

    {:stream, _query, [], state} = Connection.handle_connect(state)

    relation_payload =
      <<
        ?R,
        1::32,
        "public",
        0,
        "users",
        0,
        ?f,
        2::16,
        1::8,
        "id",
        0,
        23::32,
        -1::32-signed,
        0::8,
        "name",
        0,
        25::32,
        -1::32-signed
      >>

    assert {:disconnect, {:max_lag_exceeded, 10, 1}} =
             Connection.handle_data(xlog(0, 10, relation_payload), state)
  end

  test "handle_disconnect emits noreply" do
    {:ok, state} =
      Connection.init(
        slot_name: "duck_slot",
        publication_name: "duck_pub",
        start_lsn: "0/0",
        event_sink: self(),
        status_interval_ms: 0
      )

    assert {:noreply, %State{slot_name: "duck_slot"}} = Connection.handle_disconnect(state)
  end

  defp xlog(wal_start, wal_end, payload) do
    <<?w, wal_start::64, wal_end::64, 0::64-signed, payload::binary>>
  end

  defp encode_tuple(values) do
    values
    |> Enum.map(fn
      nil -> <<?n>>
      :unchanged_toast -> <<?u>>
      value when is_binary(value) -> <<?t, byte_size(value)::32, value::binary>>
    end)
    |> IO.iodata_to_binary()
  end
end
