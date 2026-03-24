defmodule DuckFeeder.CDC.ReplicationProtocolTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.CDC.{Lsn, ReplicationProtocol}

  test "builds replication start SQL" do
    assert ReplicationProtocol.start_replication_sql("duck_slot", "0/16B6A98", "duck_pub") =~
             "START_REPLICATION SLOT \"duck_slot\" LOGICAL 0/16B6A98"
  end

  test "encodes standby status update message" do
    write_lsn = "0/10"
    flush_lsn = "0/11"
    apply_lsn = "0/12"

    message =
      ReplicationProtocol.encode_standby_status_update(write_lsn, flush_lsn, apply_lsn, true)

    assert byte_size(message) == 34

    <<type, write::64, flush::64, apply::64, _timestamp::64-signed, reply::8>> = message

    assert type == ?r
    assert write == Lsn.parse!(write_lsn)
    assert flush == Lsn.parse!(flush_lsn)
    assert apply == Lsn.parse!(apply_lsn)
    assert reply == 1
  end

  test "converts datetime to postgres epoch microseconds" do
    dt = DateTime.from_naive!(~N[2000-01-01 00:00:01], "Etc/UTC")
    assert ReplicationProtocol.pg_timestamp_microseconds(dt) == 1_000_000
  end
end
