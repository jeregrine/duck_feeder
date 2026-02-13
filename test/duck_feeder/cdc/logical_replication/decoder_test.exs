defmodule DuckFeeder.CDC.LogicalReplication.DecoderTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.CDC.LogicalReplication.Decoder
  alias DuckFeeder.CDC.LogicalReplication.Messages

  test "decodes begin and commit messages" do
    begin_message = <<?B, 16::64, 1_000_000::64-signed, 42::32>>

    assert %Messages.Begin{xid: 42, final_lsn: 16, commit_timestamp: begin_ts} =
             Decoder.decode(begin_message)

    assert DateTime.compare(begin_ts, DateTime.from_naive!(~N[2000-01-01 00:00:01], "Etc/UTC")) ==
             :eq

    commit_message = <<?C, 0::8, 16::64, 32::64, 2_000_000::64-signed>>

    assert %Messages.Commit{lsn: 16, end_lsn: 32, commit_timestamp: commit_ts, flags: []} =
             Decoder.decode(commit_message)

    assert DateTime.compare(commit_ts, DateTime.from_naive!(~N[2000-01-01 00:00:02], "Etc/UTC")) ==
             :eq
  end

  test "decodes relation and row change messages" do
    relation_columns =
      IO.iodata_to_binary([
        <<1::8, "id", 0, 23::32, -1::32-signed>>,
        <<0::8, "name", 0, 25::32, -1::32-signed>>
      ])

    relation_message = <<?R, 1::32, "public", 0, "users", 0, ?f, 2::16, relation_columns::binary>>

    assert %Messages.Relation{
             id: 1,
             namespace: "public",
             name: "users",
             replica_identity: :all_columns,
             columns: columns
           } =
             Decoder.decode(relation_message)

    assert Enum.map(columns, & &1.name) == ["id", "name"]

    insert_message = <<?I, 1::32, ?N, 2::16, encode_tuple(["1", "alice"])::binary>>

    assert %Messages.Insert{relation_id: 1, tuple_data: ["1", "alice"], bytes: 6} =
             Decoder.decode(insert_message)

    old_tuple = encode_tuple(["1", "alice"])
    new_tuple = encode_tuple(["1", "alice2"])
    update_message = <<?U, 1::32, ?O, 2::16, old_tuple::binary, ?N, 2::16, new_tuple::binary>>

    assert %Messages.Update{
             relation_id: 1,
             old_tuple_data: ["1", "alice"],
             tuple_data: ["1", "alice2"]
           } =
             Decoder.decode(update_message)

    delete_message = <<?D, 1::32, ?O, 2::16, old_tuple::binary>>

    assert %Messages.Delete{relation_id: 1, old_tuple_data: ["1", "alice"]} =
             Decoder.decode(delete_message)
  end

  test "decodes truncate and logical message payloads" do
    truncate_message = <<?T, 2::32, 1::8, 11::32, 12::32>>

    assert %Messages.Truncate{
             number_of_relations: 2,
             truncated_relations: [11, 12],
             options: [:cascade]
           } =
             Decoder.decode(truncate_message)

    msg_message = <<?M, 1::8, 33::64, "duck", 0, 4::32, "ping">>

    assert %Messages.Message{transactional?: true, lsn: 33, prefix: "duck", content: "ping"} =
             Decoder.decode(msg_message)
  end

  test "returns unsupported for malformed payload" do
    malformed = <<?I, 1::32>>

    assert %Messages.Unsupported{type: ?I, data: ^malformed} = Decoder.decode(malformed)
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
