defmodule DuckFeeder.CDC.LogicalReplication.Decoder do
  @moduledoc """
  Decodes pgoutput logical replication messages.
  """

  alias DuckFeeder.CDC.LogicalReplication.Messages

  alias Messages.{
    Begin,
    Commit,
    Delete,
    Insert,
    Message,
    Origin,
    Relation,
    Truncate,
    Type,
    Unsupported,
    Update
  }

  alias Relation.Column

  @pg_epoch DateTime.from_naive!(~N[2000-01-01 00:00:00], "Etc/UTC")

  @spec decode(binary()) :: Messages.message()
  def decode(message) when is_binary(message) do
    case decode_impl(message) do
      {:ok, decoded} -> decoded
      :error -> %Unsupported{type: message_type(message), data: message}
    end
  end

  defp decode_impl(<<?B, final_lsn::64, timestamp::64-signed, xid::32>>) do
    {:ok,
     %Begin{
       final_lsn: final_lsn,
       commit_timestamp: pg_timestamp_to_datetime(timestamp),
       xid: xid
     }}
  end

  defp decode_impl(<<?C, flags::8, lsn::64, end_lsn::64, timestamp::64-signed>>) do
    {:ok,
     %Commit{
       flags: decode_commit_flags(flags),
       lsn: lsn,
       end_lsn: end_lsn,
       commit_timestamp: pg_timestamp_to_datetime(timestamp)
     }}
  end

  defp decode_impl(<<?O, origin_commit_lsn::64, rest::binary>>) do
    with {:ok, name, <<>>} <- take_cstring(rest) do
      {:ok, %Origin{origin_commit_lsn: origin_commit_lsn, name: name}}
    else
      _ -> :error
    end
  end

  defp decode_impl(<<?M, flags::8, lsn::64, rest::binary>>) do
    with {:ok, prefix, rest} <- take_cstring(rest),
         <<content_len::32, content::binary-size(content_len), <<>>::binary>> <- rest do
      {:ok,
       %Message{
         transactional?: flags == 1,
         lsn: lsn,
         prefix: prefix,
         content: content
       }}
    else
      _ -> :error
    end
  end

  defp decode_impl(<<?R, id::32, rest::binary>>) do
    with {:ok, namespace, rest} <- take_cstring(rest),
         {:ok, name, <<replica_identity::8, column_count::16, columns_bin::binary>>} <-
           take_cstring(rest),
         {:ok, replica_identity} <- decode_replica_identity(replica_identity),
         {:ok, columns, <<>>} <- decode_columns(columns_bin, column_count) do
      {:ok,
       %Relation{
         id: id,
         namespace: namespace,
         name: name,
         replica_identity: replica_identity,
         columns: columns
       }}
    else
      _ -> :error
    end
  end

  defp decode_impl(<<?I, relation_id::32, ?N, column_count::16, tuple_bin::binary>>) do
    with {:ok, tuple_data, bytes, <<>>} <- decode_tuple_data(tuple_bin, column_count) do
      {:ok, %Insert{relation_id: relation_id, tuple_data: tuple_data, bytes: bytes}}
    else
      _ -> :error
    end
  end

  defp decode_impl(<<?U, relation_id::32, ?N, column_count::16, tuple_bin::binary>>) do
    with {:ok, tuple_data, bytes, <<>>} <- decode_tuple_data(tuple_bin, column_count) do
      {:ok,
       %Update{
         relation_id: relation_id,
         tuple_data: tuple_data,
         changed_key_tuple_data: nil,
         old_tuple_data: nil,
         bytes: bytes
       }}
    else
      _ -> :error
    end
  end

  defp decode_impl(<<?U, relation_id::32, key_or_old::8, column_count::16, tuple_bin::binary>>)
       when key_or_old in [?K, ?O] do
    with {:ok, old_tuple_data, old_bytes, <<?N, new_column_count::16, new_tuple_bin::binary>>} <-
           decode_tuple_data(tuple_bin, column_count),
         {:ok, tuple_data, new_bytes, <<>>} <- decode_tuple_data(new_tuple_bin, new_column_count) do
      base = %Update{
        relation_id: relation_id,
        tuple_data: tuple_data,
        changed_key_tuple_data: nil,
        old_tuple_data: nil,
        bytes: old_bytes + new_bytes
      }

      update =
        case key_or_old do
          ?K -> %{base | changed_key_tuple_data: old_tuple_data}
          ?O -> %{base | old_tuple_data: old_tuple_data}
        end

      {:ok, update}
    else
      _ -> :error
    end
  end

  defp decode_impl(<<?D, relation_id::32, key_or_old::8, column_count::16, tuple_bin::binary>>)
       when key_or_old in [?K, ?O] do
    with {:ok, tuple_data, bytes, <<>>} <- decode_tuple_data(tuple_bin, column_count) do
      base = %Delete{
        relation_id: relation_id,
        changed_key_tuple_data: nil,
        old_tuple_data: nil,
        bytes: bytes
      }

      delete =
        case key_or_old do
          ?K -> %{base | changed_key_tuple_data: tuple_data}
          ?O -> %{base | old_tuple_data: tuple_data}
        end

      {:ok, delete}
    else
      _ -> :error
    end
  end

  defp decode_impl(<<?T, number_of_relations::32, options::8, relations_bin::binary>>) do
    with {:ok, relation_ids, <<>>} <- decode_relation_ids(relations_bin, number_of_relations) do
      {:ok,
       %Truncate{
         number_of_relations: number_of_relations,
         options: decode_truncate_options(options),
         truncated_relations: relation_ids
       }}
    else
      _ -> :error
    end
  end

  defp decode_impl(<<?Y, type_id::32, rest::binary>>) do
    with {:ok, namespace, rest} <- take_cstring(rest),
         {:ok, name, <<>>} <- take_cstring(rest) do
      {:ok, %Type{id: type_id, namespace: namespace, name: name}}
    else
      _ -> :error
    end
  end

  defp decode_impl(_), do: :error

  defp decode_tuple_data(binary, column_count, acc \\ [], bytes \\ 0)

  defp decode_tuple_data(binary, 0, acc, bytes), do: {:ok, Enum.reverse(acc), bytes, binary}

  defp decode_tuple_data(<<?n, rest::binary>>, column_count, acc, bytes) do
    decode_tuple_data(rest, column_count - 1, [nil | acc], bytes)
  end

  defp decode_tuple_data(<<?u, rest::binary>>, column_count, acc, bytes) do
    decode_tuple_data(rest, column_count - 1, [:unchanged_toast | acc], bytes)
  end

  defp decode_tuple_data(
         <<?t, len::32, value::binary-size(len), rest::binary>>,
         column_count,
         acc,
         bytes
       ) do
    decode_tuple_data(rest, column_count - 1, [value | acc], bytes + len)
  end

  defp decode_tuple_data(_binary, _column_count, _acc, _bytes), do: :error

  defp decode_columns(binary, count, acc \\ [])

  defp decode_columns(binary, 0, acc), do: {:ok, Enum.reverse(acc), binary}

  defp decode_columns(<<flags::8, rest::binary>>, count, acc) do
    with {:ok, name, rest} <- take_cstring(rest),
         <<type_oid::32, type_modifier::32-signed, rest::binary>> <- rest do
      decoded_flags = if flags == 1, do: [:key], else: []

      decode_columns(
        rest,
        count - 1,
        [
          %Column{
            flags: decoded_flags,
            name: name,
            type_oid: type_oid,
            type_modifier: type_modifier
          }
          | acc
        ]
      )
    else
      _ -> :error
    end
  end

  defp decode_columns(_binary, _count, _acc), do: :error

  defp decode_relation_ids(binary, count, acc \\ [])

  defp decode_relation_ids(binary, 0, acc), do: {:ok, Enum.reverse(acc), binary}

  defp decode_relation_ids(<<relation_id::32, rest::binary>>, count, acc) do
    decode_relation_ids(rest, count - 1, [relation_id | acc])
  end

  defp decode_relation_ids(_binary, _count, _acc), do: :error

  defp decode_replica_identity(?d), do: {:ok, :default}
  defp decode_replica_identity(?n), do: {:ok, :nothing}
  defp decode_replica_identity(?f), do: {:ok, :all_columns}
  defp decode_replica_identity(?i), do: {:ok, :index}
  defp decode_replica_identity(_), do: :error

  defp decode_commit_flags(0), do: []
  defp decode_commit_flags(_), do: [:unknown]

  defp decode_truncate_options(options_byte) do
    <<_::6, restart_identity::1, cascade::1>> = <<options_byte>>

    Enum.reject(
      [
        if(cascade == 1, do: :cascade),
        if(restart_identity == 1, do: :restart_identity)
      ],
      &is_nil/1
    )
  end

  defp take_cstring(binary) do
    case :binary.match(binary, <<0>>) do
      {index, 1} ->
        <<value::binary-size(index), 0, rest::binary>> = binary
        {:ok, value, rest}

      :nomatch ->
        :error
    end
  end

  defp pg_timestamp_to_datetime(microseconds_since_pg_epoch) do
    DateTime.add(@pg_epoch, microseconds_since_pg_epoch, :microsecond)
  end

  defp message_type(<<type::8, _::binary>>), do: type
  defp message_type(_), do: nil
end
