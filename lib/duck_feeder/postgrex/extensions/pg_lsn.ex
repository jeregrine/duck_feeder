defmodule DuckFeeder.Postgrex.Extensions.PgLsn do
  @moduledoc false

  use Postgrex.BinaryExtension, send: "pg_lsn_send"
  import Postgrex.BinaryUtils, warn: false

  def encode(_state) do
    quote location: :keep do
      lsn when is_binary(lsn) ->
        <<8::int32(), DuckFeeder.CDC.Lsn.parse!(lsn)::uint64()>>

      lsn when is_integer(lsn) and lsn >= 0 ->
        <<8::int32(), lsn::uint64()>>

      other ->
        raise DBConnection.EncodeError,
              Postgrex.Utils.encode_msg(other, "a pg_lsn string (e.g. 0/16B6A98)")
    end
  end

  def decode(_state) do
    quote location: :keep do
      <<8::int32(), wal_offset::uint64()>> ->
        DuckFeeder.CDC.Lsn.to_string(wal_offset)
    end
  end
end
