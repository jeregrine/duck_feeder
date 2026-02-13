defmodule DuckFeeder.CDC.ReplicationProtocol do
  @moduledoc """
  Logical replication SQL/binary protocol helpers.
  """

  alias DuckFeeder.CDC.Lsn

  @pg_epoch DateTime.from_naive!(~N[2000-01-01 00:00:00], "Etc/UTC")

  @spec identify_system_sql() :: String.t()
  def identify_system_sql, do: "IDENTIFY_SYSTEM"

  @spec create_slot_sql(String.t(), keyword()) :: String.t()
  def create_slot_sql(slot_name, opts \\ []) when is_binary(slot_name) do
    plugin = Keyword.get(opts, :plugin, "pgoutput")
    temporary? = Keyword.get(opts, :temporary, false)

    temporary_clause = if temporary?, do: " TEMPORARY", else: ""

    "CREATE_REPLICATION_SLOT #{quote_ident(slot_name)}#{temporary_clause} LOGICAL #{quote_ident(plugin)}"
  end

  @spec drop_slot_sql(String.t()) :: String.t()
  def drop_slot_sql(slot_name) when is_binary(slot_name) do
    "DROP_REPLICATION_SLOT #{quote_ident(slot_name)}"
  end

  @spec start_replication_sql(String.t(), String.t(), String.t()) :: String.t()
  def start_replication_sql(slot_name, start_lsn, publication_name)
      when is_binary(slot_name) and is_binary(start_lsn) and is_binary(publication_name) do
    "START_REPLICATION SLOT #{quote_ident(slot_name)} LOGICAL #{start_lsn} " <>
      "(proto_version '1', publication_names '#{escape_single_quotes(publication_name)}')"
  end

  @doc """
  Encodes a Standby Status Update message.

  See Postgres replication protocol docs for message `Byte1('r')`.
  """
  @spec encode_standby_status_update(
          String.t() | non_neg_integer(),
          String.t() | non_neg_integer(),
          String.t() | non_neg_integer(),
          boolean()
        ) ::
          binary()
  def encode_standby_status_update(write_lsn, flush_lsn, apply_lsn, reply_requested? \\ false) do
    write = normalize_lsn(write_lsn)
    flush = normalize_lsn(flush_lsn)
    apply = normalize_lsn(apply_lsn)
    timestamp = pg_timestamp_microseconds(DateTime.utc_now())
    reply = if reply_requested?, do: 1, else: 0

    <<?r, write::64, flush::64, apply::64, timestamp::64-signed, reply::8>>
  end

  @spec pg_timestamp_microseconds(DateTime.t()) :: integer()
  def pg_timestamp_microseconds(%DateTime{} = dt) do
    DateTime.diff(dt, @pg_epoch, :microsecond)
  end

  defp normalize_lsn(value) when is_integer(value) and value >= 0, do: value
  defp normalize_lsn(value) when is_binary(value), do: Lsn.parse!(value)

  defp quote_ident(identifier) do
    escaped = String.replace(identifier, "\"", "\"\"")
    "\"#{escaped}\""
  end

  defp escape_single_quotes(value), do: String.replace(value, "'", "''")
end
