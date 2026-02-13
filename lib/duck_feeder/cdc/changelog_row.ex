defmodule DuckFeeder.CDC.ChangelogRow do
  @moduledoc """
  Converts routed CDC changes into changelog row shape.
  """

  @spec from_change(map(), map(), keyword()) :: map()
  def from_change(change, transaction, opts \\ []) when is_map(change) and is_map(transaction) do
    ingest_ts = Keyword.get(opts, :ingest_ts, DateTime.utc_now())

    %{
      _op: op_code(change[:op]),
      _commit_lsn: transaction[:end_lsn],
      _xid: transaction[:xid],
      _source_ts: transaction[:commit_timestamp],
      _ingest_ts: ingest_ts,
      _relation_schema: elem(change[:relation], 0),
      _relation_table: elem(change[:relation], 1),
      _record: Map.get(change, :record, %{}),
      _old_record: Map.get(change, :old_record, %{})
    }
  end

  @spec op_code(atom() | String.t()) :: String.t()
  def op_code(:insert), do: "I"
  def op_code(:update), do: "U"
  def op_code(:delete), do: "D"
  def op_code(:truncate), do: "T"
  def op_code(op) when is_binary(op), do: op
  def op_code(op), do: to_string(op)
end
