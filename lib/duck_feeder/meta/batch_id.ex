defmodule DuckFeeder.Meta.BatchId do
  @moduledoc """
  Deterministic batch id builder.

  Used to make batch inserts idempotent across retries/restarts.
  """

  @spec build(pos_integer(), String.t(), String.t(), [term()]) :: String.t()
  def build(designated_table_id, lsn_start, lsn_end, file_indexes \\ [])
      when is_integer(designated_table_id) and designated_table_id > 0 and is_binary(lsn_start) and
             is_binary(lsn_end) and is_list(file_indexes) do
    normalized_indexes = Enum.sort(file_indexes)

    payload = "#{designated_table_id}|#{lsn_start}|#{lsn_end}|#{inspect(normalized_indexes)}"

    hash = :crypto.hash(:sha256, payload) |> Base.encode16(case: :lower)
    "b_" <> String.slice(hash, 0, 40)
  end
end
