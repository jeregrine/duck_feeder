defmodule DuckFeeder.CDC.SnapshotBoundary do
  @moduledoc """
  Snapshot boundary helpers for initial snapshot + WAL handoff.
  """

  alias DuckFeeder.CDC.Lsn

  @type decision :: :accept | :skip | {:error, term()}

  @spec should_process_transaction?(String.t(), String.t()) :: decision()
  def should_process_transaction?(tx_end_lsn, boundary_lsn)
      when is_binary(tx_end_lsn) and is_binary(boundary_lsn) do
    case Lsn.compare(tx_end_lsn, boundary_lsn) do
      :gt -> :accept
      :eq -> :skip
      :lt -> :skip
      {:error, _} = error -> error
    end
  end

  @spec tag_snapshot_row(map(), String.t(), keyword()) :: map()
  def tag_snapshot_row(row, boundary_lsn, opts \\ [])
      when is_map(row) and is_binary(boundary_lsn) do
    xid = Keyword.get(opts, :xid, 0)
    source_ts = Keyword.get(opts, :source_ts)
    ingest_ts = Keyword.get(opts, :ingest_ts, DateTime.utc_now())

    row
    |> Map.put(:_op, "R")
    |> Map.put(:_commit_lsn, boundary_lsn)
    |> Map.put(:_xid, xid)
    |> Map.put(:_source_ts, source_ts)
    |> Map.put(:_ingest_ts, ingest_ts)
  end
end
