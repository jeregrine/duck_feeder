defmodule DuckFeeder.Meta do
  @moduledoc """
  Public API for the `duckfeeder_meta` control-plane store.
  """

  alias DuckFeeder.Meta.Store

  defdelegate bootstrap(conn), to: Store

  defdelegate fetch_start_lsn(conn, checkpoint_keys, default_lsn \\ "0/0"), to: Store

  defdelegate fetch_checkpoint(conn, checkpoint_key), to: Store
  defdelegate upsert_checkpoint(conn, checkpoint_key, lsn), to: Store

  defdelegate fetch_snapshot_handoff(conn, source_name), to: Store
  defdelegate mark_snapshot_handoff_pending(conn, source_name, boundary_lsn), to: Store
  defdelegate mark_snapshot_handoff_complete(conn, source_name, boundary_lsn), to: Store
  defdelegate clear_snapshot_handoff(conn, source_name), to: Store
end
