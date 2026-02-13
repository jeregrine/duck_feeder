defmodule DuckFeeder.Meta do
  @moduledoc """
  Public API for the `duckfeeder_meta` control-plane store.
  """

  alias DuckFeeder.Meta.{BatchId, Store}

  defdelegate bootstrap(conn), to: Store

  defdelegate register_source(conn, attrs), to: Store
  defdelegate get_source(conn, source_name), to: Store
  defdelegate register_designated_table(conn, attrs), to: Store
  defdelegate list_designated_tables(conn, opts \\ []), to: Store
  defdelegate fetch_source_start_lsn(conn, source_id, default_lsn \\ "0/0"), to: Store

  defdelegate fetch_checkpoint(conn, designated_table_id), to: Store
  defdelegate upsert_checkpoint(conn, designated_table_id, lsn), to: Store

  defdelegate insert_batch(conn, attrs), to: Store
  defdelegate get_batch_state(conn, batch_id), to: Store
  defdelegate transition_batch(conn, batch_id, to_state, opts \\ []), to: Store
  defdelegate commit_uploaded_batch(conn, batch_id), to: Store
  defdelegate commit_uploaded_batch_tx(conn, batch_id), to: Store

  defdelegate put_batch_file(conn, attrs), to: Store
  defdelegate list_stale_batches(conn, opts \\ []), to: Store

  defdelegate build_batch_id(designated_table_id, lsn_start, lsn_end, file_indexes \\ []),
    to: BatchId,
    as: :build
end
