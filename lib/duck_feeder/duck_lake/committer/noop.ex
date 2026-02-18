defmodule DuckFeeder.DuckLake.Committer.Noop do
  @moduledoc """
  Checkpoint-only committer that advances batch state without writing DuckLake metadata.

  Delegates to `DuckFeeder.Meta.commit_uploaded_batch/2`.
  """

  @behaviour DuckFeeder.DuckLake.Committer

  alias DuckFeeder.Meta

  @impl true
  def commit_batch(meta_conn, batch_id, opts) when is_binary(batch_id) do
    meta_module = Keyword.get(opts, :meta_module, Meta)
    meta_module.commit_uploaded_batch(meta_conn, batch_id)
  end
end
