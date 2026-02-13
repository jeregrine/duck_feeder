defmodule DuckFeeder.DuckLake.Committer.Noop do
  @moduledoc """
  Temporary committer implementation that only advances batch checkpoint state.
  """

  @behaviour DuckFeeder.DuckLake.Committer

  alias DuckFeeder.Meta

  @impl true
  def commit_batch(meta_conn, batch_id, opts) when is_binary(batch_id) do
    meta_module = Keyword.get(opts, :meta_module, Meta)
    meta_module.commit_uploaded_batch(meta_conn, batch_id)
  end
end
