defmodule DuckFeeder.DuckLake.Committer do
  @moduledoc """
  DuckLake commit interface.

  Current default implementation is a no-op committer that delegates to
  `DuckFeeder.Meta.commit_uploaded_batch/2` until DuckLake metadata table writes
  are implemented.
  """

  @callback commit_batch(term(), String.t(), keyword()) :: {:ok, map()} | {:error, term()}
end
