defmodule DuckFeeder.DuckLake.Committer do
  @moduledoc """
  DuckLake commit interface.

  Implementations receive a batch id and are responsible for persisting DuckLake
  metadata (snapshots, data files, column mappings, stats) alongside the
  `duckfeeder_meta` checkpoint advance.

  Implementations:
  - `DuckFeeder.DuckLake.Committer.Noop` — checkpoint-only (no DuckLake metadata writes)
  - `DuckFeeder.DuckLake.Committer.Postgres` — transactional DuckLake + checkpoint commit
  """

  @callback commit_batch(term(), String.t(), keyword()) :: {:ok, map()} | {:error, term()}
end
