defmodule DuckFeeder.DuckLake.SQL do
  @moduledoc """
  SQL statement provider for DuckLake commit transactions.

  This module currently provides a configurable statement list hook used by
  `DuckFeeder.DuckLake.Committer.Postgres`.
  """

  @type statement :: String.t() | {String.t(), list()}

  @spec commit_statements(String.t(), keyword()) :: [statement()]
  def commit_statements(batch_id, opts \\ []) when is_binary(batch_id) do
    case Keyword.get(opts, :ducklake_sql, []) do
      statements when is_list(statements) -> statements
      fun when is_function(fun, 1) -> List.wrap(fun.(batch_id))
      _ -> []
    end
  end
end
