defmodule DuckFeeder.Meta.SQL do
  @moduledoc """
  SQL assets for `duckfeeder_meta` schema bootstrap.
  """

  @schema_sql_path Path.expand("../../../priv/duckfeeder_meta/create_tables.sql", __DIR__)
  @external_resource @schema_sql_path

  @bootstrap_sql File.read!(@schema_sql_path)

  @spec bootstrap_sql() :: String.t()
  def bootstrap_sql, do: @bootstrap_sql

  @spec bootstrap_statements() :: [String.t()]
  def bootstrap_statements do
    @bootstrap_sql
    |> strip_sql_comments()
    |> String.split(";", trim: true)
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
  end

  defp strip_sql_comments(sql) do
    sql
    |> String.split("\n")
    |> Enum.map(&strip_comment_line/1)
    |> Enum.join("\n")
  end

  defp strip_comment_line(line) do
    case String.split(line, "--", parts: 2) do
      [code] -> code
      [code, _comment] -> code
    end
  end
end
