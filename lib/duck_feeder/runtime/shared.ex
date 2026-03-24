defmodule DuckFeeder.Runtime.Shared do
  @moduledoc false

  @spec fetch_duckdb!(keyword()) :: map() | nil
  def fetch_duckdb!(opts) when is_list(opts) do
    case {Keyword.fetch(opts, :duckdb), Keyword.fetch(opts, :duckdb_config)} do
      {{:ok, duckdb}, _} ->
        duckdb

      {:error, {:ok, duckdb}} ->
        duckdb

      {:error, :error} ->
        raise ArgumentError,
              "expected :duckdb or :duckdb_config in runtime opts, got keys: #{inspect(Keyword.keys(opts))}"
    end
  end

  @spec mapify(term()) :: map()
  def mapify(value) when is_map(value), do: value

  def mapify(value) when is_list(value) do
    if Keyword.keyword?(value), do: Map.new(value), else: %{}
  end

  def mapify(_value), do: %{}
end
