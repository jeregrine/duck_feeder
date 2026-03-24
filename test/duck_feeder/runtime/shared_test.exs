defmodule DuckFeeder.Runtime.SharedTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.Runtime.Shared

  test "fetch_duckdb! raises a helpful error when duckdb config is missing" do
    assert_raise ArgumentError, ~r/expected :duckdb or :duckdb_config/, fn ->
      Shared.fetch_duckdb!([])
    end
  end
end
