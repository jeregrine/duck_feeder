defmodule DuckFeeder.DuckDB.ConnectionTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.DuckDB.Connection

  test "resolve_opts leaves owned duckdb startup to the caller" do
    assert {:ok, duckdb} = Connection.resolve_opts(duckdb: %{})
    assert duckdb == %{}
  end

  test "resolve_opts keeps explicit external connections" do
    assert {:ok, server} = Connection.start_link(name: nil)
    conn = Connection.get_conn(server)

    try do
      assert {:ok, %{conn: ^conn}} = Connection.resolve_opts(duckdb: %{conn: conn})
    after
      GenServer.stop(server)
    end
  end
end
