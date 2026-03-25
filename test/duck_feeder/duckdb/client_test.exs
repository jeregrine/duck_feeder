defmodule DuckFeeder.DuckDB.ClientTest do
  use ExUnit.Case, async: false

  alias DuckFeeder.DuckDB.Client
  alias DuckFeeder.DuckDB.Connection, as: DuckDBConnection

  setup do
    server =
      start_supervised!(%{
        id: {:duckdb_connection, System.unique_integer([:positive])},
        start: {DuckDBConnection, :start_link, [[name: nil]]}
      })

    conn = DuckDBConnection.get_conn(server)

    {:ok, conn: conn}
  end

  test "execute returns success", %{conn: conn} do
    assert :ok = Client.execute(conn, "CREATE TABLE client_test_users (id INTEGER)")
  end

  test "query_map returns result maps", %{conn: conn} do
    assert :ok = Client.execute(conn, "SELECT 1")
    assert {:ok, %{"n" => [1]}} = Client.query_map(conn, "SELECT 1 AS n")
  end

  test "returns a helpful error for invalid connections" do
    assert {:error, {:invalid_duckdb_conn, nil}} = Client.execute(nil, "SELECT 1")
    assert {:error, {:invalid_duckdb_conn, nil}} = Client.query_map(nil, "SELECT 1")
  end
end
