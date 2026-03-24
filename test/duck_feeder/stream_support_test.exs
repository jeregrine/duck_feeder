defmodule DuckFeeder.StreamSupportTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.StreamSupport

  test "designated_table_config_mapping accepts string-key designated tables" do
    designated_tables = [
      %{
        "source_schema" => "public",
        "source_table" => "users",
        "target_schema" => "raw",
        "target_table" => "users"
      }
    ]

    assert %{{"raw", "users"} => table} =
             StreamSupport.designated_table_config_mapping(designated_tables)

    assert table.target_schema == "raw"
    assert table.target_table == "users"
  end

  test "resolve_duckdb explicitly stops owned duckdb connections when the caller exits normally" do
    parent = self()

    _caller =
      spawn(fn ->
        Process.flag(:trap_exit, true)

        assert {:ok, %{conn: conn, server: server}} =
                 StreamSupport.resolve_duckdb([duckdb: %{}], DuckFeeder.Sink.DuckDB)

        send(parent, {:resolved_duckdb_conn, conn, server})
      end)

    assert_receive {:resolved_duckdb_conn, conn, server}
    assert is_pid(conn)
    assert is_pid(server)

    assert_eventually(fn -> not Process.alive?(server) end)
    assert_eventually(fn -> not Process.alive?(conn) end)
  end

  defp assert_eventually(fun, attempts \\ 20)

  defp assert_eventually(fun, attempts) when attempts > 0 do
    if fun.() do
      :ok
    else
      Process.sleep(25)
      assert_eventually(fun, attempts - 1)
    end
  end

  defp assert_eventually(_fun, 0), do: flunk("condition did not become true")
end
