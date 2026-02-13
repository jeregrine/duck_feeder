defmodule DuckFeeder.CDC.InitialSnapshotTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.CDC.InitialSnapshot

  test "begins snapshot transaction and returns snapshot id + boundary lsn" do
    {:ok, calls, query_fun} =
      fake_query_fun([
        {:ok, %Postgrex.Result{rows: []}},
        {:ok, %Postgrex.Result{rows: [["00000003-0000009A-1", "0/16B6A98"]]}}
      ])

    assert {:ok, %{snapshot_id: "00000003-0000009A-1", boundary_lsn: "0/16B6A98"}} =
             InitialSnapshot.begin_snapshot(:conn, query_fun)

    assert [
             {"BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY", []},
             {"SELECT pg_export_snapshot(), pg_current_wal_lsn()::text", []}
           ] == Agent.get(calls, &Enum.reverse/1)
  end

  test "finishes snapshot transaction" do
    query_fun = fn _conn, _sql, [] -> {:ok, %Postgrex.Result{rows: []}} end

    assert :ok = InitialSnapshot.finish_snapshot(:conn, :commit, query_fun)
    assert :ok = InitialSnapshot.finish_snapshot(:conn, :rollback, query_fun)
  end

  test "builds copy query with optional filters" do
    assert InitialSnapshot.copy_query("public", "users") ==
             "SELECT * FROM \"public\".\"users\""

    assert InitialSnapshot.copy_query("public", "users",
             columns: ["id", "name"],
             where: "id > 10",
             order_by: ["id"]
           ) ==
             "SELECT \"id\", \"name\" FROM \"public\".\"users\" WHERE id > 10 ORDER BY \"id\""
  end

  test "converts query result rows to snapshot-tagged rows" do
    result = %Postgrex.Result{columns: ["id", "name"], rows: [[1, "duck"]]}

    [row] = InitialSnapshot.result_rows_to_snapshot(result, "0/10", xid: 0)

    assert row["id"] == 1
    assert row["name"] == "duck"
    assert row[:_op] == "R"
    assert row[:_commit_lsn] == "0/10"
  end

  defp fake_query_fun(responses) do
    {:ok, calls_agent} = Agent.start_link(fn -> [] end)
    {:ok, responses_agent} = Agent.start_link(fn -> responses end)

    query_fun = fn _conn, sql, params ->
      Agent.update(calls_agent, &[{sql, params} | &1])

      Agent.get_and_update(responses_agent, fn
        [next | rest] -> {next, rest}
        [] -> {{:error, :no_more_fake_responses}, []}
      end)
    end

    on_exit(fn ->
      if Process.alive?(calls_agent), do: Agent.stop(calls_agent)
      if Process.alive?(responses_agent), do: Agent.stop(responses_agent)
    end)

    {:ok, calls_agent, query_fun}
  end
end
