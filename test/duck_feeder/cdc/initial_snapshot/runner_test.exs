defmodule DuckFeeder.CDC.InitialSnapshot.RunnerTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.CDC.InitialSnapshot.Runner

  test "runs snapshot copy and dispatches rows" do
    {:ok, calls, query_fun} =
      fake_query_fun([
        {:ok, %Postgrex.Result{rows: []}},
        {:ok, %Postgrex.Result{rows: [["00000003-0000009A-1", "0/16B6A98"]]}},
        {:ok, %Postgrex.Result{columns: ["id", "name"], rows: [[1, "duck"], [2, "goose"]]}},
        {:ok, %Postgrex.Result{rows: []}}
      ])

    designated_tables = [%{source_schema: "public", source_table: "users"}]

    row_handler = fn table, row ->
      send(self(), {:snapshot_row, table, row})
      :ok
    end

    assert {:ok, result} =
             Runner.run(:conn, designated_tables, query_fun: query_fun, row_handler: row_handler)

    assert result.snapshot_id == "00000003-0000009A-1"
    assert result.boundary_lsn == "0/16B6A98"
    assert result.table_counts == %{{"public", "users"} => 2}

    assert_receive {:snapshot_row, %{source_table: "users"}, row1}
    assert_receive {:snapshot_row, %{source_table: "users"}, row2}

    assert row1[:_op] == "R"
    assert row2[:_op] == "R"

    queries = Agent.get(calls, &Enum.reverse/1)
    assert List.last(queries) == {"COMMIT", []}
  end

  test "rolls back snapshot when row handler fails" do
    {:ok, calls, query_fun} =
      fake_query_fun([
        {:ok, %Postgrex.Result{rows: []}},
        {:ok, %Postgrex.Result{rows: [["snap", "0/10"]]}},
        {:ok, %Postgrex.Result{columns: ["id"], rows: [[1]]}},
        {:ok, %Postgrex.Result{rows: []}}
      ])

    designated_tables = [%{source_schema: "public", source_table: "users"}]

    row_handler = fn _table, _row -> {:error, :stop_snapshot} end

    assert {:error, :stop_snapshot} =
             Runner.run(:conn, designated_tables, query_fun: query_fun, row_handler: row_handler)

    queries = Agent.get(calls, &Enum.reverse/1)
    assert List.last(queries) == {"ROLLBACK", []}
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
