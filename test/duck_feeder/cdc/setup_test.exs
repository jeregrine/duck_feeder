defmodule DuckFeeder.CDC.SetupTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.CDC.Setup

  test "ensure_publication returns :exists when publication already exists" do
    {:ok, calls, query_fun} = fake_query_fun([{:ok, %Postgrex.Result{num_rows: 1, rows: [[1]]}}])

    assert {:ok, :exists} =
             Setup.ensure_publication(
               :conn,
               "duck_pub",
               [%{source_schema: "public", source_table: "users"}],
               query_fun: query_fun
             )

    assert [{"SELECT 1 FROM pg_publication WHERE pubname = $1", ["duck_pub"]}] ==
             Agent.get(calls, &Enum.reverse/1)
  end

  test "ensure_publication creates publication when missing" do
    {:ok, calls, query_fun} =
      fake_query_fun([
        {:ok, %Postgrex.Result{num_rows: 0, rows: []}},
        {:ok, %Postgrex.Result{num_rows: 0, rows: []}}
      ])

    designated_tables = [
      %{source_schema: "public", source_table: "users"},
      %{source_schema: "public", source_table: "orders"}
    ]

    assert {:ok, :created} =
             Setup.ensure_publication(:conn, "duck_pub", designated_tables, query_fun: query_fun)

    [_, {create_sql, []}] = Agent.get(calls, &Enum.reverse/1)

    assert create_sql =~ "CREATE PUBLICATION \"duck_pub\""
    assert create_sql =~ "\"public\".\"users\""
    assert create_sql =~ "\"public\".\"orders\""
  end

  test "ensure_slot creates logical slot when missing" do
    {:ok, _calls, query_fun} =
      fake_query_fun([
        {:ok, %Postgrex.Result{num_rows: 0, rows: []}},
        {:ok, %Postgrex.Result{rows: [["duck_slot", "0/16B6A98"]]}}
      ])

    assert {:ok, {:created, %{slot_name: "duck_slot", lsn: "0/16B6A98"}}} =
             Setup.ensure_slot(:conn, "duck_slot", "pgoutput", query_fun: query_fun)
  end

  test "drop_slot no-ops when slot does not exist" do
    {:ok, calls, query_fun} =
      fake_query_fun([
        {:ok, %Postgrex.Result{num_rows: 0, rows: []}}
      ])

    assert :ok = Setup.drop_slot(:conn, "duck_slot", query_fun: query_fun)

    assert [{"SELECT 1 FROM pg_replication_slots WHERE slot_name = $1", ["duck_slot"]}] ==
             Agent.get(calls, &Enum.reverse/1)
  end

  test "publication_tables_sql validates designated tables" do
    assert {:error, :no_designated_tables} = Setup.publication_tables_sql([])

    assert {:error, {:missing_required, :source_table}} =
             Setup.publication_tables_sql([
               %{source_schema: "public"}
             ])
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
      safe_stop(calls_agent)
      safe_stop(responses_agent)
    end)

    {:ok, calls_agent, query_fun}
  end

  defp safe_stop(pid) when is_pid(pid) do
    if Process.alive?(pid), do: Agent.stop(pid)
  end

  defp safe_stop(_), do: :ok
end
