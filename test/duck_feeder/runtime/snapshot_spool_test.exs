defmodule DuckFeeder.Runtime.SnapshotSpoolTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.Runtime.SnapshotSpool

  test "collector spools rows and replay_rows replays them" do
    assert {:ok, row_handler, collect_rows} = SnapshotSpool.collector()

    designated_table = %{source_table: "users", target_table: "users"}

    assert :ok = row_handler.(designated_table, %{"id" => 1})
    assert :ok = row_handler.(designated_table, %{"id" => 2})

    {:spooled_snapshot_rows, path, row_count} = collect_rows.()
    rows_source = {:spooled_snapshot_rows, path, 0, row_count}

    replayed =
      Agent.start_link(fn -> [] end)
      |> then(fn {:ok, agent} ->
        on_exit(fn ->
          if Process.alive?(agent), do: GenServer.stop(agent)
        end)

        assert :ok =
                 SnapshotSpool.replay_rows(rows_source, fn table, row ->
                   Agent.update(agent, &[{table, row} | &1])
                   :ok
                 end)

        Agent.get(agent, &Enum.reverse/1)
      end)

    assert replayed == [
             {designated_table, %{"id" => 1}},
             {designated_table, %{"id" => 2}}
           ]
  end

  test "replay_plan skips snapshot rows already covered by checkpoint progress" do
    assert {:ok, row_handler, collect_rows} = SnapshotSpool.collector()

    designated_table = %{source_table: "users", target_table: "users"}

    assert :ok = row_handler.(designated_table, %{"id" => 1})
    assert :ok = row_handler.(designated_table, %{"id" => 2})
    assert :ok = row_handler.(designated_table, %{"id" => 3})

    rows_source = collect_rows.()

    assert {:ok, %{rows: remaining_rows, snapshot_lsn_start: "0/34"}} =
             SnapshotSpool.replay_plan("0/34", "0/35", rows_source)

    assert :ok =
             SnapshotSpool.replay_rows(remaining_rows, fn _table, row ->
               send(self(), {:replayed_row, row})
               :ok
             end)

    assert_receive {:replayed_row, %{"id" => 3}}
    refute_receive {:replayed_row, %{"id" => 1}}, 50
    refute_receive {:replayed_row, %{"id" => 2}}, 50
  end

  test "cleanup_rows_source removes spooled files" do
    assert {:ok, row_handler, collect_rows} = SnapshotSpool.collector()
    assert :ok = row_handler.(%{source_table: "users"}, %{"id" => 1})

    rows_source = collect_rows.()
    {:spooled_snapshot_rows, path, _row_count} = rows_source

    assert File.exists?(path)
    assert :ok = SnapshotSpool.cleanup_rows_source(rows_source)
    refute File.exists?(path)
  end
end
