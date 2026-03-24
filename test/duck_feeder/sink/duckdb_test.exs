defmodule DuckFeeder.Sink.DuckDBTest do
  use ExUnit.Case, async: false

  alias DuckFeeder.Sink.DuckDB

  defmodule FakeMeta do
    def upsert_checkpoint(conn, checkpoint_key, lsn) do
      if is_pid(conn) do
        send(conn, {:checkpoint_upserted, checkpoint_key, lsn})
      end

      {:ok, lsn}
    end
  end

  setup do
    :ok = Adbc.download_driver!(:duckdb)
    {:ok, db} = Adbc.Database.start_link(driver: :duckdb)
    {:ok, conn} = Adbc.Connection.start_link(database: db)

    on_exit(fn ->
      safe_stop(conn)
      safe_stop(db)
    end)

    {:ok, conn: conn}
  end

  test "appends rows directly into target table and persists checkpoint", %{conn: conn} do
    context = %{
      meta_conn: self(),
      meta_module: FakeMeta,
      designated_table_by_target: %{{"raw", "events"} => "source-a:raw.events"},
      designated_table_config_by_target: %{
        {"raw", "events"} => %{
          checkpoint_key: "source-a:raw.events",
          target_schema: "raw",
          target_table: "events"
        }
      },
      duckdb: %{conn: conn}
    }

    batch = %{
      rows: [
        %{"id" => 1, "kind" => "page_view"},
        %{"id" => 2, "kind" => "signup"}
      ],
      lsn_start: "0/10",
      lsn_end: "0/11"
    }

    assert {:ok, result} = DuckDB.process_batch(context, {"raw", "events"}, batch)
    assert result.status == :committed
    assert result.checkpoint_key == "source-a:raw.events"
    assert result.checkpoint_lsn == "0/11"

    assert_receive {:checkpoint_upserted, "source-a:raw.events", "0/11"}

    assert %{"id" => [1, 2], "kind" => ["page_view", "signup"]} =
             query_map(conn, "SELECT id, kind FROM raw.events ORDER BY id")
  end

  test "applies CDC batches as table operations", %{conn: conn} do
    context = %{
      meta_conn: self(),
      meta_module: FakeMeta,
      designated_table_by_target: %{{"raw", "users"} => "source-a:raw.users"},
      designated_table_config_by_target: %{
        {"raw", "users"} => %{
          checkpoint_key: "source-a:raw.users",
          source_schema: "public",
          source_table: "users",
          target_schema: "raw",
          target_table: "users",
          primary_keys: ["id"]
        }
      },
      duckdb: %{conn: conn}
    }

    initial_batch = %{
      rows: [
        cdc_row("I", %{"id" => 1, "name" => "alice"}),
        cdc_row("I", %{"id" => 2, "name" => "bob"})
      ],
      lsn_start: "0/20",
      lsn_end: "0/21"
    }

    assert {:ok, initial_result} = DuckDB.process_batch(context, {"raw", "users"}, initial_batch)
    assert initial_result.checkpoint_lsn == "0/21"

    assert %{"id" => [1, 2], "name" => ["alice", "bob"]} =
             query_map(conn, "SELECT id, name FROM raw.users ORDER BY id")

    update_batch = %{
      rows: [
        cdc_row("U", %{"id" => 1, "name" => "alice-2", "age" => 31}, %{
          "id" => 1,
          "name" => "alice"
        }),
        cdc_row("D", %{}, %{"id" => 2, "name" => "bob"}),
        cdc_row("I", %{"id" => 3, "name" => "carol", "age" => 28})
      ],
      lsn_start: "0/21",
      lsn_end: "0/22"
    }

    assert {:ok, update_result} = DuckDB.process_batch(context, {"raw", "users"}, update_batch)
    assert update_result.operation_counts == %{truncate: 0, deletes: 1, upserts: 2}
    assert update_result.checkpoint_lsn == "0/22"

    assert %{"id" => [1, 3], "name" => ["alice-2", "carol"], "age" => [31, 28]} =
             query_map(conn, "SELECT id, name, age FROM raw.users ORDER BY id")

    truncate_batch = %{
      rows: [cdc_row("T", %{})],
      lsn_start: "0/22",
      lsn_end: "0/23"
    }

    assert {:ok, truncate_result} =
             DuckDB.process_batch(context, {"raw", "users"}, truncate_batch)

    assert truncate_result.operation_counts == %{truncate: 1, deletes: 0, upserts: 0}
    assert %{"n" => [0]} = query_map(conn, "SELECT count(*) AS n FROM raw.users")
  end

  test "fails closed on CDC updates without primary keys", %{conn: conn} do
    context = %{
      meta_conn: self(),
      meta_module: FakeMeta,
      designated_table_by_target: %{{"raw", "users"} => "source-a:raw.users"},
      designated_table_config_by_target: %{
        {"raw", "users"} => %{
          checkpoint_key: "source-a:raw.users",
          target_schema: "raw",
          target_table: "users"
        }
      },
      duckdb: %{conn: conn}
    }

    batch = %{
      rows: [
        cdc_row("U", %{"id" => 1, "name" => "alice-2"}, %{"id" => 1, "name" => "alice"})
      ],
      lsn_start: "0/30",
      lsn_end: "0/31"
    }

    assert {:error, {:missing_primary_keys, {"raw", "users"}}} =
             DuckDB.process_batch(context, {"raw", "users"}, batch)
  end

  test "runs setup hooks once per connection/config", %{conn: conn} do
    context = %{
      meta_conn: self(),
      meta_module: FakeMeta,
      designated_table_by_target: %{{"raw", "events"} => "source-a:raw.events"},
      designated_table_config_by_target: %{
        {"raw", "events"} => %{
          checkpoint_key: "source-a:raw.events",
          target_schema: "raw",
          target_table: "events"
        }
      },
      duckdb: %{
        conn: conn,
        setup_sql: ["CREATE SCHEMA IF NOT EXISTS raw"],
        setup_fun: fn _ ->
          send(self(), :setup_fun_called)
          :ok
        end
      }
    }

    batch = %{
      rows: [%{"id" => 1, "kind" => "page_view"}],
      lsn_start: "0/40",
      lsn_end: "0/41"
    }

    assert {:ok, _} = DuckDB.process_batch(context, {"raw", "events"}, batch)
    assert {:ok, _} = DuckDB.process_batch(context, {"raw", "events"}, %{batch | lsn_end: "0/42"})

    assert_receive :setup_fun_called
    refute_receive :setup_fun_called
  end

  defp cdc_row(op, record, old_record \\ %{}) do
    %{
      _op: op,
      _record: record,
      _old_record: old_record
    }
  end

  defp query_map(conn, sql) do
    conn
    |> Adbc.Connection.query!(sql)
    |> Adbc.Result.to_map()
    |> Map.new(fn {key, values} -> {key, Enum.map(values, &normalize_value/1)} end)
  end

  defp normalize_value(%Decimal{} = value), do: Decimal.to_integer(value)
  defp normalize_value(value), do: value

  defp safe_stop(pid) when is_pid(pid) do
    _ = GenServer.stop(pid)
    :ok
  catch
    :exit, _reason -> :ok
  end
end
