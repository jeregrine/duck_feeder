defmodule DuckFeeder.Sink.DuckDBTest do
  use ExUnit.Case, async: false

  alias DuckFeeder.DuckDB.Client, as: DuckDBClient
  alias DuckFeeder.DuckDB.Connection, as: DuckDBConnection
  alias DuckFeeder.Sink.DuckDB

  defmodule FakeMeta do
    def upsert_checkpoint(conn, checkpoint_key, lsn) do
      if is_pid(conn) do
        send(conn, {:checkpoint_upserted, checkpoint_key, lsn})
      end

      {:ok, lsn}
    end
  end

  defmodule FailOnceMeta do
    def upsert_checkpoint(conn, checkpoint_key, lsn) do
      if is_pid(conn) do
        send(conn, {:checkpoint_attempted, checkpoint_key, lsn})
      end

      case Process.get({__MODULE__, checkpoint_key}) do
        nil ->
          Process.put({__MODULE__, checkpoint_key}, :failed_once)
          {:error, :forced_checkpoint_failure}

        :failed_once ->
          {:ok, lsn}
      end
    end
  end

  setup do
    {:ok, server} = DuckDBConnection.start_link(name: nil)
    conn = DuckDBConnection.get_conn(server)

    on_exit(fn ->
      safe_stop(server)
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

  test "falls back to context checkpoint mapping when designated table config omits checkpoint key",
       %{conn: conn} do
    context = %{
      meta_conn: self(),
      meta_module: FakeMeta,
      designated_table_by_target: %{{"raw", "events"} => "append-stream:raw.events"},
      designated_table_config_by_target: %{
        {"raw", "events"} => %{
          target_schema: "raw",
          target_table: "events"
        }
      },
      duckdb: %{conn: conn}
    }

    batch = %{
      rows: [%{"id" => 1, "kind" => "page_view"}],
      lsn_start: "0/50",
      lsn_end: "0/51"
    }

    assert {:ok, result} = DuckDB.process_batch(context, {"raw", "events"}, batch)
    assert result.checkpoint_key == "append-stream:raw.events"
    assert result.checkpoint_lsn == "0/51"
  end

  test "chunks large append batches into multiple source statements", %{conn: conn} do
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
      rows: Enum.map(1..501, fn id -> %{"id" => id, "kind" => "page_view"} end),
      lsn_start: "0/11",
      lsn_end: "0/12"
    }

    assert {:ok, result} = DuckDB.process_batch(context, {"raw", "events"}, batch)
    assert result.row_count == 501
    assert %{"n" => [501]} = query_map(conn, "SELECT count(*) AS n FROM raw.events")
  end

  test "does not duplicate duckdb rows when checkpoint persistence fails once", %{conn: conn} do
    context = %{
      meta_conn: self(),
      meta_module: FailOnceMeta,
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
      rows: [%{"id" => 1, "kind" => "page_view"}],
      lsn_start: "0/12",
      lsn_end: "0/13"
    }

    assert {:error, :forced_checkpoint_failure} =
             DuckDB.process_batch(context, {"raw", "events"}, batch)

    assert %{"n" => [1]} = query_map(conn, "SELECT count(*) AS n FROM raw.events")

    assert {:ok, result} = DuckDB.process_batch(context, {"raw", "events"}, batch)
    assert result.deduped?
    assert %{"n" => [1]} = query_map(conn, "SELECT count(*) AS n FROM raw.events")
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

  test "applies CDC batches after snapshot-created numeric columns", %{conn: conn} do
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

    snapshot_batch = %{
      rows: [
        %{"id" => 1, "name" => "alice"},
        %{"id" => 2, "name" => "bob"}
      ],
      lsn_start: "0/24",
      lsn_end: "0/25"
    }

    assert {:ok, _} = DuckDB.process_batch(context, {"raw", "users"}, snapshot_batch)

    cdc_batch = %{
      rows: [
        cdc_row("U", %{"id" => "1", "name" => "alice-2"}, %{"id" => "1", "name" => "alice"}),
        cdc_row("D", %{}, %{"id" => "2", "name" => "bob"}),
        cdc_row("I", %{"id" => "3", "name" => "carol"})
      ],
      lsn_start: "0/25",
      lsn_end: "0/26"
    }

    assert {:ok, result} = DuckDB.process_batch(context, {"raw", "users"}, cdc_batch)
    assert result.operation_counts == %{truncate: 0, deletes: 1, upserts: 2}

    assert %{"id" => [1, 3], "name" => ["alice-2", "carol"]} =
             query_map(conn, "SELECT id, name FROM raw.users ORDER BY id")
  end

  test "preserves numeric-looking strings in existing varchar columns during CDC merges", %{
    conn: conn
  } do
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

    snapshot_batch = %{
      rows: [
        %{"id" => 1, "code" => "seed"}
      ],
      lsn_start: "0/26",
      lsn_end: "0/27"
    }

    assert {:ok, _} = DuckDB.process_batch(context, {"raw", "users"}, snapshot_batch)

    cdc_batch = %{
      rows: [
        cdc_row("U", %{"id" => "1", "code" => "123"}, %{"id" => "1", "code" => "seed"}),
        cdc_row("I", %{"id" => "2", "code" => "456"})
      ],
      lsn_start: "0/27",
      lsn_end: "0/28"
    }

    assert {:ok, _} = DuckDB.process_batch(context, {"raw", "users"}, cdc_batch)

    assert %{"id" => [1, 2], "code" => ["123", "456"]} =
             query_map(conn, "SELECT id, code FROM raw.users ORDER BY id")
  end

  test "requires an explicit duckdb connection" do
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
      duckdb: %{}
    }

    batch = %{
      rows: [%{"id" => 1, "kind" => "page_view"}],
      lsn_start: "0/30",
      lsn_end: "0/31"
    }

    assert {:error, :missing_duckdb_conn} =
             DuckDB.process_batch(context, {"raw", "events"}, batch)
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

  test "reruns setup hooks after the duckdb connection dies" do
    context_base = %{
      meta_conn: self(),
      meta_module: FakeMeta,
      designated_table_by_target: %{{"raw", "events"} => "source-a:raw.events"},
      designated_table_config_by_target: %{
        {"raw", "events"} => %{
          checkpoint_key: "source-a:raw.events",
          target_schema: "raw",
          target_table: "events"
        }
      }
    }

    batch = %{
      rows: [%{"id" => 1, "kind" => "page_view"}],
      lsn_start: "0/42",
      lsn_end: "0/43"
    }

    {:ok, server_one} = DuckDBConnection.start_link(name: nil)
    conn_one = DuckDBConnection.get_conn(server_one)

    context_one =
      Map.put(context_base, :duckdb, %{
        conn: conn_one,
        setup_sql: ["CREATE SCHEMA IF NOT EXISTS raw"],
        setup_fun: fn _ ->
          send(self(), {:setup_fun_called, :one})
          :ok
        end
      })

    assert {:ok, _} = DuckDB.process_batch(context_one, {"raw", "events"}, batch)
    assert_receive {:setup_fun_called, :one}
    safe_stop(server_one)

    {:ok, server_two} = DuckDBConnection.start_link(name: nil)
    conn_two = DuckDBConnection.get_conn(server_two)

    context_two =
      Map.put(context_base, :duckdb, %{
        conn: conn_two,
        setup_sql: ["CREATE SCHEMA IF NOT EXISTS raw"],
        setup_fun: fn _ ->
          send(self(), {:setup_fun_called, :two})
          :ok
        end
      })

    on_exit(fn ->
      safe_stop(server_two)
    end)

    assert {:ok, _} =
             DuckDB.process_batch(context_two, {"raw", "events"}, %{batch | lsn_end: "0/44"})

    assert_receive {:setup_fun_called, :two}
  end

  defp cdc_row(op, record, old_record \\ %{}) do
    %{
      _op: op,
      _record: record,
      _old_record: old_record
    }
  end

  defp query_map(conn, sql) do
    {:ok, result} = DuckDBClient.query_map(conn, sql)
    result
  end

  defp safe_stop(pid) when is_pid(pid) do
    _ = GenServer.stop(pid)
    :ok
  catch
    :exit, _reason -> :ok
  end
end
