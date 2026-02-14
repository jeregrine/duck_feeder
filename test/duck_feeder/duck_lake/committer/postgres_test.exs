defmodule DuckFeeder.DuckLake.Committer.PostgresTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.DuckLake.Committer.Postgres

  defmodule FakeMeta do
    def commit_uploaded_batch_tx(_conn, batch_id) do
      if pid = Process.get(:test_pid), do: send(pid, {:meta_commit_uploaded_batch_tx, batch_id})

      {:ok,
       %{
         batch_id: batch_id,
         designated_table_id: 7,
         checkpoint_lsn: "0/20",
         committed?: true,
         already_committed?: false,
         batch_lsn_end: "0/20"
       }}
    end
  end

  defmodule FakeSQL do
    def commit_statements(_batch_id, _opts), do: ["SELECT 1", {"SELECT $1", [1]}]
  end

  defmodule FakeSQLInvalid do
    def commit_statements(_batch_id, _opts), do: [:bad_statement]
  end

  setup do
    Process.put(:test_pid, self())
    :ok
  end

  test "runs sql statements and commits uploaded batch in one transaction" do
    tx_fun = fn conn, fun ->
      send(self(), {:tx_conn, conn})

      try do
        {:ok, fun.(:tx_conn)}
      catch
        {:rollback, reason} -> {:error, reason}
      end
    end

    query_fun = fn conn, sql, params ->
      send(self(), {:query, conn, sql, params})
      {:ok, %Postgrex.Result{rows: []}}
    end

    rollback_fun = fn _conn, reason -> throw({:rollback, reason}) end

    assert {:ok, %{batch_id: "batch-1", checkpoint_lsn: "0/20"}} =
             Postgres.commit_batch(:meta_conn, "batch-1",
               meta_module: FakeMeta,
               sql_module: FakeSQL,
               transaction_fun: tx_fun,
               query_fun: query_fun,
               rollback_fun: rollback_fun
             )

    assert_received {:tx_conn, :meta_conn}
    assert_received {:query, :tx_conn, "SELECT 1", []}
    assert_received {:query, :tx_conn, "SELECT $1", [1]}
    assert_received {:meta_commit_uploaded_batch_tx, "batch-1"}
  end

  test "rolls back transaction when sql execution fails" do
    tx_fun = fn _conn, fun ->
      try do
        {:ok, fun.(:tx_conn)}
      catch
        {:rollback, reason} -> {:error, reason}
      end
    end

    query_fun = fn _conn, sql, _params ->
      {:error, {:query_failed, sql}}
    end

    rollback_fun = fn _conn, reason -> throw({:rollback, reason}) end

    assert {:error, {:ducklake_sql_failed, "SELECT 1", {:query_failed, "SELECT 1"}}} =
             Postgres.commit_batch(:meta_conn, "batch-1",
               meta_module: FakeMeta,
               sql_module: FakeSQL,
               transaction_fun: tx_fun,
               query_fun: query_fun,
               rollback_fun: rollback_fun
             )

    refute_received {:meta_commit_uploaded_batch_tx, _}
  end

  test "uses default ducklake spec-aligned statements" do
    tx_fun = fn _conn, fun ->
      try do
        {:ok, fun.(:tx_conn)}
      catch
        {:rollback, reason} -> {:error, reason}
      end
    end

    query_fun = fn _conn, sql, params ->
      send(self(), {:query, sql, params})
      {:ok, %Postgrex.Result{rows: []}}
    end

    rollback_fun = fn _conn, reason -> throw({:rollback, reason}) end

    assert {:ok, %{batch_id: "batch-1"}} =
             Postgres.commit_batch(:meta_conn, "batch-1",
               meta_module: FakeMeta,
               transaction_fun: tx_fun,
               query_fun: query_fun,
               rollback_fun: rollback_fun,
               object_key: "raw/users/file-1.parquet",
               write_result: %{row_count: 10, file_size_bytes: 99},
               batch: %{rows: [%{"id" => 1, "name" => "duck"}]}
             )

    assert_received {:meta_commit_uploaded_batch_tx, "batch-1"}

    queries =
      Stream.repeatedly(fn ->
        receive do
          {:query, sql, params} -> {sql, params}
        after
          10 -> :done
        end
      end)
      |> Enum.take_while(&(&1 != :done))

    assert Enum.any?(queries, fn {sql, _} ->
             sql =~ "INSERT INTO ducklake_metadata.ducklake_snapshot"
           end)

    assert Enum.any?(queries, fn {sql, _} ->
             sql =~ "INSERT INTO ducklake_metadata.ducklake_table"
           end)

    assert Enum.any?(queries, fn {sql, _} ->
             sql =~ "INSERT INTO ducklake_metadata.ducklake_data_file"
           end)

    assert Enum.any?(queries, fn {sql, _} ->
             sql =~ "INSERT INTO ducklake_metadata.ducklake_table_stats"
           end)

    assert Enum.any?(queries, fn {sql, _} ->
             sql =~ "INSERT INTO ducklake_metadata.ducklake_table_column_stats"
           end)

    assert Enum.any?(queries, fn {sql, _} ->
             sql =~ "INSERT INTO ducklake_metadata.ducklake_file_column_stats"
           end)

    assert Enum.any?(queries, fn {sql, _} ->
             sql =~ "INSERT INTO ducklake_metadata.ducklake_snapshot_changes"
           end)

    assert Enum.any?(queries, fn {sql, _} ->
             sql =~ "INSERT INTO duckfeeder_meta.schema_history"
           end)

    assert Enum.any?(queries, fn {sql, _} ->
             sql =~ "INSERT INTO duckfeeder_meta.ducklake_commits"
           end)
  end

  test "executes delete-file and replacement metadata SQL when configured" do
    tx_fun = fn _conn, fun ->
      try do
        {:ok, fun.(:tx_conn)}
      catch
        {:rollback, reason} -> {:error, reason}
      end
    end

    query_fun = fn _conn, sql, params ->
      send(self(), {:query, sql, params})
      {:ok, %Postgrex.Result{rows: []}}
    end

    rollback_fun = fn _conn, reason -> throw({:rollback, reason}) end

    assert {:ok, %{batch_id: "batch-1"}} =
             Postgres.commit_batch(:meta_conn, "batch-1",
               meta_module: FakeMeta,
               transaction_fun: tx_fun,
               query_fun: query_fun,
               rollback_fun: rollback_fun,
               object_key: "raw/users/file-1.parquet",
               write_result: %{row_count: 10, file_size_bytes: 99},
               batch: %{rows: [%{"id" => 1, "name" => "duck"}]},
               delete_files: [
                 %{path: "raw/users/file-1-deletes.parquet", data_file_id: 77, delete_count: 1}
               ],
               replace_data_file_ids: [77]
             )

    queries =
      Stream.repeatedly(fn ->
        receive do
          {:query, sql, params} -> {sql, params}
        after
          10 -> :done
        end
      end)
      |> Enum.take_while(&(&1 != :done))

    assert Enum.any?(queries, fn {sql, _} ->
             sql =~ "INSERT INTO ducklake_metadata.ducklake_delete_file"
           end)

    assert Enum.any?(queries, fn {sql, _} ->
             sql =~ "UPDATE ducklake_metadata.ducklake_data_file"
           end)

    assert Enum.any?(queries, fn {sql, _} ->
             sql =~ "UPDATE ducklake_metadata.ducklake_delete_file"
           end)

    assert Enum.any?(queries, fn {sql, _} ->
             sql =~ "INSERT INTO ducklake_metadata.ducklake_files_scheduled_for_deletion"
           end)
  end

  test "executes schema-change SQL when configured" do
    tx_fun = fn _conn, fun ->
      try do
        {:ok, fun.(:tx_conn)}
      catch
        {:rollback, reason} -> {:error, reason}
      end
    end

    query_fun = fn _conn, sql, params ->
      send(self(), {:query, sql, params})
      {:ok, %Postgrex.Result{rows: []}}
    end

    rollback_fun = fn _conn, reason -> throw({:rollback, reason}) end

    assert {:ok, %{batch_id: "batch-1"}} =
             Postgres.commit_batch(:meta_conn, "batch-1",
               meta_module: FakeMeta,
               transaction_fun: tx_fun,
               query_fun: query_fun,
               rollback_fun: rollback_fun,
               object_key: "raw/users/file-1.parquet",
               write_result: %{row_count: 1, file_size_bytes: 99},
               batch: %{rows: [%{"value" => 1}]},
               schema_changes: [
                 %{op: :rename_column, from: "value", to: "metric"},
                 %{op: :drop_column, column: "legacy"},
                 %{op: :alter_column_type, column: "metric", type: "DOUBLE"}
               ]
             )

    queries =
      Stream.repeatedly(fn ->
        receive do
          {:query, sql, params} -> {sql, params}
        after
          10 -> :done
        end
      end)
      |> Enum.take_while(&(&1 != :done))

    assert Enum.any?(queries, fn {sql, _} ->
             sql =~ "UPDATE ducklake_metadata.ducklake_column col" and sql =~ "SET column_name ="
           end)

    assert Enum.any?(queries, fn {sql, _} ->
             sql =~ "DELETE FROM ducklake_metadata.ducklake_name_mapping"
           end)

    assert Enum.any?(queries, fn {sql, _} ->
             sql =~ "SET column_type ="
           end)
  end

  test "returns error for invalid statement shape" do
    tx_fun = fn _conn, fun ->
      try do
        {:ok, fun.(:tx_conn)}
      catch
        {:rollback, reason} -> {:error, reason}
      end
    end

    rollback_fun = fn _conn, reason -> throw({:rollback, reason}) end

    assert {:error, {:invalid_ducklake_statement, :bad_statement}} =
             Postgres.commit_batch(:meta_conn, "batch-1",
               meta_module: FakeMeta,
               sql_module: FakeSQLInvalid,
               transaction_fun: tx_fun,
               query_fun: fn _, _, _ -> {:ok, %Postgrex.Result{rows: []}} end,
               rollback_fun: rollback_fun
             )
  end
end
