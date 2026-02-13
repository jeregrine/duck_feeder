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
