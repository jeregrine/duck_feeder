defmodule DuckFeeder.DuckLake.Committer.Postgres do
  @moduledoc """
  Transactional DuckLake committer scaffold for Postgres-backed metadata.

  Executes DuckLake SQL statements and `commit_uploaded_batch_tx/2` inside the
  same transaction.
  """

  @behaviour DuckFeeder.DuckLake.Committer

  alias DuckFeeder.Meta
  alias DuckFeeder.DuckLake.SQL

  @type statement :: String.t() | {String.t(), list()}

  @impl true
  def commit_batch(meta_conn, batch_id, opts) when is_binary(batch_id) do
    meta_module = Keyword.get(opts, :meta_module, Meta)
    sql_module = Keyword.get(opts, :sql_module, SQL)
    transaction_fun = Keyword.get(opts, :transaction_fun, &Postgrex.transaction/2)
    query_fun = Keyword.get(opts, :query_fun, &Postgrex.query/3)
    rollback_fun = Keyword.get(opts, :rollback_fun, &Postgrex.rollback/2)

    statements = sql_module.commit_statements(batch_id, opts)

    case transaction_fun.(meta_conn, fn tx_conn ->
           with :ok <- execute_statements(tx_conn, statements, query_fun),
                {:ok, commit_result} <- meta_module.commit_uploaded_batch_tx(tx_conn, batch_id) do
             commit_result
           else
             {:error, reason} -> rollback_fun.(tx_conn, reason)
           end
         end) do
      {:ok, result} -> {:ok, result}
      {:error, reason} -> {:error, reason}
    end
  end

  defp execute_statements(_conn, [], _query_fun), do: :ok

  defp execute_statements(conn, [statement | rest], query_fun) do
    with :ok <- execute_statement(conn, statement, query_fun) do
      execute_statements(conn, rest, query_fun)
    end
  end

  defp execute_statement(conn, sql, query_fun) when is_binary(sql) do
    case query_fun.(conn, sql, []) do
      {:ok, _result} -> :ok
      {:error, reason} -> {:error, {:ducklake_sql_failed, sql, reason}}
    end
  end

  defp execute_statement(conn, {sql, params}, query_fun)
       when is_binary(sql) and is_list(params) do
    case query_fun.(conn, sql, params) do
      {:ok, _result} -> :ok
      {:error, reason} -> {:error, {:ducklake_sql_failed, sql, reason}}
    end
  end

  defp execute_statement(_conn, statement, _query_fun),
    do: {:error, {:invalid_ducklake_statement, statement}}
end
