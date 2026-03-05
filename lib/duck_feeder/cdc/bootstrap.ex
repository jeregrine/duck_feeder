defmodule DuckFeeder.CDC.Bootstrap do
  @moduledoc """
  High-level replication bootstrap helper.

  Ensures publication and slot, fetches current WAL LSN, and prepares
  the START_REPLICATION SQL statement.
  """

  alias DuckFeeder.CDC.{ReplicationProtocol, Setup}

  @type bootstrap_result :: %{
          publication: :exists | :created,
          slot: :exists | {:created, %{slot_name: String.t(), lsn: String.t()}},
          start_lsn: String.t(),
          current_lsn: String.t(),
          start_replication_sql: String.t()
        }

  @spec bootstrap(pid(), map(), keyword()) :: {:ok, bootstrap_result()} | {:error, term()}
  def bootstrap(conn, attrs, opts \\ []) when is_map(attrs) do
    setup_module = Keyword.get(opts, :setup_module, Setup)
    query_fun = Keyword.get(opts, :query_fun, &Postgrex.query/3)

    with {:ok, publication_name} <- fetch_required(attrs, :publication_name),
         {:ok, slot_name} <- fetch_required(attrs, :slot_name),
         {:ok, designated_tables} <- fetch_required(attrs, :designated_tables),
         plugin <- Map.get(attrs, :plugin, "pgoutput"),
         :ok <-
           maybe_ensure_replica_identity_full(
             setup_module,
             conn,
             designated_tables,
             query_fun,
             opts
           ),
         {:ok, publication_result} <-
           setup_module.ensure_publication(conn, publication_name, designated_tables,
             query_fun: query_fun
           ),
         {:ok, slot_result} <-
           setup_module.ensure_slot(conn, slot_name, plugin, query_fun: query_fun),
         {:ok, current_lsn} <- fetch_current_lsn(conn, query_fun),
         {:ok, start_lsn} <- resolve_start_lsn(slot_result, current_lsn) do
      {:ok,
       %{
         publication: publication_result,
         slot: slot_result,
         start_lsn: start_lsn,
         current_lsn: current_lsn,
         start_replication_sql:
           ReplicationProtocol.start_replication_sql(slot_name, start_lsn, publication_name)
       }}
    end
  end

  @spec fetch_current_lsn(pid(), Setup.query_fun()) :: {:ok, String.t()} | {:error, term()}
  def fetch_current_lsn(conn, query_fun \\ &Postgrex.query/3) do
    case query_fun.(conn, "SELECT pg_current_wal_lsn()::text", []) do
      {:ok, %Postgrex.Result{rows: [[lsn]]}} when is_binary(lsn) -> {:ok, lsn}
      {:ok, %Postgrex.Result{rows: rows}} -> {:error, {:unexpected_current_lsn_rows, rows}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp resolve_start_lsn({:created, %{lsn: lsn}}, _current_lsn) when is_binary(lsn),
    do: {:ok, lsn}

  defp resolve_start_lsn(:exists, current_lsn) when is_binary(current_lsn), do: {:ok, current_lsn}
  defp resolve_start_lsn(other, _current_lsn), do: {:error, {:invalid_slot_result, other}}

  defp maybe_ensure_replica_identity_full(setup_module, conn, designated_tables, query_fun, opts)
       when is_atom(setup_module) do
    if Keyword.get(opts, :enforce_replica_identity_full?, true) and
         function_exported?(setup_module, :ensure_replica_identity_full, 3) do
      setup_module.ensure_replica_identity_full(conn, designated_tables, query_fun: query_fun)
    else
      :ok
    end
  end

  defp fetch_required(attrs, key) do
    case Map.get(attrs, key) do
      nil -> {:error, {:missing_required, key}}
      "" -> {:error, {:missing_required, key}}
      value -> {:ok, value}
    end
  end
end
