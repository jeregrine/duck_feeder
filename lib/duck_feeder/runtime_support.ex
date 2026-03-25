defmodule DuckFeeder.RuntimeSupport do
  @moduledoc false

  alias DuckFeeder.DesignatedTable
  alias DuckFeeder.DuckDB.{Connection, Init}

  @spec resolve_common_init([map()], keyword(), keyword()) :: {:ok, map()} | {:error, term()}
  def resolve_common_init(designated_tables, opts, defaults \\ [])
      when is_list(designated_tables) and is_list(opts) and is_list(defaults) do
    with {:ok, duckdb_opts} <- Connection.resolve_opts(opts),
         {:ok, duckdb} <- start_duckdb(duckdb_opts),
         :ok <- Init.initialize(duckdb),
         {:ok, batch_processor} <- normalize_batch_processor(Keyword.get(opts, :batch_processor)),
         {:ok, max_inflight_batches} <-
           normalize_positive_integer(
             Keyword.get(
               opts,
               :max_inflight_batches,
               Keyword.get(defaults, :max_inflight_batches, 1)
             ),
             :max_inflight_batches
           ),
         {:ok, max_pending_batches} <-
           normalize_positive_integer(
             Keyword.get(
               opts,
               :max_pending_batches,
               Keyword.get(defaults, :max_pending_batches, 1_000)
             ),
             :max_pending_batches
           ) do
      designated_tables_by_target =
        DesignatedTable.by_target(designated_tables, Keyword.get(defaults, :checkpoint_prefix))

      {:ok,
       %{
         duckdb: duckdb,
         observer_pid: Keyword.get(opts, :observer_pid, self()),
         max_inflight_batches: max_inflight_batches,
         max_pending_batches: max_pending_batches,
         context: %{
           meta_conn: Keyword.fetch!(opts, :meta_conn),
           designated_tables_by_target: designated_tables_by_target,
           duckdb: duckdb,
           meta_module: Keyword.get(opts, :meta_module),
           batch_processor: batch_processor
         }
       }}
    end
  end

  @spec normalize_positive_integer(term(), atom()) :: {:ok, pos_integer()} | {:error, term()}
  def normalize_positive_integer(value, _key) when is_integer(value) and value > 0,
    do: {:ok, value}

  def normalize_positive_integer(value, key), do: {:error, {:invalid_option, key, value}}

  defp start_duckdb(%{conn: conn} = duckdb) when is_pid(conn), do: {:ok, duckdb}

  defp start_duckdb(duckdb_opts) when is_map(duckdb_opts) do
    start_opts =
      [name: nil, path: Map.get(duckdb_opts, :path)]
      |> Enum.reject(fn {key, value} -> is_nil(value) and key != :name end)

    with {:ok, server} <- Connection.start_link(start_opts) do
      {:ok,
       duckdb_opts
       |> Map.put(:server, server)
       |> Map.put(:conn, Connection.get_conn(server))}
    else
      {:error, reason} -> {:error, {:duckdb_connection_start_failed, reason}}
    end
  end

  defp normalize_batch_processor(nil), do: {:ok, &DuckFeeder.Sink.DuckDB.process_batch/3}
  defp normalize_batch_processor(fun) when is_function(fun, 3), do: {:ok, fun}
  defp normalize_batch_processor(other), do: {:error, {:invalid_option, :batch_processor, other}}

  @spec normalize_optional_duckdb(map() | nil | term()) :: {:ok, map() | nil} | {:error, term()}
  def normalize_optional_duckdb(nil), do: {:ok, nil}
  def normalize_optional_duckdb(duckdb) when is_map(duckdb), do: {:ok, duckdb}
  def normalize_optional_duckdb(duckdb), do: {:error, {:invalid_option, :duckdb, duckdb}}
end
