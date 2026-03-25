defmodule DuckFeeder.DuckDB.Connection do
  @moduledoc false

  @owned_conn_registry __MODULE__.OwnedConnRegistry

  def start_link(opts \\ []) do
    case Keyword.get(opts, :name, __MODULE__) do
      nil ->
        GenServer.start_link(Dux.Connection, Keyword.delete(opts, :name))

      name ->
        GenServer.start_link(Dux.Connection, Keyword.delete(opts, :name), name: name)
    end
  end

  def get_conn(server \\ __MODULE__) do
    Dux.Connection.get_conn(server)
  end

  @spec resolve_opts(keyword()) :: {:ok, map()} | {:error, term()}
  def resolve_opts(opts) when is_list(opts) do
    case Keyword.fetch(opts, :duckdb) do
      {:ok, duckdb} when is_map(duckdb) ->
        maybe_start_owned_connection(duckdb)

      {:ok, duckdb} when is_list(duckdb) ->
        if Keyword.keyword?(duckdb) do
          duckdb
          |> Map.new()
          |> maybe_start_owned_connection()
        else
          {:error, {:invalid_option, :duckdb, duckdb}}
        end

      {:ok, other} ->
        {:error, {:invalid_option, :duckdb, other}}

      :error ->
        maybe_start_owned_connection(%{})
    end
  end

  defp maybe_start_owned_connection(%{conn: conn} = duckdb) when is_pid(conn), do: {:ok, duckdb}

  defp maybe_start_owned_connection(%{conn: other}), do: {:error, {:invalid_duckdb_conn, other}}

  defp maybe_start_owned_connection(duckdb) when is_map(duckdb) do
    start_opts =
      [name: nil, path: Map.get(duckdb, :path)]
      |> Enum.reject(fn {key, value} -> is_nil(value) and key != :name end)

    with {:ok, server} <- start_link(start_opts) do
      Process.unlink(server)
      :ok = bind_owned_server_to_owner(server, self())

      {:ok,
       duckdb
       |> Map.put(:server, server)
       |> Map.put(:conn, get_conn(server))}
    else
      {:error, reason} -> {:error, {:duckdb_connection_start_failed, reason}}
    end
  end

  defp bind_owned_server_to_owner(server, owner) when is_pid(server) and is_pid(owner) do
    registry = ensure_owned_conn_registry()

    case :ets.lookup(registry, server) do
      [{^server, watcher}] when is_pid(watcher) ->
        if Process.alive?(watcher) do
          :ok
        else
          watcher = spawn(fn -> monitor_owned_server_owner(server, owner) end)
          true = :ets.insert(registry, {server, watcher})
          :ok
        end

      _ ->
        watcher = spawn(fn -> monitor_owned_server_owner(server, owner) end)
        true = :ets.insert(registry, {server, watcher})
        :ok
    end
  end

  defp monitor_owned_server_owner(server, owner) when is_pid(server) and is_pid(owner) do
    owner_ref = Process.monitor(owner)
    server_ref = Process.monitor(server)

    receive do
      {:DOWN, ^owner_ref, :process, ^owner, _reason} ->
        clear_owned_conn_watcher(server)
        Process.exit(server, :shutdown)
        :ok

      {:DOWN, ^server_ref, :process, ^server, _reason} ->
        clear_owned_conn_watcher(server)
        :ok
    end
  end

  defp ensure_owned_conn_registry do
    case :ets.whereis(@owned_conn_registry) do
      :undefined ->
        try do
          :ets.new(@owned_conn_registry, [:named_table, :public, :set, read_concurrency: true])
        catch
          :error, :badarg -> @owned_conn_registry
        end

      registry ->
        registry
    end
  end

  defp clear_owned_conn_watcher(server) when is_pid(server) do
    case :ets.whereis(@owned_conn_registry) do
      :undefined ->
        :ok

      registry ->
        true = :ets.delete(registry, server)
        :ok
    end
  end
end
