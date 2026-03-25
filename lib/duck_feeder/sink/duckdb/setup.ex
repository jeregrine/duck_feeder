defmodule DuckFeeder.Sink.DuckDB.Setup do
  @moduledoc false

  @setup_registry __MODULE__.Registry
  @setup_conn_registry __MODULE__.ConnRegistry

  @spec ensure(pid(), map(), (String.t() -> :ok | {:error, term()})) :: :ok | {:error, term()}
  def ensure(conn, duckdb, execute_sql)
      when is_pid(conn) and is_map(duckdb) and is_function(execute_sql, 1) do
    key = setup_key(conn, duckdb)

    :ok = ensure_setup_conn_monitor(conn)

    if setup_complete?(key) do
      :ok
    else
      with :ok <- execute_setup_sql(execute_sql, Map.get(duckdb, :setup_sql, [])),
           :ok <- execute_setup_fun(conn, Map.get(duckdb, :setup_fun)) do
        remember_setup(key)
      end
    end
  end

  defp execute_setup_sql(_execute_sql, []), do: :ok

  defp execute_setup_sql(execute_sql, statements) when is_list(statements) do
    Enum.reduce_while(statements, :ok, fn statement, :ok ->
      case execute_sql.(statement) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp execute_setup_sql(_execute_sql, other), do: {:error, {:invalid_duckdb_setup_sql, other}}

  defp execute_setup_fun(_conn, nil), do: :ok

  defp execute_setup_fun(conn, fun) when is_function(fun, 1) do
    case fun.(conn) do
      :ok -> :ok
      {:error, _reason} = error -> error
      other -> {:error, {:invalid_duckdb_setup_fun_result, other}}
    end
  end

  defp execute_setup_fun(_conn, other), do: {:error, {:invalid_duckdb_setup_fun, other}}

  defp setup_key(conn, duckdb) when is_pid(conn) and is_map(duckdb) do
    {conn, Map.get(duckdb, :setup_sql, []), Map.get(duckdb, :setup_fun)}
  end

  defp setup_complete?(key) do
    registry = ensure_setup_registry()
    match?([{^key, true}], :ets.lookup(registry, key))
  end

  defp remember_setup(key) do
    registry = ensure_setup_registry()
    true = :ets.insert(registry, {key, true})
    :ok
  end

  defp ensure_setup_conn_monitor(conn) when is_pid(conn) do
    registry = ensure_setup_conn_registry()

    case :ets.lookup(registry, conn) do
      [{^conn, watcher}] when is_pid(watcher) ->
        if Process.alive?(watcher) do
          :ok
        else
          watcher = spawn(fn -> monitor_setup_conn(conn) end)
          true = :ets.insert(registry, {conn, watcher})
          :ok
        end

      _ ->
        watcher = spawn(fn -> monitor_setup_conn(conn) end)
        true = :ets.insert(registry, {conn, watcher})
        :ok
    end
  end

  defp monitor_setup_conn(conn) do
    ref = Process.monitor(conn)

    receive do
      {:DOWN, ^ref, :process, ^conn, _reason} ->
        clear_setup_entries(conn)
        clear_setup_conn_monitor(conn)
    end
  end

  defp clear_setup_entries(conn) when is_pid(conn) do
    case :ets.whereis(@setup_registry) do
      :undefined ->
        :ok

      registry ->
        true = :ets.match_delete(registry, {{conn, :_, :_}, :_})
        :ok
    end
  end

  defp clear_setup_conn_monitor(conn) when is_pid(conn) do
    case :ets.whereis(@setup_conn_registry) do
      :undefined ->
        :ok

      registry ->
        true = :ets.delete(registry, conn)
        :ok
    end
  end

  defp ensure_setup_registry do
    case :ets.whereis(@setup_registry) do
      :undefined ->
        try do
          :ets.new(@setup_registry, [:named_table, :public, :set, read_concurrency: true])
        catch
          :error, :badarg -> @setup_registry
        end

      registry ->
        registry
    end
  end

  defp ensure_setup_conn_registry do
    case :ets.whereis(@setup_conn_registry) do
      :undefined ->
        try do
          :ets.new(@setup_conn_registry, [:named_table, :public, :set, read_concurrency: true])
        catch
          :error, :badarg -> @setup_conn_registry
        end

      registry ->
        registry
    end
  end
end
