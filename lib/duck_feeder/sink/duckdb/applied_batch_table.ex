defmodule DuckFeeder.Sink.DuckDB.AppliedBatchTable do
  @moduledoc false

  @registry __MODULE__.Registry
  @conn_registry __MODULE__.ConnRegistry

  @spec ensure(pid(), term(), (-> :ok | {:error, term()})) :: :ok | {:error, term()}
  def ensure(conn, catalog, ensure_table_fun)
      when is_pid(conn) and is_function(ensure_table_fun, 0) do
    key = {conn, catalog}

    :ok = ensure_conn_monitor(conn)

    if table_ready?(key) do
      :ok
    else
      with :ok <- ensure_table_fun.() do
        remember_table(key)
      end
    end
  end

  defp table_ready?(key) do
    registry = ensure_registry()
    match?([{^key, true}], :ets.lookup(registry, key))
  end

  defp remember_table(key) do
    registry = ensure_registry()
    true = :ets.insert(registry, {key, true})
    :ok
  end

  defp ensure_conn_monitor(conn) when is_pid(conn) do
    registry = ensure_conn_registry()

    case :ets.lookup(registry, conn) do
      [{^conn, watcher}] when is_pid(watcher) ->
        if Process.alive?(watcher) do
          :ok
        else
          watcher = spawn(fn -> monitor_conn(conn) end)
          true = :ets.insert(registry, {conn, watcher})
          :ok
        end

      _ ->
        watcher = spawn(fn -> monitor_conn(conn) end)
        true = :ets.insert(registry, {conn, watcher})
        :ok
    end
  end

  defp monitor_conn(conn) do
    ref = Process.monitor(conn)

    receive do
      {:DOWN, ^ref, :process, ^conn, _reason} ->
        clear_entries(conn)
        clear_conn_monitor(conn)
    end
  end

  defp clear_entries(conn) when is_pid(conn) do
    case :ets.whereis(@registry) do
      :undefined ->
        :ok

      registry ->
        true = :ets.match_delete(registry, {{conn, :_}, :_})
        :ok
    end
  end

  defp clear_conn_monitor(conn) when is_pid(conn) do
    case :ets.whereis(@conn_registry) do
      :undefined ->
        :ok

      registry ->
        true = :ets.delete(registry, conn)
        :ok
    end
  end

  defp ensure_registry do
    case :ets.whereis(@registry) do
      :undefined ->
        try do
          :ets.new(@registry, [:named_table, :public, :set, read_concurrency: true])
        catch
          :error, :badarg -> @registry
        end

      registry ->
        registry
    end
  end

  defp ensure_conn_registry do
    case :ets.whereis(@conn_registry) do
      :undefined ->
        try do
          :ets.new(@conn_registry, [:named_table, :public, :set, read_concurrency: true])
        catch
          :error, :badarg -> @conn_registry
        end

      registry ->
        registry
    end
  end
end
