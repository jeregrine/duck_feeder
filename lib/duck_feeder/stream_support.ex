defmodule DuckFeeder.StreamSupport do
  @moduledoc false

  alias DuckFeeder.{DesignatedTable, Sink}
  alias DuckFeeder.DuckDB.Connection, as: DuckDBConnection

  @type table :: {String.t(), String.t()}

  @spec designated_table_mapping([map()], String.t() | nil) :: %{optional(table()) => String.t()}
  def designated_table_mapping(designated_tables, checkpoint_prefix \\ nil)
      when is_list(designated_tables) do
    Enum.reduce(designated_tables, %{}, fn designated_table, acc ->
      normalized_table = DesignatedTable.normalize(designated_table)
      target = DesignatedTable.target_relation(normalized_table)
      Map.put(acc, target, DesignatedTable.checkpoint_key(normalized_table, checkpoint_prefix))
    end)
  end

  @spec designated_table_config_mapping([map()]) :: %{optional(table()) => map()}
  def designated_table_config_mapping(designated_tables) when is_list(designated_tables) do
    Enum.reduce(designated_tables, %{}, fn designated_table, acc ->
      normalized_table = DesignatedTable.normalize(designated_table)
      target = DesignatedTable.target_relation(normalized_table)
      Map.put(acc, target, normalized_table)
    end)
  end

  @spec resolve_sink_module_option(keyword()) :: {:ok, module()} | {:error, term()}
  def resolve_sink_module_option(opts) when is_list(opts) do
    sink_module =
      Keyword.get(opts, :sink_module) ||
        implied_sink_module_from_duckdb(Keyword.get(opts, :duckdb))

    Sink.normalize_module(sink_module)
  end

  @spec resolve_duckdb(keyword(), module()) :: {:ok, map() | nil} | {:error, term()}
  def resolve_duckdb(opts, sink_module) when is_list(opts) and is_atom(sink_module) do
    case Keyword.fetch(opts, :duckdb) do
      {:ok, duckdb} when is_map(duckdb) ->
        maybe_start_duckdb_connection(duckdb, sink_module)

      {:ok, duckdb} when is_list(duckdb) ->
        if Keyword.keyword?(duckdb) do
          duckdb
          |> Map.new()
          |> maybe_start_duckdb_connection(sink_module)
        else
          {:error, {:invalid_option, :duckdb, duckdb}}
        end

      {:ok, other} ->
        {:error, {:invalid_option, :duckdb, other}}

      :error ->
        if sink_module == DuckFeeder.Sink.DuckDB do
          maybe_start_duckdb_connection(%{}, sink_module)
        else
          {:ok, nil}
        end
    end
  end

  @spec batch_result_status(term()) :: :ok | :error | :unknown
  def batch_result_status({:ok, _result}), do: :ok
  def batch_result_status({:error, _reason}), do: :error
  def batch_result_status(_result), do: :unknown

  @spec batch_queue_measurements(map()) :: map()
  def batch_queue_measurements(state) when is_map(state) do
    %{
      pending_batch_count: Map.fetch!(state, :pending_batch_count),
      inflight_batch_count: map_size(Map.fetch!(state, :inflight_batch_tasks)),
      max_pending_batches: Map.fetch!(state, :max_pending_batches),
      max_inflight_batches: Map.fetch!(state, :max_inflight_batches)
    }
  end

  @spec maybe_put_table_metadata(map()) :: map()
  def maybe_put_table_metadata(metadata) when is_map(metadata) do
    case Map.get(metadata, :table) do
      {schema, table} when is_binary(schema) and is_binary(table) ->
        metadata
        |> Map.put(:table_schema, schema)
        |> Map.put(:table_name, table)

      _ ->
        metadata
    end
  end

  @spec normalize_positive_integer(term(), atom()) :: {:ok, pos_integer()} | {:error, term()}
  def normalize_positive_integer(value, _key) when is_integer(value) and value > 0,
    do: {:ok, value}

  def normalize_positive_integer(value, key), do: {:error, {:invalid_option, key, value}}

  @spec maybe_put_optional(map(), atom(), term()) :: map()
  def maybe_put_optional(context, _key, nil), do: context
  def maybe_put_optional(context, key, value), do: Map.put(context, key, value)

  defp maybe_start_duckdb_connection(%{conn: conn} = duckdb, DuckFeeder.Sink.DuckDB)
       when is_pid(conn),
       do: {:ok, duckdb}

  defp maybe_start_duckdb_connection(%{conn: other}, DuckFeeder.Sink.DuckDB),
    do: {:error, {:invalid_duckdb_conn, other}}

  defp maybe_start_duckdb_connection(duckdb, DuckFeeder.Sink.DuckDB) when is_map(duckdb) do
    start_opts =
      [name: nil, path: Map.get(duckdb, :path)]
      |> Enum.reject(fn {key, value} -> is_nil(value) and key != :name end)

    with {:ok, server} <- DuckDBConnection.start_link(start_opts) do
      Process.unlink(server)
      :ok = bind_duckdb_server_to_owner(server, self())

      {:ok,
       duckdb
       |> Map.put(:server, server)
       |> Map.put(:conn, DuckDBConnection.get_conn(server))}
    else
      {:error, reason} -> {:error, {:duckdb_connection_start_failed, reason}}
    end
  end

  defp maybe_start_duckdb_connection(duckdb, _sink_module), do: {:ok, duckdb}

  defp bind_duckdb_server_to_owner(server, owner) when is_pid(server) and is_pid(owner) do
    _watcher = spawn(fn -> monitor_duckdb_server_owner(server, owner) end)
    :ok
  end

  defp monitor_duckdb_server_owner(server, owner) when is_pid(server) and is_pid(owner) do
    owner_ref = Process.monitor(owner)
    server_ref = Process.monitor(server)

    receive do
      {:DOWN, ^owner_ref, :process, ^owner, _reason} ->
        safe_stop(server)

      {:DOWN, ^server_ref, :process, ^server, _reason} ->
        :ok
    end
  end

  defp safe_stop(pid) when is_pid(pid) do
    _ = GenServer.stop(pid, :shutdown)
    :ok
  catch
    :exit, _reason -> :ok
  end

  defp safe_stop(_other), do: :ok

  defp implied_sink_module_from_duckdb(nil), do: nil
  defp implied_sink_module_from_duckdb(_duckdb), do: DuckFeeder.Sink.DuckDB
end
