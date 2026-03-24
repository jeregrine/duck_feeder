defmodule DuckFeeder.Runtime.Supervisor do
  @moduledoc """
  Supervises a runtime stream worker and optional reconciler worker.
  """

  use Supervisor

  @type option ::
          {:name, Supervisor.name()}
          | {:meta_conn, term()}
          | {:source_name, String.t()}
          | {:storage_config, map() | nil}
          | {:runtime_opts, keyword()}
          | {:runtime_module, module()}
          | {:stream_worker_module, module()}
          | {:stream_worker_name, GenServer.name()}
          | {:start_reconciler?, boolean()}
          | {:reconciler_worker_module, module()}
          | {:reconciler_name, GenServer.name()}
          | {:reconciler_module, module()}
          | {:reconciler_interval_ms, pos_integer()}
          | {:reconciler_run_on_start?, boolean()}
          | {:reconcile_opts, keyword()}
          | {:reconciler_context, map()}
          | {:observer_pid, pid()}
          | {:meta_module, module()}
          | {:storage_module, module()}

  @spec start_link([option()]) :: Supervisor.on_start()
  def start_link(opts) do
    {name_opt, opts} = Keyword.pop(opts, :name)
    Supervisor.start_link(__MODULE__, opts, name: name_opt)
  end

  @impl true
  def init(opts) do
    meta_conn = Keyword.fetch!(opts, :meta_conn)
    source_name = Keyword.fetch!(opts, :source_name)
    storage_config = Keyword.fetch!(opts, :storage_config)

    stream_worker_module =
      Keyword.get(opts, :stream_worker_module, DuckFeeder.Runtime.StreamWorker)

    stream_worker_child =
      {stream_worker_module,
       [
         name: Keyword.get(opts, :stream_worker_name),
         meta_conn: meta_conn,
         source_name: source_name,
         storage_config: storage_config,
         runtime_opts: Keyword.get(opts, :runtime_opts, []),
         runtime_module: Keyword.get(opts, :runtime_module),
         observer_pid: Keyword.get(opts, :observer_pid)
       ]
       |> Enum.reject(fn {key, value} -> is_nil(value) and key != :storage_config end)}

    children =
      if Keyword.get(opts, :start_reconciler?, false) do
        [stream_worker_child, reconciler_child(meta_conn, storage_config, opts)]
      else
        [stream_worker_child]
      end

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp reconciler_child(meta_conn, storage_config, opts) do
    reconciler_worker_module =
      Keyword.get(opts, :reconciler_worker_module, DuckFeeder.Reconciler.Worker)

    context =
      opts
      |> Keyword.get(:reconciler_context, %{})
      |> Map.new()
      |> Map.put_new(:meta_conn, meta_conn)
      |> Map.put_new(:storage, storage_config)
      |> maybe_put_context(:meta_module, Keyword.get(opts, :meta_module))
      |> maybe_put_context(:storage_module, Keyword.get(opts, :storage_module))

    {reconciler_worker_module,
     [
       name: Keyword.get(opts, :reconciler_name),
       context: context,
       interval_ms: Keyword.get(opts, :reconciler_interval_ms, 60_000),
       run_on_start?: Keyword.get(opts, :reconciler_run_on_start?, true),
       reconcile_opts: Keyword.get(opts, :reconcile_opts, []),
       reconciler_module: Keyword.get(opts, :reconciler_module),
       observer_pid: Keyword.get(opts, :observer_pid)
     ]
     |> Enum.reject(fn {_k, v} -> is_nil(v) end)}
  end

  defp maybe_put_context(context, _key, nil), do: context
  defp maybe_put_context(context, key, value), do: Map.put(context, key, value)
end
