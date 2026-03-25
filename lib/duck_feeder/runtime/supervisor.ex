defmodule DuckFeeder.Runtime.Supervisor do
  @moduledoc """
  Supervises a runtime stream worker.
  """

  use Supervisor

  @type option ::
          {:name, Supervisor.name()}
          | {:meta_conn, term()}
          | {:source_name, String.t()}
          | {:duckdb, map() | nil}
          | {:runtime_opts, keyword()}
          | {:runtime_module, module()}
          | {:stream_worker_module, module()}
          | {:stream_worker_name, GenServer.name()}
          | {:observer_pid, pid()}

  @spec start_link([option()]) :: Supervisor.on_start()
  def start_link(opts) do
    {name_opt, opts} = Keyword.pop(opts, :name)
    Supervisor.start_link(__MODULE__, opts, name: name_opt)
  end

  @impl true
  def init(opts) do
    meta_conn = Keyword.fetch!(opts, :meta_conn)
    source_name = Keyword.fetch!(opts, :source_name)
    duckdb = Keyword.fetch!(opts, :duckdb)

    stream_worker_module =
      Keyword.get(opts, :stream_worker_module, DuckFeeder.Runtime.StreamWorker)

    child_opts =
      [
        name: Keyword.get(opts, :stream_worker_name),
        meta_conn: meta_conn,
        source_name: source_name,
        runtime_opts: Keyword.get(opts, :runtime_opts, []),
        runtime_module: Keyword.get(opts, :runtime_module),
        observer_pid: Keyword.get(opts, :observer_pid)
      ]
      |> Keyword.put(:duckdb, duckdb)
      |> Enum.reject(fn {_key, value} -> is_nil(value) end)

    children = [
      {stream_worker_module, child_opts}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
