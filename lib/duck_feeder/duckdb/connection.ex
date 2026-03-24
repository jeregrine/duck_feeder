defmodule DuckFeeder.DuckDB.Connection do
  @moduledoc false

  use GenServer

  @driver_download_key {__MODULE__, :duckdb_driver_downloaded}

  def start_link(opts \\ []) do
    case Keyword.get(opts, :name, __MODULE__) do
      nil ->
        GenServer.start_link(__MODULE__, Keyword.delete(opts, :name))

      name ->
        GenServer.start_link(__MODULE__, opts, name: name)
    end
  end

  def get_conn(server \\ __MODULE__) do
    GenServer.call(server, :get_conn)
  end

  @impl true
  def init(opts) do
    ensure_driver_downloaded!()

    db_opts =
      case Keyword.get(opts, :path) do
        nil -> []
        path -> [path: path]
      end

    {:ok, db} = Adbc.Database.start_link([driver: :duckdb] ++ db_opts)
    {:ok, conn} = Adbc.Connection.start_link(database: db)
    {:ok, %{db: db, conn: conn}}
  end

  @impl true
  def handle_call(:get_conn, _from, %{conn: conn} = state) do
    {:reply, conn, state}
  end

  defp ensure_driver_downloaded! do
    case :persistent_term.get(@driver_download_key, false) do
      true ->
        :ok

      false ->
        :ok = Adbc.download_driver!(:duckdb)
        :persistent_term.put(@driver_download_key, true)
        :ok
    end
  end
end
