defmodule DuckFeeder.DuckDB.Connection do
  @moduledoc false

  use GenServer

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

  def load_extension(extension, server \\ __MODULE__) when is_atom(extension) do
    conn = get_conn(server)
    ext = Atom.to_string(extension)
    Adbc.Connection.query!(conn, "INSTALL #{ext}; LOAD #{ext};")
    :ok
  end

  @impl true
  def init(opts) do
    Adbc.download_driver!(:duckdb)

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
end
