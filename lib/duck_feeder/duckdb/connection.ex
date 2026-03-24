defmodule DuckFeeder.DuckDB.Connection do
  @moduledoc false

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
end
