defmodule DuckFeeder.DuckDB.Connection do
  @moduledoc false

  def child_spec(opts) when is_list(opts) do
    %{
      id: {__MODULE__, Keyword.get(opts, :name, make_ref())},
      start: {__MODULE__, :start_link, [opts]}
    }
  end

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
        validate_opts(duckdb)

      {:ok, duckdb} when is_list(duckdb) ->
        if Keyword.keyword?(duckdb) do
          duckdb
          |> Map.new()
          |> validate_opts()
        else
          {:error, {:invalid_option, :duckdb, duckdb}}
        end

      {:ok, other} ->
        {:error, {:invalid_option, :duckdb, other}}

      :error ->
        {:ok, %{}}
    end
  end

  defp validate_opts(%{conn: conn} = duckdb) when is_pid(conn), do: {:ok, duckdb}
  defp validate_opts(%{conn: other}), do: {:error, {:invalid_duckdb_conn, other}}
  defp validate_opts(duckdb) when is_map(duckdb), do: {:ok, duckdb}
end
