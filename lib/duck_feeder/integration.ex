defmodule DuckFeeder.Integration do
  @moduledoc """
  Convenience helpers for embedding DuckFeeder in an existing OTP supervision tree.
  """

  alias DuckFeeder.{Config, Runtime}

  @spec runtime_child_spec(term(), String.t(), map() | nil, keyword()) :: Supervisor.child_spec()
  def runtime_child_spec(meta_conn, source_name, duckdb_config, opts \\ [])
      when is_binary(source_name) do
    child_opts = [
      name: Keyword.get(opts, :name),
      meta_conn: meta_conn,
      source_name: source_name,
      duckdb_config: duckdb_config,
      runtime_opts: Keyword.get(opts, :runtime_opts, []),
      observer_pid: Keyword.get(opts, :observer_pid)
    ]

    Runtime.Supervisor.child_spec(
      Enum.reject(child_opts, fn {key, value} -> is_nil(value) and key != :duckdb_config end)
    )
  end

  @spec runtime_child_spec_from_config(term(), map() | keyword(), keyword()) ::
          {:ok, Supervisor.child_spec()} | {:error, term()}
  def runtime_child_spec_from_config(meta_conn, config, opts \\ []) do
    with {:ok, validated} <- Config.validate(config) do
      source_name = Keyword.get(opts, :source_name, Map.get(validated.source, :name, "default"))
      duckdb_config = Config.duckdb_config(validated)

      {:ok,
       runtime_child_spec(
         meta_conn,
         source_name,
         duckdb_config,
         opts
       )}
    end
  end
end
