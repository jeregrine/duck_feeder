defmodule DuckFeeder.Integration do
  @moduledoc """
  Convenience helpers for embedding DuckFeeder in an existing OTP supervision tree.
  """

  alias DuckFeeder.{Config, Runtime}

  @spec runtime_child_spec(term(), String.t(), map(), keyword()) :: Supervisor.child_spec()
  def runtime_child_spec(meta_conn, source_name, storage_config, opts \\ [])
      when is_binary(source_name) and is_map(storage_config) do
    child_opts = [
      name: Keyword.get(opts, :name),
      meta_conn: meta_conn,
      source_name: source_name,
      storage_config: storage_config,
      runtime_opts: Keyword.get(opts, :runtime_opts, []),
      start_reconciler?: Keyword.get(opts, :start_reconciler?, false),
      reconcile_opts: Keyword.get(opts, :reconcile_opts, []),
      reconciler_interval_ms: Keyword.get(opts, :reconciler_interval_ms),
      observer_pid: Keyword.get(opts, :observer_pid),
      meta_module: Keyword.get(opts, :meta_module),
      storage_module: Keyword.get(opts, :storage_module)
    ]

    Runtime.Supervisor.child_spec(Enum.reject(child_opts, fn {_k, v} -> is_nil(v) end))
  end

  @spec runtime_child_spec_from_config(term(), map() | keyword(), keyword()) ::
          {:ok, Supervisor.child_spec()} | {:error, term()}
  def runtime_child_spec_from_config(meta_conn, config, opts \\ []) do
    with {:ok, validated} <- Config.validate(config) do
      source_name = Keyword.get(opts, :source_name, Map.get(validated.source, :name, "default"))
      storage_config = Config.storage_config(validated)

      {:ok,
       runtime_child_spec(
         meta_conn,
         source_name,
         storage_config,
         opts
       )}
    end
  end
end
