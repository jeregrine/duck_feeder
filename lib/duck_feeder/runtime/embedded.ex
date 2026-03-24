defmodule DuckFeeder.Runtime.Embedded do
  @moduledoc """
  Repo/schema-first managed runtime wrapper used by `use DuckFeeder.Runtime` modules.

  Startup flow:

      app config (repo/schemas or explicit config)
          |
          v
      DuckFeeder.Runtime.resolve_app_config/1
          |
          v
      Postgrex metadata connection
          |
          v
      DuckFeeder.seed_meta/3
          |
          v
      DuckFeeder.Runtime.Supervisor (stream worker)
  """

  use GenServer

  alias DuckFeeder.CDC.ConnectionOptions
  alias DuckFeeder.Runtime.Shared

  defmodule State do
    @enforce_keys [:module, :otp_app, :config]
    defstruct [
      :module,
      :otp_app,
      :config,
      :meta_conn,
      :runtime_supervisor,
      enabled?: false,
      monitors: %{}
    ]
  end

  @type option ::
          {:name, GenServer.name()}
          | {:module, module()}
          | {:otp_app, atom()}
          | {:start_opts, keyword()}

  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts) do
    {name_opt, opts} = Keyword.pop(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name_opt)
  end

  @spec runtime_info(GenServer.server()) :: {:ok, map()}
  def runtime_info(server), do: GenServer.call(server, :runtime_info)

  @impl true
  def init(opts) do
    module = Keyword.fetch!(opts, :module)
    otp_app = Keyword.fetch!(opts, :otp_app)
    start_opts = Keyword.get(opts, :start_opts, [])

    config =
      module
      |> apply(:duckfeeder_config, [])
      |> merge_start_opts(start_opts)

    with {:ok, state} <- boot_runtime(module, otp_app, config) do
      {:ok, state}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call(:runtime_info, _from, %State{} = state) do
    {:reply,
     {:ok,
      %{
        enabled?: state.enabled?,
        config: state.config,
        meta_conn: state.meta_conn,
        runtime_supervisor: state.runtime_supervisor
      }}, state}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, pid, reason}, %State{monitors: monitors} = state) do
    if Map.has_key?(monitors, ref) do
      {:stop, {:runtime_child_down, pid, reason}, state}
    else
      {:noreply, state}
    end
  end

  @impl true
  def terminate(_reason, %State{} = state) do
    stop_if_alive(state.runtime_supervisor)
    stop_if_alive(state.meta_conn)
    :ok
  end

  defp boot_runtime(module, otp_app, config) do
    with {:ok, resolved} <- DuckFeeder.Runtime.resolve_app_config(config) do
      if resolved.enabled? do
        runtime_opts =
          resolved.runtime_opts ++
            [source: resolved.source, designated_tables: resolved.designated_tables]

        with {:ok, meta_conn} <-
               start_metadata_connection(resolved.validated_config.metadata.postgres_url),
             {:ok, _seed} <-
               DuckFeeder.Bootstrap.seed_meta_validated(meta_conn, resolved.validated_config,
                 source_name: resolved.source_name
               ),
             {:ok, runtime_supervisor} <-
               DuckFeeder.Runtime.Supervisor.start_link(
                 meta_conn: meta_conn,
                 source_name: resolved.source_name,
                 duckdb: resolved.duckdb,
                 runtime_opts: runtime_opts
               ) do
          monitors = %{
            Process.monitor(meta_conn) => meta_conn,
            Process.monitor(runtime_supervisor) => runtime_supervisor
          }

          {:ok,
           %State{
             module: module,
             otp_app: otp_app,
             config: resolved,
             meta_conn: meta_conn,
             runtime_supervisor: runtime_supervisor,
             enabled?: true,
             monitors: monitors
           }}
        else
          {:error, reason} -> {:error, reason}
        end
      else
        {:ok, %State{module: module, otp_app: otp_app, config: resolved, enabled?: false}}
      end
    end
  end

  defp start_metadata_connection(postgres_url) when is_binary(postgres_url) do
    with {:ok, opts} <- ConnectionOptions.parse_url(postgres_url),
         {:ok, conn} <- Postgrex.start_link(opts ++ [types: DuckFeeder.Postgrex.Types]) do
      {:ok, conn}
    end
  end

  defp merge_start_opts(config, start_opts) do
    config
    |> Shared.mapify()
    |> Map.merge(Shared.mapify(start_opts))
  end

  defp stop_if_alive(pid) when is_pid(pid) do
    if Process.alive?(pid), do: GenServer.stop(pid, :shutdown)
  catch
    :exit, _ -> :ok
  end

  defp stop_if_alive(_), do: :ok
end
