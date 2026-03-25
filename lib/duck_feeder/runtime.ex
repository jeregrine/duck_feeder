defmodule DuckFeeder.Runtime do
  @moduledoc """
  Runtime wiring helpers to start `DuckFeeder.Service` from app config.

  High-level startup flow:

      app config / Ecto-derived source + designated tables
                |
                v
      resolve start_lsn + snapshot handoff plan
                |
                v
      start Service
                |
                v
      (optional) replay snapshot rows into Service ingest path
                |
                v
      start CDC.Connection
                |
                v
      attach_cdc(Service, cdc_pid) for durable ack feedback

  The runtime keeps restart correctness centered on persisted checkpoint/snapshot
  handoff state and only acknowledges WAL progression after committed batch
  checkpoints are available.

  App-facing wrapper mode is also available:

      defmodule MyApp.DuckFeeder do
        use DuckFeeder.Runtime, otp_app: :my_app
      end

  In this mode, DuckFeeder can resolve config from `repo` + `schemas` defaults
  and manage stream startup as a supervised child.
  """

  alias DuckFeeder.{Config, DesignatedTable, Meta, RuntimeSupport, Service}
  alias DuckFeeder.Runtime.SnapshotSpool
  alias DuckFeeder.CDC.{Bootstrap, Connection, ConnectionOptions, Lsn}

  @default_reconnect_backoff 1_000

  @callback duckfeeder_config() :: map() | keyword()

  defmacro __using__(opts) do
    otp_app = Keyword.fetch!(opts, :otp_app)

    quote do
      @behaviour DuckFeeder.Runtime
      @duckfeeder_otp_app unquote(otp_app)

      def child_spec(start_opts \\ []) do
        %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, [start_opts]}
        }
      end

      def start_link(start_opts \\ []) when is_list(start_opts) do
        DuckFeeder.Runtime.Embedded.start_link(
          module: __MODULE__,
          otp_app: @duckfeeder_otp_app,
          start_opts: start_opts
        )
      end

      @impl true
      def duckfeeder_config do
        Application.get_env(@duckfeeder_otp_app, __MODULE__, [])
      end

      defoverridable child_spec: 1, duckfeeder_config: 0
    end
  end

  @doc """
  Resolves app-facing runtime config into validated DuckFeeder runtime shape.

  Supports two styles:
  - explicit engine config via `config: %{source: ..., duckdb: ..., metadata: ...}`
  - simplified repo/schemas config via `repo`, `schemas`, and `duckdb`
  """
  @spec resolve_app_config(map() | keyword()) :: {:ok, map()} | {:error, term()}
  def resolve_app_config(config) when is_map(config) or is_list(config) do
    with {:ok, cfg} <- normalize_options_map(config, :config) do
      enabled? = truthy?(Map.get(cfg, :enabled, true))

      if enabled? do
        runtime_opts =
          cfg
          |> Map.get(:runtime_opts, [])
          |> List.wrap()
          |> put_default_runtime_opt(:snapshot_before_stream?, true)
          |> put_default_runtime_opt(:resume_incomplete_snapshot?, true)

        source_name = normalize_source_name(Map.get(cfg, :source_name, "default"))

        with {:ok, runtime_config} <- runtime_config(cfg, source_name),
             {:ok, validated} <- Config.validate(runtime_config) do
          designated_tables =
            put_runtime_checkpoint_keys(source_name, validated.source.designated_tables)

          source = build_runtime_source(source_name, validated.source)
          duckdb = Config.duckdb(validated)

          {:ok,
           %{
             enabled?: true,
             source_name: source_name,
             source: source,
             designated_tables: designated_tables,
             validated_config: validated,
             duckdb: duckdb,
             runtime_opts: runtime_opts
           }}
        end
      else
        {:ok, %{enabled?: false}}
      end
    end
  end

  @doc false
  @spec build_runtime_source(String.t(), map()) :: map()
  def build_runtime_source(source_name, source) when is_binary(source_name) and is_map(source) do
    postgres_url = Map.get(source, :postgres_url)

    source
    |> Map.put(:name, source_name)
    |> Map.put(:postgres_url, postgres_url)
    |> maybe_put_runtime_connection_info(postgres_url)
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
    |> Map.new()
  end

  defp maybe_put_runtime_connection_info(source, postgres_url)
       when is_map(source) and is_binary(postgres_url) and postgres_url != "" do
    Map.put_new(source, :connection_info, %{postgres_url: postgres_url})
  end

  defp maybe_put_runtime_connection_info(source, _postgres_url), do: source

  @doc false
  @spec put_runtime_checkpoint_keys(String.t(), [map()]) :: [map()]
  def put_runtime_checkpoint_keys(source_name, designated_tables)
      when is_binary(source_name) and is_list(designated_tables) do
    DesignatedTable.put_checkpoint_keys(designated_tables, source_name)
  end

  @spec repo_postgres_url(module()) :: {:ok, String.t()} | {:error, term()}
  def repo_postgres_url(repo) when is_atom(repo) do
    if function_exported?(repo, :config, 0) do
      with {:ok, repo_config} <- normalize_options_map(apply(repo, :config, []), :repo_config) do
        repo_config_to_url(repo_config)
      end
    else
      {:error, {:invalid_repo, repo}}
    end
  end

  defp runtime_config(cfg, source_name) do
    case Map.get(cfg, :config) do
      nil ->
        simplified_runtime_config(cfg, source_name)

      explicit ->
        normalize_options_map(explicit, :config)
    end
  end

  defp simplified_runtime_config(cfg, source_name) do
    with {:ok, repo} <- fetch_repo(cfg),
         {:ok, schemas} <- fetch_schemas(cfg),
         {:ok, duckdb} <- fetch_duckdb(cfg),
         {:ok, ingest} <- normalize_optional_options_map(Map.get(cfg, :ingest), :ingest),
         {:ok, source_postgres_url} <-
           fetch_or_repo_url(cfg, :source_postgres_url, repo),
         {:ok, metadata_postgres_url} <-
           fetch_or_repo_url(cfg, :metadata_postgres_url, Map.get(cfg, :metadata_repo, repo)),
         {:ok, designated_tables} <- infer_designated_tables(schemas, cfg) do
      slot_name = Map.get(cfg, :slot_name, "duck_feeder_#{source_name}_slot")
      publication_name = Map.get(cfg, :publication_name, "duck_feeder_#{source_name}_pub")

      {:ok,
       %{
         source: %{
           postgres_url: source_postgres_url,
           slot_name: slot_name,
           publication_name: publication_name,
           designated_tables: designated_tables
         },
         duckdb: duckdb,
         metadata: %{postgres_url: metadata_postgres_url},
         ingest: ingest
       }}
    end
  end

  defp fetch_repo(cfg) do
    case Map.get(cfg, :repo) do
      repo when is_atom(repo) -> {:ok, repo}
      other -> {:error, {:missing_or_invalid_option, :repo, other}}
    end
  end

  defp fetch_schemas(cfg) do
    case Map.get(cfg, :schemas) do
      schemas when is_list(schemas) and schemas != [] -> {:ok, schemas}
      other -> {:error, {:missing_or_invalid_option, :schemas, other}}
    end
  end

  defp fetch_duckdb(cfg) do
    case Map.fetch(cfg, :duckdb) do
      {:ok, duckdb} -> normalize_options_map(duckdb, :duckdb)
      :error -> {:error, {:missing_or_invalid_option, :duckdb, nil}}
    end
  end

  defp fetch_or_repo_url(cfg, key, repo) do
    case Map.get(cfg, key) do
      value when is_binary(value) and value != "" -> {:ok, normalize_repo_url(value)}
      nil -> repo_postgres_url(repo)
      other -> {:error, {:invalid_option, key, other}}
    end
  end

  defp infer_designated_tables(schema_entries, cfg) when is_list(schema_entries) do
    default_target_schema = Map.get(cfg, :target_schema, "raw")

    schema_entries
    |> Enum.reduce_while({:ok, []}, fn entry, {:ok, acc} ->
      case normalize_schema_entry(entry, default_target_schema) do
        {:ok, nil} -> {:cont, {:ok, acc}}
        {:ok, designated_table} -> {:cont, {:ok, [designated_table | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, tables} -> {:ok, Enum.reverse(tables)}
      {:error, _reason} = error -> error
    end
  end

  defp normalize_schema_entry({schema_module, opts}, default_target_schema)
       when is_atom(schema_module) and (is_map(opts) or is_list(opts)) do
    with {:ok, overrides} <- normalize_options_map(opts, :schema_options) do
      if truthy?(Map.get(overrides, :enabled?, true)) do
        build_designated_table(schema_module, overrides, default_target_schema)
      else
        {:ok, nil}
      end
    end
  end

  defp normalize_schema_entry(schema_module, default_target_schema) when is_atom(schema_module) do
    build_designated_table(schema_module, %{}, default_target_schema)
  end

  defp normalize_schema_entry(other, _default_target_schema),
    do: {:error, {:invalid_schema_entry, other}}

  defp build_designated_table(schema_module, overrides, default_target_schema)
       when is_atom(schema_module) and is_map(overrides) do
    if function_exported?(schema_module, :__schema__, 1) do
      source_table =
        Map.get(overrides, :source_table, schema_module.__schema__(:source) |> to_string())

      if is_binary(source_table) and source_table != "" do
        source_schema =
          Map.get(overrides, :source_schema, schema_module.__schema__(:prefix) || "public")

        target_schema = Map.get(overrides, :target_schema, default_target_schema)
        target_table = Map.get(overrides, :target_table, source_table)
        mode = Map.get(overrides, :mode, "cdc_changelog") |> to_string()

        primary_keys =
          Map.get(
            overrides,
            :primary_keys,
            schema_module.__schema__(:primary_key) |> List.wrap() |> Enum.map(&to_string/1)
          )
          |> List.wrap()
          |> Enum.map(&to_string/1)

        {:ok,
         %{
           source_schema: to_string(source_schema),
           source_table: to_string(source_table),
           target_schema: to_string(target_schema),
           target_table: to_string(target_table),
           mode: mode,
           primary_keys: primary_keys
         }}
      else
        {:error, {:invalid_schema_source, schema_module, source_table}}
      end
    else
      {:error, {:invalid_schema_module, schema_module}}
    end
  end

  defp repo_config_to_url(repo_cfg) when is_map(repo_cfg) do
    case Map.get(repo_cfg, :url) do
      url when is_binary(url) and url != "" ->
        {:ok, normalize_repo_url(url)}

      _ ->
        build_repo_url(repo_cfg)
    end
  end

  defp build_repo_url(repo_cfg) do
    database = Map.get(repo_cfg, :database)

    if is_binary(database) and database != "" do
      host = Map.get(repo_cfg, :hostname, Map.get(repo_cfg, :host, "localhost"))
      port = Map.get(repo_cfg, :port, 5432)
      username = Map.get(repo_cfg, :username, Map.get(repo_cfg, :user))
      password = Map.get(repo_cfg, :password)
      ssl? = truthy?(Map.get(repo_cfg, :ssl, false))

      query = if ssl?, do: URI.encode_query(%{"sslmode" => "require"}), else: nil

      userinfo =
        case {username, password} do
          {user, pass} when is_binary(user) and user != "" and is_binary(pass) and pass != "" ->
            URI.encode_www_form(user) <> ":" <> URI.encode_www_form(pass)

          {user, _pass} when is_binary(user) and user != "" ->
            URI.encode_www_form(user)

          _ ->
            nil
        end

      {:ok,
       URI.to_string(%URI{
         scheme: "postgres",
         userinfo: userinfo,
         host: to_string(host),
         port: port,
         path: "/" <> database,
         query: query
       })}
    else
      {:error, {:invalid_repo_config, :missing_database}}
    end
  end

  defp normalize_repo_url(url) when is_binary(url) do
    if String.starts_with?(url, "ecto://") do
      "postgres://" <> String.trim_leading(url, "ecto://")
    else
      url
    end
  end

  defp normalize_source_name(name) when is_binary(name) and name != "", do: name
  defp normalize_source_name(_name), do: "default"

  defp normalize_options_map(value, _key) when is_map(value), do: {:ok, value}

  defp normalize_options_map(value, key) when is_list(value) do
    if Keyword.keyword?(value) do
      {:ok, Map.new(value)}
    else
      {:error, {:invalid_option, key, value}}
    end
  end

  defp normalize_options_map(value, key), do: {:error, {:invalid_option, key, value}}

  defp normalize_optional_options_map(nil, _key), do: {:ok, %{}}
  defp normalize_optional_options_map(value, key), do: normalize_options_map(value, key)

  defp truthy?(value), do: value in [true, 1, "1", "true"]

  defp put_default_runtime_opt(opts, key, value) when is_list(opts) and is_atom(key) do
    if Keyword.keyword?(opts) and not Keyword.has_key?(opts, key) do
      Keyword.put(opts, key, value)
    else
      opts
    end
  end

  defp resolve_runtime_source(source_name, opts)
       when is_binary(source_name) and is_list(opts) do
    case Keyword.get(opts, :source) do
      nil ->
        {:error, {:missing_runtime_source, source_name}}

      source ->
        with {:ok, source_map} <- normalize_options_map(source, :source) do
          {:ok, normalize_runtime_source(source_name, source_map)}
        end
    end
  end

  defp resolve_runtime_designated_tables(source_name, opts)
       when is_binary(source_name) and is_list(opts) do
    case Keyword.get(opts, :designated_tables) do
      designated_tables when is_list(designated_tables) ->
        {:ok, put_runtime_checkpoint_keys(source_name, designated_tables)}

      nil ->
        {:error, {:missing_designated_tables, source_name}}

      other ->
        {:error, {:invalid_option, :designated_tables, other}}
    end
  end

  defp fetch_runtime_start_lsn(meta_module, meta_conn, designated_tables, opts)
       when is_atom(meta_module) and is_list(designated_tables) and is_list(opts) do
    default_start_lsn = Keyword.get(opts, :default_start_lsn, "0/0")
    checkpoint_keys = DesignatedTable.checkpoint_keys(designated_tables)

    if function_exported?(meta_module, :fetch_start_lsn, 3) do
      meta_module.fetch_start_lsn(meta_conn, checkpoint_keys, default_start_lsn)
    else
      {:ok, default_start_lsn}
    end
  end

  defp normalize_runtime_source(source_name, source)
       when is_binary(source_name) and is_map(source) do
    source
    |> Map.put_new(:name, source_name)
    |> maybe_put_connection_info()
  end

  defp maybe_put_connection_info(source) when is_map(source) do
    case Map.get(source, :connection_info) do
      value when is_map(value) ->
        source

      _ ->
        case Map.get(source, :postgres_url) do
          postgres_url when is_binary(postgres_url) and postgres_url != "" ->
            Map.put(source, :connection_info, %{postgres_url: postgres_url})

          _ ->
            source
        end
    end
  end

  @spec service_options(pid(), String.t(), map() | nil, keyword()) ::
          {:ok, keyword()} | {:error, term()}
  def service_options(meta_conn, source_name, duckdb, opts \\ [])
      when is_binary(source_name) do
    meta_module = Keyword.get(opts, :meta_module, Meta)

    with {:ok, duckdb} <- RuntimeSupport.normalize_optional_duckdb(duckdb),
         {:ok, _source} <- resolve_runtime_source(source_name, opts),
         {:ok, designated_tables} <- resolve_runtime_designated_tables(source_name, opts) do
      {:ok,
       [
         name: Keyword.get(opts, :name),
         designated_tables: designated_tables,
         meta_conn: meta_conn,
         duckdb: duckdb,
         pipeline_opts: Keyword.get(opts, :pipeline_opts, %{}),
         max_tx_changes: Keyword.get(opts, :max_tx_changes),
         observer_pid: Keyword.get(opts, :observer_pid),
         meta_module: meta_module
       ]
       |> Enum.reject(fn {_key, value} -> is_nil(value) end)}
    end
  end

  @spec start_service(pid(), String.t(), map() | nil, keyword()) ::
          GenServer.on_start() | {:error, term()}
  def start_service(meta_conn, source_name, duckdb, opts \\ []) do
    with {:ok, service_opts} <- service_options(meta_conn, source_name, duckdb, opts) do
      Service.start_link(service_opts)
    end
  end

  @spec start_stream(pid(), String.t(), map() | nil, keyword()) ::
          {:ok, %{service_pid: pid(), cdc_pid: pid(), start_lsn: String.t(), source: map()}}
          | {:error, term()}
  def start_stream(meta_conn, source_name, duckdb, opts \\ []) do
    with {:ok, phase} <- resolve_start_stream_phase(meta_conn, source_name, duckdb, opts),
         {:ok, started_phase} <- start_stream_service_phase(phase) do
      finish_start_stream_phase(started_phase)
    end
  end

  defp resolve_start_stream_phase(meta_conn, source_name, duckdb, opts)
       when is_binary(source_name) and is_list(opts) do
    meta_module = Keyword.get(opts, :meta_module, Meta)
    service_module = Keyword.get(opts, :service_module, Service)
    cdc_module = Keyword.get(opts, :cdc_module, Connection)
    connection_options_module = Keyword.get(opts, :connection_options_module, ConnectionOptions)

    with {:ok, duckdb} <- RuntimeSupport.normalize_optional_duckdb(duckdb),
         {:ok, source} <- resolve_runtime_source(source_name, opts),
         {:ok, designated_tables} <- resolve_runtime_designated_tables(source_name, opts),
         {:ok, slot_name} <- require_source_field(source, :slot_name),
         {:ok, publication_name} <- require_source_field(source, :publication_name),
         {:ok, meta_start_lsn} <-
           fetch_runtime_start_lsn(meta_module, meta_conn, designated_tables, opts),
         {:ok, snapshot_handoff} <- fetch_snapshot_handoff(meta_module, meta_conn, source_name),
         {:ok, snapshot_plan} <- snapshot_plan(meta_start_lsn, snapshot_handoff, opts),
         {:ok, connection_opts} <- connection_options_module.resolve(source, opts),
         {:ok, bootstrap_start_lsn} <-
           maybe_bootstrap_start_lsn(
             connection_opts,
             slot_name,
             publication_name,
             designated_tables,
             opts
           ),
         {:ok, snapshot_result} <-
           maybe_snapshot_boundary_lsn(
             connection_opts,
             designated_tables,
             meta_start_lsn,
             snapshot_plan,
             opts
           ),
         {:ok, start_lsn} <-
           resolve_start_lsn(meta_start_lsn, [bootstrap_start_lsn, snapshot_result.boundary_lsn]),
         {:ok, snapshot_replay_plan} <- snapshot_replay_plan(meta_start_lsn, snapshot_result) do
      {:ok,
       %{
         meta_conn: meta_conn,
         source_name: source_name,
         duckdb: duckdb,
         opts: opts,
         meta_module: meta_module,
         service_module: service_module,
         cdc_module: cdc_module,
         source: source,
         designated_tables: designated_tables,
         slot_name: slot_name,
         publication_name: publication_name,
         connection_opts: connection_opts,
         snapshot_plan: snapshot_plan,
         snapshot_result: snapshot_result,
         snapshot_replay_plan: snapshot_replay_plan,
         start_lsn: start_lsn
       }}
    end
  end

  defp start_stream_service_phase(phase) when is_map(phase) do
    service_opts =
      build_service_opts(
        phase.meta_conn,
        phase.designated_tables,
        phase.duckdb,
        phase.meta_module,
        with_snapshot_lsn_start(phase.opts, phase.snapshot_replay_plan.snapshot_lsn_start)
      )

    case phase.service_module.start_link(service_opts) do
      {:ok, service_pid} -> {:ok, Map.put(phase, :service_pid, service_pid)}
      {:error, _reason} = error -> error
      other -> other
    end
  end

  defp finish_start_stream_phase(phase) when is_map(phase) do
    with :ok <- mark_snapshot_handoff_pending_for_stream(phase),
         :ok <- replay_snapshot_rows_for_stream(phase) do
      start_stream_cdc_phase(phase)
    else
      {:error, reason} ->
        :ok = stop_service(phase.service_pid)
        {:error, reason}
    end
  end

  defp mark_snapshot_handoff_pending_for_stream(phase) when is_map(phase) do
    maybe_mark_snapshot_handoff_pending(
      phase.meta_module,
      phase.meta_conn,
      phase.source_name,
      phase.snapshot_plan,
      phase.snapshot_result,
      phase.opts
    )
  end

  defp replay_snapshot_rows_for_stream(phase) when is_map(phase) do
    maybe_replay_snapshot_rows(
      phase.service_module,
      phase.service_pid,
      phase.snapshot_replay_plan.rows
    )
  end

  defp start_stream_cdc_phase(phase) when is_map(phase) do
    cdc_opts =
      build_cdc_opts(
        phase.connection_opts,
        phase.slot_name,
        phase.publication_name,
        phase.start_lsn,
        phase.service_pid,
        phase.opts
      )

    case phase.cdc_module.start_link(cdc_opts) do
      {:ok, cdc_pid} ->
        complete_start_stream_phase(Map.put(phase, :cdc_pid, cdc_pid))

      {:error, reason} ->
        :ok = stop_service(phase.service_pid)
        {:error, reason}
    end
  end

  defp complete_start_stream_phase(phase) when is_map(phase) do
    with :ok <- attach_cdc_to_service(phase.service_module, phase.service_pid, phase.cdc_pid),
         :ok <-
           maybe_mark_snapshot_handoff_complete(
             phase.meta_module,
             phase.meta_conn,
             phase.source_name,
             phase.snapshot_result,
             phase.opts
           ) do
      {:ok,
       %{
         service_pid: phase.service_pid,
         cdc_pid: phase.cdc_pid,
         start_lsn: phase.start_lsn,
         source: phase.source
       }}
    else
      {:error, {:service_attach_cdc_failed, _} = reason} ->
        true = Process.exit(phase.cdc_pid, :shutdown)
        :ok = stop_service(phase.service_pid)
        {:error, reason}

      {:error, reason} ->
        true = Process.exit(phase.cdc_pid, :shutdown)
        :ok = stop_service(phase.service_pid)
        {:error, {:snapshot_handoff_mark_complete_failed, reason}}
    end
  end

  defp build_service_opts(
         meta_conn,
         designated_tables,
         duckdb,
         meta_module,
         opts
       ) do
    [
      name: Keyword.get(opts, :service_name),
      designated_tables: designated_tables,
      meta_conn: meta_conn,
      duckdb: duckdb,
      pipeline_opts: Keyword.get(opts, :pipeline_opts, %{}),
      max_tx_changes: Keyword.get(opts, :max_tx_changes),
      observer_pid: Keyword.get(opts, :observer_pid),
      snapshot_lsn_start: Keyword.get(opts, :snapshot_lsn_start),
      max_inflight_batches: Keyword.get(opts, :max_inflight_batches),
      max_pending_batches: Keyword.get(opts, :max_pending_batches),
      meta_module: meta_module
    ]
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
  end

  defp build_cdc_opts(
         connection_opts,
         slot_name,
         publication_name,
         start_lsn,
         service_pid,
         opts
       ) do
    reconnect_backoff =
      case Keyword.fetch(opts, :reconnect_backoff) do
        {:ok, value} -> value
        :error -> @default_reconnect_backoff
      end

    reconnect_backoff =
      normalize_reconnect_backoff(
        reconnect_backoff,
        Keyword.get(opts, :reconnect_backoff_min_ms),
        Keyword.get(opts, :reconnect_backoff_max_ms),
        Keyword.get(opts, :reconnect_backoff_jitter_ms, 0),
        Keyword.get(opts, :reconnect_backoff_jitter_fun)
      )

    [
      name: Keyword.get(opts, :cdc_name),
      connection_opts: connection_opts,
      slot_name: slot_name,
      publication_name: publication_name,
      start_lsn: start_lsn,
      status_interval_ms: Keyword.get(opts, :status_interval_ms, 10_000),
      max_lag_bytes: Keyword.get(opts, :max_lag_bytes),
      backpressure_lag_bytes: Keyword.get(opts, :backpressure_lag_bytes),
      decoder_module: Keyword.get(opts, :decoder_module),
      converter_module: Keyword.get(opts, :converter_module),
      event_sink: service_pid,
      auto_reconnect: Keyword.get(opts, :auto_reconnect, true),
      reconnect_backoff: reconnect_backoff,
      sync_connect: Keyword.get(opts, :sync_connect, true)
    ]
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
  end

  defp maybe_bootstrap_start_lsn(
         connection_opts,
         slot_name,
         publication_name,
         designated_tables,
         opts
       ) do
    if Keyword.get(opts, :bootstrap_replication?, true) do
      bootstrap_module = Keyword.get(opts, :bootstrap_module, Bootstrap)
      query_connect_fun = Keyword.get(opts, :query_connect_fun, &Postgrex.start_link/1)
      query_disconnect_fun = Keyword.get(opts, :query_disconnect_fun, &GenServer.stop/1)

      case query_connect_fun.(connection_opts) do
        {:ok, query_conn} ->
          result =
            safe_bootstrap(
              bootstrap_module,
              query_conn,
              publication_name,
              slot_name,
              designated_tables
            )

          :ok = query_disconnect_fun.(query_conn)

          case result do
            {:ok, %{slot: {:created, _slot}, start_lsn: start_lsn}} when is_binary(start_lsn) ->
              {:ok, start_lsn}

            {:ok, %{slot: :exists}} ->
              {:ok, nil}

            {:ok, %{start_lsn: start_lsn}} when is_binary(start_lsn) ->
              {:ok, start_lsn}

            {:ok, other} ->
              {:error, {:invalid_bootstrap_result, other}}

            {:error, _reason} = error ->
              error
          end

        {:error, reason} ->
          {:error, {:query_connection_failed, reason}}
      end
    else
      {:ok, nil}
    end
  end

  defp maybe_snapshot_boundary_lsn(
         _connection_opts,
         _designated_tables,
         _meta_start_lsn,
         %{run_snapshot?: false, handoff_boundary_lsn: boundary_lsn},
         _opts
       ) do
    {:ok, %{boundary_lsn: boundary_lsn, rows: []}}
  end

  defp maybe_snapshot_boundary_lsn(
         connection_opts,
         designated_tables,
         _meta_start_lsn,
         %{run_snapshot?: true},
         opts
       ) do
    snapshot_runner_module =
      Keyword.get(opts, :snapshot_runner_module, DuckFeeder.CDC.InitialSnapshot.Runner)

    query_connect_fun = Keyword.get(opts, :query_connect_fun, &Postgrex.start_link/1)
    query_disconnect_fun = Keyword.get(opts, :query_disconnect_fun, &GenServer.stop/1)
    snapshot_runner_opts = Keyword.get(opts, :snapshot_runner_opts, [])

    case snapshot_row_handler_with_collector(opts) do
      {:ok, row_handler, collect_rows} ->
        case query_connect_fun.(connection_opts) do
          {:ok, query_conn} ->
            result =
              safe_snapshot_run(
                snapshot_runner_module,
                query_conn,
                designated_tables,
                Keyword.merge(snapshot_runner_opts, row_handler: row_handler)
              )

            rows_source = collect_rows.()
            :ok = query_disconnect_fun.(query_conn)

            case result do
              {:ok, %{boundary_lsn: boundary_lsn}} ->
                {:ok, %{boundary_lsn: boundary_lsn, rows: rows_source}}

              {:ok, other} ->
                :ok = cleanup_snapshot_rows_source(rows_source)
                {:error, {:initial_snapshot_failed, {:invalid_snapshot_result, other}}}

              {:error, reason} ->
                :ok = cleanup_snapshot_rows_source(rows_source)
                {:error, {:initial_snapshot_failed, reason}}
            end

          {:error, reason} ->
            rows_source = collect_rows.()
            :ok = cleanup_snapshot_rows_source(rows_source)
            {:error, {:query_connection_failed, reason}}
        end

      {:error, :missing_snapshot_row_handler} = error ->
        error
    end
  end

  defp snapshot_plan(_meta_start_lsn, _snapshot_handoff, opts)
       when not is_list(opts),
       do: {:ok, %{run_snapshot?: false, mark_pending?: false, handoff_boundary_lsn: nil}}

  defp snapshot_plan(meta_start_lsn, snapshot_handoff, opts) when is_binary(meta_start_lsn) do
    snapshot_before_stream? = Keyword.get(opts, :snapshot_before_stream?, false)

    cond do
      match?(%{state: :pending}, snapshot_handoff) ->
        snapshot_plan_from_pending_handoff(
          meta_start_lsn,
          snapshot_handoff,
          snapshot_before_stream?,
          opts
        )

      match?(%{state: :complete}, snapshot_handoff) ->
        if snapshot_before_stream? and Keyword.get(opts, :snapshot_on_restart?, false) do
          {:ok, %{run_snapshot?: true, mark_pending?: true, handoff_boundary_lsn: nil}}
        else
          {:ok, %{run_snapshot?: false, mark_pending?: false, handoff_boundary_lsn: nil}}
        end

      snapshot_before_stream? ->
        if should_run_snapshot_before_stream?(meta_start_lsn, opts) do
          {:ok, %{run_snapshot?: true, mark_pending?: true, handoff_boundary_lsn: nil}}
        else
          {:ok, %{run_snapshot?: false, mark_pending?: false, handoff_boundary_lsn: nil}}
        end

      true ->
        {:ok, %{run_snapshot?: false, mark_pending?: false, handoff_boundary_lsn: nil}}
    end
  end

  defp snapshot_plan_from_pending_handoff(
         meta_start_lsn,
         %{boundary_lsn: boundary_lsn} = snapshot_handoff,
         snapshot_before_stream?,
         opts
       ) do
    if Keyword.get(opts, :resume_incomplete_snapshot?, false) do
      with {:ok, meta_at_or_past_boundary?} <- lsn_at_or_past?(meta_start_lsn, boundary_lsn) do
        cond do
          meta_at_or_past_boundary? ->
            {:ok,
             %{run_snapshot?: false, mark_pending?: false, handoff_boundary_lsn: boundary_lsn}}

          snapshot_before_stream? ->
            {:ok, %{run_snapshot?: true, mark_pending?: true, handoff_boundary_lsn: boundary_lsn}}

          true ->
            {:error, {:snapshot_resume_requires_snapshot_before_stream, snapshot_handoff}}
        end
      end
    else
      {:error, {:snapshot_handoff_incomplete, snapshot_handoff}}
    end
  end

  defp snapshot_plan_from_pending_handoff(
         _meta_start_lsn,
         snapshot_handoff,
         _snapshot_before_stream?,
         _opts
       ) do
    {:error, {:snapshot_handoff_incomplete, snapshot_handoff}}
  end

  defp lsn_at_or_past?(_meta_start_lsn, nil), do: {:ok, false}

  defp lsn_at_or_past?(meta_start_lsn, boundary_lsn)
       when is_binary(meta_start_lsn) and is_binary(boundary_lsn) do
    case Lsn.compare(meta_start_lsn, boundary_lsn) do
      :eq -> {:ok, true}
      :gt -> {:ok, true}
      :lt -> {:ok, false}
      {:error, reason} -> {:error, {:invalid_snapshot_handoff_lsn, reason}}
    end
  end

  defp should_run_snapshot_before_stream?(meta_start_lsn, opts) when is_binary(meta_start_lsn) do
    if Keyword.get(opts, :snapshot_on_restart?, false) do
      true
    else
      default_start_lsn = Keyword.get(opts, :default_start_lsn, "0/0")

      case Lsn.compare(meta_start_lsn, default_start_lsn) do
        :eq -> true
        _ -> false
      end
    end
  end

  defp fetch_snapshot_handoff(meta_module, meta_conn, source_name)
       when is_atom(meta_module) and is_binary(source_name) do
    if function_exported?(meta_module, :fetch_snapshot_handoff, 2) do
      meta_module.fetch_snapshot_handoff(meta_conn, source_name)
    else
      {:ok, nil}
    end
  end

  defp maybe_mark_snapshot_handoff_pending(
         _meta_module,
         _meta_conn,
         _source_name,
         %{mark_pending?: false},
         _snapshot_result,
         _opts
       ),
       do: :ok

  defp maybe_mark_snapshot_handoff_pending(
         meta_module,
         meta_conn,
         source_name,
         %{mark_pending?: true},
         %{boundary_lsn: boundary_lsn},
         opts
       )
       when is_atom(meta_module) and is_binary(source_name) do
    cond do
      not is_binary(boundary_lsn) ->
        :ok

      function_exported?(meta_module, :mark_snapshot_handoff_pending, 3) ->
        retry_mark_snapshot_handoff(opts, fn ->
          mark_snapshot_handoff_pending(meta_module, meta_conn, source_name, boundary_lsn)
        end)

      true ->
        :ok
    end
  end

  defp maybe_mark_snapshot_handoff_complete(
         meta_module,
         meta_conn,
         source_name,
         %{boundary_lsn: boundary_lsn},
         opts
       )
       when is_atom(meta_module) and is_binary(source_name) do
    cond do
      not is_binary(boundary_lsn) ->
        :ok

      function_exported?(meta_module, :mark_snapshot_handoff_complete, 3) ->
        retry_mark_snapshot_handoff(opts, fn ->
          mark_snapshot_handoff_complete(
            meta_module,
            meta_conn,
            source_name,
            boundary_lsn
          )
        end)

      true ->
        :ok
    end
  end

  defp mark_snapshot_handoff_pending(meta_module, meta_conn, source_name, boundary_lsn)
       when is_atom(meta_module) and is_binary(source_name) and is_binary(boundary_lsn) do
    case meta_module.mark_snapshot_handoff_pending(meta_conn, source_name, boundary_lsn) do
      {:ok, _} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp mark_snapshot_handoff_complete(meta_module, meta_conn, source_name, boundary_lsn)
       when is_atom(meta_module) and is_binary(source_name) and is_binary(boundary_lsn) do
    case meta_module.mark_snapshot_handoff_complete(meta_conn, source_name, boundary_lsn) do
      {:ok, _} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp retry_mark_snapshot_handoff(opts, mark_fun)
       when is_list(opts) and is_function(mark_fun, 0) do
    retries =
      normalize_snapshot_handoff_mark_retries(
        Keyword.get(opts, :snapshot_handoff_mark_retries, 2)
      )

    delay_ms =
      normalize_snapshot_handoff_mark_retry_delay_ms(
        Keyword.get(opts, :snapshot_handoff_mark_retry_delay_ms, 0)
      )

    do_retry_mark_snapshot_handoff(mark_fun, retries, delay_ms)
  end

  defp do_retry_mark_snapshot_handoff(mark_fun, retries_left, delay_ms)
       when is_function(mark_fun, 0) and is_integer(retries_left) and retries_left >= 0 do
    case mark_fun.() do
      :ok ->
        :ok

      {:error, _reason} = error ->
        if retries_left > 0 do
          if delay_ms > 0, do: Process.sleep(delay_ms)
          do_retry_mark_snapshot_handoff(mark_fun, retries_left - 1, delay_ms)
        else
          error
        end

      other ->
        {:error, {:invalid_snapshot_handoff_mark_result, other}}
    end
  end

  defp normalize_snapshot_handoff_mark_retries(value)
       when is_integer(value) and value >= 0,
       do: value

  defp normalize_snapshot_handoff_mark_retries(_value), do: 2

  defp normalize_snapshot_handoff_mark_retry_delay_ms(value)
       when is_integer(value) and value >= 0,
       do: value

  defp normalize_snapshot_handoff_mark_retry_delay_ms(_value), do: 0

  defp snapshot_row_handler_with_collector(opts) do
    case Keyword.get(opts, :snapshot_row_handler) do
      handler when is_function(handler, 2) ->
        {:ok, handler, fn -> [] end}

      _ ->
        if Keyword.get(opts, :snapshot_ingest?, true) do
          SnapshotSpool.collector()
        else
          {:error, :missing_snapshot_row_handler}
        end
    end
  end

  defp snapshot_replay_plan(meta_start_lsn, %{boundary_lsn: boundary_lsn, rows: rows_source})
       when is_binary(meta_start_lsn) do
    SnapshotSpool.replay_plan(meta_start_lsn, boundary_lsn, rows_source)
  end

  defp cleanup_snapshot_rows_source(rows_source),
    do: SnapshotSpool.cleanup_rows_source(rows_source)

  defp with_snapshot_lsn_start(opts, nil) when is_list(opts), do: opts

  defp with_snapshot_lsn_start(opts, snapshot_lsn_start) when is_list(opts) do
    Keyword.put(opts, :snapshot_lsn_start, snapshot_lsn_start)
  end

  defp maybe_replay_snapshot_rows(_service_module, _service_pid, []), do: :ok

  defp maybe_replay_snapshot_rows(service_module, service_pid, rows) do
    rows
    |> SnapshotSpool.replay_rows(fn designated_table, row ->
      safe_snapshot_ingest(service_module, service_pid, designated_table, row)
    end)
    |> case do
      :ok ->
        :ok

      {:error, _reason} = error ->
        :ok = stop_service(service_pid)
        error
    end
  end

  defp stop_service(service_pid) when is_pid(service_pid) do
    GenServer.stop(service_pid)
  catch
    :exit, {:noproc, _details} -> :ok
  end

  defp wrap_errors(label, fun) when is_atom(label) and is_function(fun, 0) do
    fun.()
  rescue
    exception ->
      {:error, {:"#{label}_exception", exception}}
  catch
    :exit, reason ->
      {:error, {:"#{label}_exit", reason}}

    kind, reason ->
      {:error, {:"#{label}_throw", kind, reason}}
  end

  defp safe_bootstrap(
         bootstrap_module,
         query_conn,
         publication_name,
         slot_name,
         designated_tables
       ) do
    wrap_errors(:bootstrap, fn ->
      bootstrap_module.bootstrap(query_conn, %{
        publication_name: publication_name,
        slot_name: slot_name,
        designated_tables: designated_tables
      })
    end)
  end

  defp safe_snapshot_run(
         snapshot_runner_module,
         query_conn,
         designated_tables,
         snapshot_runner_opts
       ) do
    wrap_errors(:snapshot_runner, fn ->
      snapshot_runner_module.run(query_conn, designated_tables, snapshot_runner_opts)
    end)
  end

  defp safe_snapshot_ingest(service_module, service_pid, designated_table, row) do
    wrap_errors(:snapshot_ingest, fn ->
      case service_module.ingest_snapshot_row(service_pid, designated_table, row) do
        :ok -> :ok
        {:error, reason} -> {:error, reason}
        other -> {:error, {:invalid_snapshot_ingest_result, other}}
      end
    end)
  end

  defp resolve_start_lsn(meta_start_lsn, candidates) when is_list(candidates) do
    candidates
    |> Enum.reject(&is_nil/1)
    |> Enum.reduce_while({:ok, meta_start_lsn}, fn candidate, {:ok, current} ->
      case Lsn.max(current, candidate) do
        {:error, _reason} = error -> {:halt, error}
        max_lsn -> {:cont, {:ok, max_lsn}}
      end
    end)
  end

  defp attach_cdc_to_service(service_module, service_pid, cdc_pid)
       when is_atom(service_module) and is_pid(service_pid) and is_pid(cdc_pid) do
    if function_exported?(service_module, :attach_cdc, 2) do
      case service_module.attach_cdc(service_pid, cdc_pid) do
        :ok -> :ok
        {:error, reason} -> {:error, {:service_attach_cdc_failed, reason}}
        other -> {:error, {:service_attach_cdc_failed, {:invalid_attach_result, other}}}
      end
    else
      :ok
    end
  rescue
    exception ->
      {:error, {:service_attach_cdc_failed, {:exception, exception}}}
  catch
    kind, reason ->
      {:error, {:service_attach_cdc_failed, {kind, reason}}}
  end

  defp normalize_reconnect_backoff(base_backoff, min_ms, max_ms, jitter_ms, jitter_fun)
       when is_integer(base_backoff) do
    {min_ms, max_ms} = normalize_reconnect_backoff_bounds(min_ms, max_ms)
    jitter_ms = normalize_non_neg_integer(jitter_ms, 0)

    bounded = clamp_reconnect_backoff(base_backoff, min_ms, max_ms)

    jitter =
      case jitter_fun do
        fun when is_function(fun, 2) ->
          fun.(bounded, jitter_ms)
          |> normalize_reconnect_jitter(jitter_ms)

        _ ->
          if jitter_ms > 0, do: :rand.uniform(jitter_ms * 2 + 1) - (jitter_ms + 1), else: 0
      end

    bounded
    |> Kernel.+(jitter)
    |> clamp_reconnect_backoff(min_ms, max_ms)
  end

  defp normalize_reconnect_backoff(_base_backoff, _min_ms, _max_ms, _jitter_ms, _jitter_fun),
    do: @default_reconnect_backoff

  defp normalize_reconnect_backoff_bounds(min_ms, max_ms) do
    min_ms = normalize_non_neg_integer(min_ms, 0)

    max_ms =
      case max_ms do
        value when is_integer(value) and value >= min_ms -> value
        _ -> nil
      end

    {min_ms, max_ms}
  end

  defp clamp_reconnect_backoff(value, min_ms, nil) when is_integer(value), do: max(value, min_ms)

  defp clamp_reconnect_backoff(value, min_ms, max_ms)
       when is_integer(value) and is_integer(max_ms),
       do: value |> max(min_ms) |> min(max_ms)

  defp normalize_reconnect_jitter(value, jitter_ms)
       when is_integer(value) and is_integer(jitter_ms) and jitter_ms >= 0 do
    cond do
      value < -jitter_ms -> -jitter_ms
      value > jitter_ms -> jitter_ms
      true -> value
    end
  end

  defp normalize_reconnect_jitter(_value, _jitter_ms), do: 0

  defp normalize_non_neg_integer(value, _default)
       when is_integer(value) and value >= 0,
       do: value

  defp normalize_non_neg_integer(_value, default), do: default

  defp require_source_field(source, key) do
    case Map.get(source, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, {:missing_source_field, key}}
    end
  end
end
