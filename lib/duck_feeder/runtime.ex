defmodule DuckFeeder.Runtime do
  @moduledoc """
  Runtime wiring helpers to start `DuckFeeder.Service` from metadata tables.

  High-level startup flow:

      Meta.get_source/list_designated_tables
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

  alias DuckFeeder.{Config, Meta, Service}
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
  - explicit engine config via `config: %{source: ..., storage: ..., metadata: ...}`
  - simplified repo/schemas config via `repo`, `schemas`, and `storage`
  """
  @spec resolve_app_config(map() | keyword()) :: {:ok, map()} | {:error, term()}
  def resolve_app_config(config) when is_map(config) or is_list(config) do
    cfg = mapify(config)

    enabled? = truthy?(Map.get(cfg, :enabled, true))

    if enabled? do
      runtime_opts = map_get(cfg, :runtime_opts, []) |> List.wrap()
      source_name = normalize_source_name(map_get(cfg, :source_name, "default"))
      start_reconciler? = truthy?(map_get(cfg, :start_reconciler?, false))
      reconcile_opts = map_get(cfg, :reconcile_opts, []) |> List.wrap()
      reconciler_interval_ms = map_get(cfg, :reconciler_interval_ms)

      with {:ok, runtime_config} <- runtime_config(cfg, source_name),
           {:ok, validated} <- Config.validate(runtime_config) do
        {:ok,
         %{
           enabled?: true,
           source_name: source_name,
           validated_config: validated,
           storage_config: Config.storage_config(validated),
           runtime_opts: runtime_opts,
           start_reconciler?: start_reconciler?,
           reconcile_opts: reconcile_opts,
           reconciler_interval_ms: reconciler_interval_ms
         }}
      end
    else
      {:ok, %{enabled?: false}}
    end
  end

  @spec repo_postgres_url(module()) :: {:ok, String.t()} | {:error, term()}
  def repo_postgres_url(repo) when is_atom(repo) do
    if function_exported?(repo, :config, 0) do
      repo
      |> apply(:config, [])
      |> mapify()
      |> repo_config_to_url()
    else
      {:error, {:invalid_repo, repo}}
    end
  end

  defp runtime_config(cfg, source_name) do
    case map_get(cfg, :config) do
      explicit when is_map(explicit) or is_list(explicit) ->
        {:ok, mapify(explicit)}

      nil ->
        simplified_runtime_config(cfg, source_name)

      other ->
        {:error, {:invalid_option, :config, other}}
    end
  end

  defp simplified_runtime_config(cfg, source_name) do
    with {:ok, repo} <- fetch_repo(cfg),
         {:ok, schemas} <- fetch_schemas(cfg),
         {:ok, storage} <- fetch_storage(cfg),
         {:ok, source_postgres_url} <-
           fetch_or_repo_url(cfg, :source_postgres_url, repo),
         {:ok, metadata_postgres_url} <-
           fetch_or_repo_url(cfg, :metadata_postgres_url, map_get(cfg, :metadata_repo, repo)),
         {:ok, designated_tables} <- infer_designated_tables(schemas, cfg) do
      slot_name = map_get(cfg, :slot_name, "duck_feeder_#{source_name}_slot")
      publication_name = map_get(cfg, :publication_name, "duck_feeder_#{source_name}_pub")

      {:ok,
       %{
         source: %{
           postgres_url: source_postgres_url,
           slot_name: slot_name,
           publication_name: publication_name,
           designated_tables: designated_tables
         },
         storage: mapify(storage),
         metadata: %{postgres_url: metadata_postgres_url},
         ingest: map_get(cfg, :ingest, %{}) |> mapify()
       }}
    end
  end

  defp fetch_repo(cfg) do
    case map_get(cfg, :repo) do
      repo when is_atom(repo) -> {:ok, repo}
      other -> {:error, {:missing_or_invalid_option, :repo, other}}
    end
  end

  defp fetch_schemas(cfg) do
    case map_get(cfg, :schemas) do
      schemas when is_list(schemas) and schemas != [] -> {:ok, schemas}
      other -> {:error, {:missing_or_invalid_option, :schemas, other}}
    end
  end

  defp fetch_storage(cfg) do
    case map_get(cfg, :storage) do
      storage when is_map(storage) or is_list(storage) -> {:ok, storage}
      other -> {:error, {:missing_or_invalid_option, :storage, other}}
    end
  end

  defp fetch_or_repo_url(cfg, key, repo) do
    case map_get(cfg, key) do
      value when is_binary(value) and value != "" -> {:ok, normalize_repo_url(value)}
      nil -> repo_postgres_url(repo)
      other -> {:error, {:invalid_option, key, other}}
    end
  end

  defp infer_designated_tables(schema_entries, cfg) when is_list(schema_entries) do
    default_target_schema = map_get(cfg, :target_schema, "raw")

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
       when is_atom(schema_module) and is_list(opts) do
    if truthy?(Keyword.get(opts, :enabled?, true)) do
      build_designated_table(schema_module, mapify(opts), default_target_schema)
    else
      {:ok, nil}
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
        map_get(overrides, :source_table, schema_module.__schema__(:source) |> to_string())

      if is_binary(source_table) and source_table != "" do
        source_schema =
          map_get(overrides, :source_schema, schema_module.__schema__(:prefix) || "public")

        target_schema = map_get(overrides, :target_schema, default_target_schema)
        target_table = map_get(overrides, :target_table, source_table)
        mode = map_get(overrides, :mode, "cdc_changelog") |> to_string()

        primary_keys =
          map_get(
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
    case map_get(repo_cfg, :url) do
      url when is_binary(url) and url != "" ->
        {:ok, normalize_repo_url(url)}

      _ ->
        build_repo_url(repo_cfg)
    end
  end

  defp build_repo_url(repo_cfg) do
    database = map_get(repo_cfg, :database)

    if is_binary(database) and database != "" do
      host = map_get(repo_cfg, :hostname, map_get(repo_cfg, :host, "localhost"))
      port = map_get(repo_cfg, :port, 5432)
      username = map_get(repo_cfg, :username, map_get(repo_cfg, :user))
      password = map_get(repo_cfg, :password)
      ssl? = truthy?(map_get(repo_cfg, :ssl, false))

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

  defp mapify(value) when is_map(value), do: value

  defp mapify(value) when is_list(value) do
    if Keyword.keyword?(value), do: Map.new(value), else: %{}
  end

  defp mapify(_value), do: %{}

  defp map_get(map, key, default \\ nil) when is_map(map) do
    Map.get(map, key, Map.get(map, Atom.to_string(key), default))
  end

  defp truthy?(value), do: value in [true, 1, "1", "true", true]

  @spec service_options(pid(), String.t(), map(), keyword()) ::
          {:ok, keyword()} | {:error, term()}
  def service_options(meta_conn, source_name, storage_config, opts \\ [])
      when is_binary(source_name) and is_map(storage_config) do
    meta_module = Keyword.get(opts, :meta_module, Meta)

    with {:ok, source} <- meta_module.get_source(meta_conn, source_name),
         {:ok, designated_tables} <-
           meta_module.list_designated_tables(meta_conn, source_id: source.id) do
      {:ok,
       [
         name: Keyword.get(opts, :name),
         designated_tables: designated_tables,
         meta_conn: meta_conn,
         storage: storage_config,
         writer: Keyword.get(opts, :writer, %{}),
         committer_module: Keyword.get(opts, :committer_module),
         committer_opts: Keyword.get(opts, :committer_opts),
         object_prefix: Keyword.get(opts, :object_prefix, source.name),
         pipeline_opts: Keyword.get(opts, :pipeline_opts, %{}),
         max_tx_changes: Keyword.get(opts, :max_tx_changes),
         observer_pid: Keyword.get(opts, :observer_pid),
         poison_row_mode: Keyword.get(opts, :poison_row_mode),
         poison_row_sink: Keyword.get(opts, :poison_row_sink),
         meta_module: meta_module
       ]
       |> Enum.reject(fn {_key, value} -> is_nil(value) end)}
    end
  end

  @spec start_service(pid(), String.t(), map(), keyword()) ::
          GenServer.on_start() | {:error, term()}
  def start_service(meta_conn, source_name, storage_config, opts \\ []) do
    with {:ok, service_opts} <- service_options(meta_conn, source_name, storage_config, opts) do
      Service.start_link(service_opts)
    end
  end

  @spec start_stream(pid(), String.t(), map(), keyword()) ::
          {:ok, %{service_pid: pid(), cdc_pid: pid(), start_lsn: String.t(), source: map()}}
          | {:error, term()}
  def start_stream(meta_conn, source_name, storage_config, opts \\ []) do
    meta_module = Keyword.get(opts, :meta_module, Meta)
    service_module = Keyword.get(opts, :service_module, Service)
    cdc_module = Keyword.get(opts, :cdc_module, Connection)
    connection_options_module = Keyword.get(opts, :connection_options_module, ConnectionOptions)

    with {:ok, source} <- meta_module.get_source(meta_conn, source_name),
         {:ok, designated_tables} <-
           meta_module.list_designated_tables(meta_conn, source_id: source.id),
         {:ok, slot_name} <- require_source_field(source, :slot_name),
         {:ok, publication_name} <- require_source_field(source, :publication_name),
         {:ok, meta_start_lsn} <-
           meta_module.fetch_source_start_lsn(
             meta_conn,
             source.id,
             Keyword.get(opts, :default_start_lsn, "0/0")
           ),
         {:ok, snapshot_handoff} <-
           fetch_snapshot_handoff(meta_module, meta_conn, source.id),
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
         {:ok, snapshot_replay_plan} <-
           snapshot_replay_plan(meta_start_lsn, snapshot_result),
         {:ok, service_pid} <-
           service_module.start_link(
             build_service_opts(
               meta_conn,
               source,
               designated_tables,
               storage_config,
               meta_module,
               with_snapshot_lsn_start(opts, snapshot_replay_plan.snapshot_lsn_start)
             )
           ) do
      with :ok <-
             maybe_mark_snapshot_handoff_pending(
               meta_module,
               meta_conn,
               source.id,
               snapshot_plan,
               snapshot_result,
               opts
             ),
           :ok <-
             maybe_replay_snapshot_rows(service_module, service_pid, snapshot_replay_plan.rows) do
        case cdc_module.start_link(
               build_cdc_opts(
                 connection_opts,
                 slot_name,
                 publication_name,
                 start_lsn,
                 service_module,
                 service_pid,
                 opts
               )
             ) do
          {:ok, cdc_pid} ->
            with :ok <- attach_cdc_to_service(service_module, service_pid, cdc_pid),
                 :ok <-
                   maybe_mark_snapshot_handoff_complete(
                     meta_module,
                     meta_conn,
                     source.id,
                     snapshot_result,
                     opts
                   ) do
              {:ok,
               %{
                 service_pid: service_pid,
                 cdc_pid: cdc_pid,
                 start_lsn: start_lsn,
                 source: source
               }}
            else
              {:error, {:service_attach_cdc_failed, _} = reason} ->
                _ = safe_stop_cdc(cdc_pid)
                _ = safe_stop_service(service_pid)
                {:error, reason}

              {:error, reason} ->
                _ = safe_stop_cdc(cdc_pid)
                _ = safe_stop_service(service_pid)
                {:error, {:snapshot_handoff_mark_complete_failed, reason}}
            end

          {:error, reason} ->
            _ = safe_stop_service(service_pid)
            {:error, reason}
        end
      else
        {:error, reason} ->
          _ = safe_stop_service(service_pid)
          {:error, reason}
      end
    end
  end

  defp build_service_opts(
         meta_conn,
         source,
         designated_tables,
         storage_config,
         meta_module,
         opts
       ) do
    [
      name: Keyword.get(opts, :service_name),
      designated_tables: designated_tables,
      meta_conn: meta_conn,
      storage: storage_config,
      writer: Keyword.get(opts, :writer, %{}),
      committer_module: Keyword.get(opts, :committer_module),
      committer_opts: Keyword.get(opts, :committer_opts),
      object_prefix: Keyword.get(opts, :object_prefix, source.name),
      pipeline_opts: Keyword.get(opts, :pipeline_opts, %{}),
      max_tx_changes: Keyword.get(opts, :max_tx_changes),
      observer_pid: Keyword.get(opts, :observer_pid),
      snapshot_lsn_start: Keyword.get(opts, :snapshot_lsn_start),
      max_inflight_batches: Keyword.get(opts, :max_inflight_batches),
      max_pending_batches: Keyword.get(opts, :max_pending_batches),
      poison_row_mode: Keyword.get(opts, :poison_row_mode),
      poison_row_sink: Keyword.get(opts, :poison_row_sink),
      meta_module: meta_module
    ]
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
  end

  defp build_cdc_opts(
         connection_opts,
         slot_name,
         publication_name,
         start_lsn,
         service_module,
         service_pid,
         opts
       ) do
    event_sink_mode = Keyword.get(opts, :event_sink_mode, :pid)

    event_sink =
      case event_sink_mode do
        :pid ->
          service_pid

        :call ->
          fn event ->
            case service_module.push_event(service_pid, event) do
              {:error, reason} -> {:error, reason}
              _ -> :ok
            end
          end
      end

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
      event_sink: event_sink,
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

          _ = safe_disconnect_query_conn(query_disconnect_fun, query_conn)

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
            _ = safe_disconnect_query_conn(query_disconnect_fun, query_conn)

            case result do
              {:ok, %{boundary_lsn: boundary_lsn}} ->
                {:ok, %{boundary_lsn: boundary_lsn, rows: rows_source}}

              {:ok, other} ->
                _ = cleanup_snapshot_rows_source(rows_source)
                {:error, {:initial_snapshot_failed, {:invalid_snapshot_result, other}}}

              {:error, reason} ->
                _ = cleanup_snapshot_rows_source(rows_source)
                {:error, {:initial_snapshot_failed, reason}}
            end

          {:error, reason} ->
            rows_source = collect_rows.()
            _ = cleanup_snapshot_rows_source(rows_source)
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

  defp fetch_snapshot_handoff(meta_module, meta_conn, source_id)
       when is_atom(meta_module) and is_integer(source_id) and source_id > 0 do
    if function_exported?(meta_module, :fetch_snapshot_handoff, 2) do
      meta_module.fetch_snapshot_handoff(meta_conn, source_id)
    else
      {:ok, nil}
    end
  end

  defp maybe_mark_snapshot_handoff_pending(
         _meta_module,
         _meta_conn,
         source_id,
         %{mark_pending?: false},
         _snapshot_result,
         _opts
       )
       when is_integer(source_id) and source_id > 0,
       do: :ok

  defp maybe_mark_snapshot_handoff_pending(
         meta_module,
         meta_conn,
         source_id,
         %{mark_pending?: true},
         %{boundary_lsn: boundary_lsn},
         opts
       )
       when is_atom(meta_module) and is_integer(source_id) and source_id > 0 do
    if is_binary(boundary_lsn) and
         function_exported?(meta_module, :mark_snapshot_handoff_pending, 3) do
      retry_mark_snapshot_handoff(opts, fn ->
        case meta_module.mark_snapshot_handoff_pending(meta_conn, source_id, boundary_lsn) do
          {:ok, _} -> :ok
          {:error, _reason} = error -> error
        end
      end)
    else
      :ok
    end
  end

  defp maybe_mark_snapshot_handoff_complete(
         meta_module,
         meta_conn,
         source_id,
         %{boundary_lsn: boundary_lsn},
         opts
       )
       when is_atom(meta_module) and is_integer(source_id) and source_id > 0 do
    if is_binary(boundary_lsn) and
         function_exported?(meta_module, :mark_snapshot_handoff_complete, 3) do
      retry_mark_snapshot_handoff(opts, fn ->
        case meta_module.mark_snapshot_handoff_complete(meta_conn, source_id, boundary_lsn) do
          {:ok, _} -> :ok
          {:error, _reason} = error -> error
        end
      end)
    else
      :ok
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
          build_snapshot_spool_collector()
        else
          {:error, :missing_snapshot_row_handler}
        end
    end
  end

  defp build_snapshot_spool_collector do
    path =
      Path.join(
        System.tmp_dir!(),
        "duck_feeder_snapshot_rows_#{System.unique_integer([:positive, :monotonic])}.spool"
      )

    case File.open(path, [:write, :binary]) do
      {:ok, io_device} ->
        counter = :atomics.new(1, [])

        row_handler = fn designated_table, row ->
          snapshot_spool_push(io_device, counter, designated_table, row)
        end

        collect_rows = fn ->
          _ = safe_close_snapshot_spool(io_device)
          {:spooled_snapshot_rows, path, :atomics.get(counter, 1)}
        end

        {:ok, row_handler, collect_rows}

      {:error, reason} ->
        {:error, {:snapshot_collector_start_failed, reason}}
    end
  end

  defp snapshot_spool_push(io_device, counter, designated_table, row)
       when is_pid(io_device) and is_reference(counter) do
    encoded =
      {designated_table, row}
      |> :erlang.term_to_binary()
      |> Base.encode64()

    case IO.binwrite(io_device, encoded <> "\n") do
      :ok ->
        _ = :atomics.add_get(counter, 1, 1)
        :ok

      {:error, reason} ->
        {:error, {:snapshot_collector_push_failed, reason}}
    end
  rescue
    exception ->
      {:error, {:snapshot_collector_push_exception, exception}}
  catch
    :exit, reason ->
      {:error, {:snapshot_collector_push_exit, reason}}

    kind, reason ->
      {:error, {:snapshot_collector_push_throw, kind, reason}}
  end

  defp safe_close_snapshot_spool(io_device) when is_pid(io_device) do
    File.close(io_device)
    :ok
  rescue
    _ -> :ok
  catch
    _, _ -> :ok
  end

  defp cleanup_snapshot_rows_source([]), do: :ok

  defp cleanup_snapshot_rows_source({:spooled_snapshot_rows, path, _row_count})
       when is_binary(path),
       do: safe_delete_snapshot_spool(path)

  defp cleanup_snapshot_rows_source({:spooled_snapshot_rows, path, _skip_count, _row_count})
       when is_binary(path),
       do: safe_delete_snapshot_spool(path)

  defp cleanup_snapshot_rows_source(_other), do: :ok

  defp safe_delete_snapshot_spool(path) when is_binary(path) do
    case File.rm(path) do
      :ok -> :ok
      {:error, :enoent} -> :ok
      {:error, _reason} -> :ok
    end
  end

  defp snapshot_replay_plan(meta_start_lsn, %{boundary_lsn: nil, rows: rows_source})
       when is_binary(meta_start_lsn) do
    _ = cleanup_snapshot_rows_source(rows_source)
    {:ok, %{rows: [], snapshot_lsn_start: nil}}
  end

  defp snapshot_replay_plan(meta_start_lsn, %{boundary_lsn: boundary_lsn, rows: rows_source})
       when is_binary(meta_start_lsn) and is_binary(boundary_lsn) do
    row_count = snapshot_row_source_count(rows_source)

    case Lsn.compare(meta_start_lsn, boundary_lsn) do
      :lt ->
        with {:ok, snapshot_lsn_start_counter} <-
               snapshot_lsn_start_counter(boundary_lsn, row_count),
             {:ok, replayed_count} <-
               replayed_snapshot_row_count(meta_start_lsn, snapshot_lsn_start_counter, row_count),
             {:ok, remaining_rows_source} <-
               snapshot_remaining_rows_source(rows_source, replayed_count) do
          snapshot_lsn_start = Lsn.to_string(snapshot_lsn_start_counter + replayed_count)

          {:ok, %{rows: remaining_rows_source, snapshot_lsn_start: snapshot_lsn_start}}
        end

      :eq ->
        _ = cleanup_snapshot_rows_source(rows_source)
        {:ok, %{rows: [], snapshot_lsn_start: nil}}

      :gt ->
        _ = cleanup_snapshot_rows_source(rows_source)
        {:ok, %{rows: [], snapshot_lsn_start: nil}}

      {:error, reason} ->
        _ = cleanup_snapshot_rows_source(rows_source)
        {:error, {:invalid_snapshot_handoff_lsn, reason}}
    end
  end

  defp snapshot_row_source_count(rows) when is_list(rows), do: length(rows)

  defp snapshot_row_source_count({:spooled_snapshot_rows, _path, row_count})
       when is_integer(row_count) and row_count >= 0,
       do: row_count

  defp snapshot_row_source_count(_rows_source), do: 0

  defp snapshot_remaining_rows_source(rows, replayed_count)
       when is_list(rows) and is_integer(replayed_count) and replayed_count >= 0 do
    {:ok, Enum.drop(rows, replayed_count)}
  end

  defp snapshot_remaining_rows_source(
         {:spooled_snapshot_rows, path, row_count},
         replayed_count
       )
       when is_binary(path) and is_integer(row_count) and row_count >= 0 and
              is_integer(replayed_count) and replayed_count >= 0 do
    if replayed_count >= row_count do
      _ = safe_delete_snapshot_spool(path)
      {:ok, []}
    else
      {:ok, {:spooled_snapshot_rows, path, replayed_count, row_count}}
    end
  end

  defp snapshot_remaining_rows_source(rows_source, _replayed_count),
    do: {:error, {:invalid_snapshot_rows_source, rows_source}}

  defp snapshot_lsn_start_counter(boundary_lsn, row_count)
       when is_binary(boundary_lsn) and is_integer(row_count) and row_count >= 0 do
    with {:ok, boundary} <- Lsn.parse(boundary_lsn) do
      {:ok, max(boundary - row_count, 0)}
    end
  end

  defp replayed_snapshot_row_count(meta_start_lsn, snapshot_lsn_start_counter, row_count)
       when is_binary(meta_start_lsn) and is_integer(snapshot_lsn_start_counter) and
              is_integer(row_count) and row_count >= 0 do
    with {:ok, meta_counter} <- Lsn.parse(meta_start_lsn) do
      replayed = max(meta_counter - snapshot_lsn_start_counter, 0)
      {:ok, min(replayed, row_count)}
    end
  end

  defp with_snapshot_lsn_start(opts, nil) when is_list(opts), do: opts

  defp with_snapshot_lsn_start(opts, snapshot_lsn_start) when is_list(opts) do
    Keyword.put(opts, :snapshot_lsn_start, snapshot_lsn_start)
  end

  defp maybe_replay_snapshot_rows(_service_module, _service_pid, []), do: :ok

  defp maybe_replay_snapshot_rows(service_module, service_pid, rows)
       when is_list(rows) do
    Enum.reduce_while(rows, :ok, fn {designated_table, row}, :ok ->
      case safe_snapshot_ingest(service_module, service_pid, designated_table, row) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {:snapshot_replay_failed, reason}}}
      end
    end)
    |> case do
      :ok ->
        :ok

      {:error, _reason} = error ->
        _ = safe_stop_service(service_pid)
        error
    end
  end

  defp maybe_replay_snapshot_rows(
         service_module,
         service_pid,
         {:spooled_snapshot_rows, path, skip_count, row_count}
       )
       when is_binary(path) and is_integer(skip_count) and skip_count >= 0 and
              is_integer(row_count) and row_count >= 0 do
    replay_result =
      path
      |> File.stream!([], :line)
      |> Stream.drop(skip_count)
      |> Enum.reduce_while(:ok, fn line, :ok ->
        with {:ok, {designated_table, row}} <- decode_snapshot_spooled_row(line),
             :ok <- safe_snapshot_ingest(service_module, service_pid, designated_table, row) do
          {:cont, :ok}
        else
          {:error, reason} -> {:halt, {:error, {:snapshot_replay_failed, reason}}}
        end
      end)

    _ = safe_delete_snapshot_spool(path)

    case replay_result do
      :ok ->
        :ok

      {:error, _reason} = error ->
        _ = safe_stop_service(service_pid)
        error
    end
  rescue
    exception ->
      _ = safe_delete_snapshot_spool(path)
      _ = safe_stop_service(service_pid)
      {:error, {:snapshot_replay_failed, {:snapshot_spool_exception, exception}}}
  catch
    kind, reason ->
      _ = safe_delete_snapshot_spool(path)
      _ = safe_stop_service(service_pid)
      {:error, {:snapshot_replay_failed, {:snapshot_spool_throw, kind, reason}}}
  end

  defp decode_snapshot_spooled_row(line) when is_binary(line) do
    trimmed = String.trim(line)

    with {:ok, binary} <- Base.decode64(trimmed),
         {designated_table, row} <- :erlang.binary_to_term(binary, [:safe]) do
      {:ok, {designated_table, row}}
    else
      :error -> {:error, {:invalid_snapshot_spool_row, trimmed}}
      other -> {:error, {:invalid_snapshot_spool_row, other}}
    end
  rescue
    exception ->
      {:error, {:invalid_snapshot_spool_row, exception}}
  end

  defp safe_bootstrap(
         bootstrap_module,
         query_conn,
         publication_name,
         slot_name,
         designated_tables
       ) do
    bootstrap_module.bootstrap(query_conn, %{
      publication_name: publication_name,
      slot_name: slot_name,
      designated_tables: designated_tables
    })
  rescue
    exception ->
      {:error, {:bootstrap_exception, exception}}
  catch
    kind, reason ->
      {:error, {:bootstrap_throw, kind, reason}}
  end

  defp safe_snapshot_run(
         snapshot_runner_module,
         query_conn,
         designated_tables,
         snapshot_runner_opts
       ) do
    snapshot_runner_module.run(query_conn, designated_tables, snapshot_runner_opts)
  rescue
    exception ->
      {:error, {:snapshot_runner_exception, exception}}
  catch
    kind, reason ->
      {:error, {:snapshot_runner_throw, kind, reason}}
  end

  defp safe_snapshot_ingest(service_module, service_pid, designated_table, row) do
    case service_module.ingest_snapshot_row(service_pid, designated_table, row) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
      other -> {:error, {:invalid_snapshot_ingest_result, other}}
    end
  rescue
    exception ->
      {:error, {:snapshot_ingest_exception, exception}}
  catch
    :exit, reason ->
      {:error, {:snapshot_ingest_exit, reason}}

    kind, reason ->
      {:error, {:snapshot_ingest_throw, kind, reason}}
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

  defp safe_stop_service(service_pid) when is_pid(service_pid) do
    if Process.alive?(service_pid) do
      GenServer.stop(service_pid)
    else
      :ok
    end
  rescue
    _ -> :ok
  catch
    _, _ -> :ok
  end

  defp safe_stop_cdc(cdc_pid) when is_pid(cdc_pid) do
    if Process.alive?(cdc_pid), do: Process.exit(cdc_pid, :shutdown)
    :ok
  rescue
    _ -> :ok
  catch
    _, _ -> :ok
  end

  defp safe_disconnect_query_conn(disconnect_fun, query_conn) do
    disconnect_fun.(query_conn)
  rescue
    _ -> :ok
  catch
    _, _ -> :ok
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
