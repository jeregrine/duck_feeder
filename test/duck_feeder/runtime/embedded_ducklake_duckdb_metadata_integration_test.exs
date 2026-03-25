defmodule DuckFeeder.Runtime.EmbeddedDuckLakeDuckDBMetadataIntegrationTest do
  use ExUnit.Case, async: false

  import DuckFeeder.TestSupport.IntegrationHelpers

  alias DuckFeeder.CDC.Setup
  alias DuckFeeder.Meta

  @moduletag :integration

  @source_table "duck_feeder_runtime_ecto_users"

  defmodule SourceRepo do
    use Ecto.Repo,
      otp_app: :duck_feeder,
      adapter: Ecto.Adapters.Postgres
  end

  defmodule MetaRepo do
    use Ecto.Repo,
      otp_app: :duck_feeder,
      adapter: Ecto.Adapters.Postgres
  end

  defmodule User do
    use Ecto.Schema

    @primary_key {:id, :integer, autogenerate: false}
    schema "duck_feeder_runtime_ecto_users" do
      field(:name, :string)
    end
  end

  defmodule RuntimeModule do
    use DuckFeeder.Runtime, otp_app: :duck_feeder
  end

  setup do
    source_postgres_url = source_database_url!()
    meta_postgres_url = meta_database_url!()

    source_conn = start_postgres_conn!(source_postgres_url)
    meta_conn = start_postgres_conn!(meta_postgres_url)
    Process.unlink(source_conn)
    Process.unlink(meta_conn)
    :ok = Meta.bootstrap(meta_conn)

    cleanup_stale_runtime_replication_artifacts(source_conn, "duck_feeder_embedded_runtime_")

    root = temp_dir!("embedded_ducklake_duckdb_runtime")
    source_name = unique_name("embedded_runtime_duckdb")
    publication_name = "duck_feeder_#{source_name}_pub"
    slot_name = "duck_feeder_#{source_name}_slot"
    checkpoint_key = "#{source_name}:raw.#{@source_table}"
    duckdb = ducklake_duckdb(root)
    runtime_duckdb = Map.take(duckdb, [:path, :catalog, :setup_sql, :setup_fun])

    Application.put_env(:duck_feeder, SourceRepo, url: source_postgres_url)
    Application.put_env(:duck_feeder, MetaRepo, url: meta_postgres_url)

    Application.put_env(:duck_feeder, RuntimeModule,
      enabled: true,
      repo: SourceRepo,
      metadata_repo: MetaRepo,
      source_name: source_name,
      schemas: [User],
      duckdb: runtime_duckdb,
      runtime_opts: [
        observer_pid: self(),
        snapshot_before_stream?: true,
        resume_incomplete_snapshot?: true,
        status_interval_ms: 200,
        reconnect_backoff: 200,
        pipeline_opts: %{max_rows: 100, max_bytes: 100_000, flush_interval_ms: 200}
      ]
    )

    assert {:ok, _} =
             Postgrex.query(source_conn, ~s|DROP TABLE IF EXISTS public."#{@source_table}"|, [])

    assert {:ok, _} =
             Postgrex.query(
               source_conn,
               ~s|CREATE TABLE public."#{@source_table}" (id integer PRIMARY KEY, name text)|,
               []
             )

    assert {:ok, _} =
             Postgrex.query(
               source_conn,
               ~s|ALTER TABLE public."#{@source_table}" REPLICA IDENTITY FULL|,
               []
             )

    on_exit(fn ->
      Application.delete_env(:duck_feeder, RuntimeModule)
      Application.delete_env(:duck_feeder, SourceRepo)
      Application.delete_env(:duck_feeder, MetaRepo)

      _ = Setup.drop_slot(source_conn, slot_name)
      _ = Postgrex.query(source_conn, ~s|DROP PUBLICATION IF EXISTS "#{publication_name}"|, [])
      _ = Postgrex.query(source_conn, ~s|DROP TABLE IF EXISTS public."#{@source_table}"|, [])

      _ = File.rm_rf(root)

      safe_stop(source_conn)
      safe_stop(meta_conn)
    end)

    {:ok,
     source_conn: source_conn,
     meta_conn: meta_conn,
     source_postgres_url: source_postgres_url,
     meta_postgres_url: meta_postgres_url,
     source_name: source_name,
     checkpoint_key: checkpoint_key,
     duckdb: duckdb}
  end

  test "embedded runtime uses repo/schemas config to mirror CDC into DuckDB-backed DuckLake", %{
    source_conn: source_conn,
    meta_conn: meta_conn,
    source_postgres_url: source_postgres_url,
    meta_postgres_url: meta_postgres_url,
    source_name: source_name,
    checkpoint_key: checkpoint_key,
    duckdb: duckdb
  } do
    assert {:ok, _} =
             Postgrex.query(
               source_conn,
               ~s|INSERT INTO public."#{@source_table}" (id, name) VALUES (1, 'alice'), (2, 'bob')|,
               []
             )

    assert {:ok, runtime} = RuntimeModule.start_link()
    Process.unlink(runtime)

    on_exit(fn ->
      safe_stop(runtime)
    end)

    assert {:ok, info} = DuckFeeder.Runtime.Embedded.runtime_info(runtime)
    assert info.enabled?
    assert info.config.source_name == source_name
    assert info.config.validated_config.source.postgres_url == source_postgres_url
    assert info.config.validated_config.metadata.postgres_url == meta_postgres_url

    assert_batch_processed!({"raw", @source_table})

    assert_eventually(fn ->
      query_duckdb!(duckdb, "SELECT id, name FROM lake.raw.#{@source_table} ORDER BY id") ==
        %{"id" => [1, 2], "name" => ["alice", "bob"]}
    end)

    assert_eventually(fn ->
      case Meta.fetch_snapshot_handoff(meta_conn, source_name) do
        {:ok, %{state: :complete}} -> true
        _ -> false
      end
    end)

    initial_checkpoint_lsn =
      assert_eventually_value(fn ->
        case Meta.fetch_checkpoint(meta_conn, checkpoint_key) do
          {:ok, lsn} when is_binary(lsn) and lsn != "0/0" -> {:ok, lsn}
          _ -> :retry
        end
      end)

    drain_observer_messages()

    assert {:ok, _} =
             Postgrex.query(
               source_conn,
               ~s|UPDATE public."#{@source_table}" SET name = 'alice-2' WHERE id = 1|,
               []
             )

    assert {:ok, _} =
             Postgrex.query(
               source_conn,
               ~s|DELETE FROM public."#{@source_table}" WHERE id = 2|,
               []
             )

    assert {:ok, _} =
             Postgrex.query(
               source_conn,
               ~s|INSERT INTO public."#{@source_table}" (id, name) VALUES (3, 'carol')|,
               []
             )

    assert_batch_processed!({"raw", @source_table})

    assert_eventually_value(fn ->
      result = query_duckdb!(duckdb, "SELECT id, name FROM lake.raw.#{@source_table} ORDER BY id")

      if result == %{"id" => [1, 3], "name" => ["alice-2", "carol"]} do
        {:ok, result}
      else
        {:retry, result}
      end
    end)

    mutation_checkpoint_lsn =
      assert_eventually_value(fn ->
        case Meta.fetch_checkpoint(meta_conn, checkpoint_key) do
          {:ok, lsn} when is_binary(lsn) and lsn != initial_checkpoint_lsn -> {:ok, lsn}
          _ -> :retry
        end
      end)

    assert mutation_checkpoint_lsn != initial_checkpoint_lsn

    drain_observer_messages()

    assert {:ok, _} =
             Postgrex.query(
               source_conn,
               ~s|ALTER TABLE public."#{@source_table}" ADD COLUMN email text|,
               []
             )

    assert {:ok, _} =
             Postgrex.query(
               source_conn,
               ~s|UPDATE public."#{@source_table}" SET email = 'alice@example.com' WHERE id = 1|,
               []
             )

    assert {:ok, _} =
             Postgrex.query(
               source_conn,
               ~s|INSERT INTO public."#{@source_table}" (id, name, email) VALUES (6, 'frank', 'frank@example.com')|,
               []
             )

    assert_batch_processed!({"raw", @source_table})

    additive_checkpoint_lsn =
      assert_eventually_value(fn ->
        case Meta.fetch_checkpoint(meta_conn, checkpoint_key) do
          {:ok, lsn} when is_binary(lsn) and lsn != mutation_checkpoint_lsn -> {:ok, lsn}
          _ -> :retry
        end
      end)

    assert additive_checkpoint_lsn != mutation_checkpoint_lsn

    assert_eventually_value(fn ->
      result =
        query_duckdb!(duckdb, "SELECT id, name, email FROM lake.raw.#{@source_table} ORDER BY id")

      if result == %{
           "id" => [1, 3, 6],
           "name" => ["alice-2", "carol", "frank"],
           "email" => ["alice@example.com", nil, "frank@example.com"]
         } do
        {:ok, result}
      else
        {:retry, result}
      end
    end)

    drain_observer_messages()

    assert {:ok, _} =
             Postgrex.query(source_conn, ~s|TRUNCATE TABLE public."#{@source_table}"|, [])

    assert_batch_processed!({"raw", @source_table})

    assert_eventually(fn ->
      query_duckdb!(duckdb, "SELECT count(*) AS n FROM lake.raw.#{@source_table}") == %{
        "n" => [0]
      }
    end)

    drain_observer_messages()

    assert {:ok, _} =
             Postgrex.query(
               source_conn,
               ~s|INSERT INTO public."#{@source_table}" (id, name) VALUES (4, 'dora')|,
               []
             )

    assert_batch_processed!({"raw", @source_table})

    assert_eventually(fn ->
      query_duckdb!(duckdb, "SELECT id, name FROM lake.raw.#{@source_table} ORDER BY id") ==
        %{"id" => [4], "name" => ["dora"]}
    end)

    safe_stop(runtime)

    assert {:ok, _} =
             Postgrex.query(
               source_conn,
               ~s|INSERT INTO public."#{@source_table}" (id, name) VALUES (5, 'erin')|,
               []
             )

    assert {:ok, restarted_runtime} = RuntimeModule.start_link()
    Process.unlink(restarted_runtime)

    on_exit(fn ->
      safe_stop(restarted_runtime)
    end)

    assert_batch_processed!({"raw", @source_table})

    assert_eventually(fn ->
      query_duckdb!(duckdb, "SELECT id, name FROM lake.raw.#{@source_table} ORDER BY id") ==
        %{"id" => [4, 5], "name" => ["dora", "erin"]}
    end)
  end

  defp assert_batch_processed!(table, timeout_ms \\ 15_000) when is_tuple(table) do
    assert_receive {:duck_feeder_batch_processed, ^table, {:ok, _result}, _batch}, timeout_ms
  end

  defp drain_observer_messages do
    receive do
      _message -> drain_observer_messages()
    after
      0 -> :ok
    end
  end

  defp cleanup_stale_runtime_replication_artifacts(source_conn, prefix)
       when is_pid(source_conn) and is_binary(prefix) do
    {:ok, %Postgrex.Result{rows: slot_rows}} =
      Postgrex.query(
        source_conn,
        "SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE $1",
        [prefix <> "%slot"]
      )

    Enum.each(slot_rows, fn [slot_name] ->
      _ = Setup.drop_slot(source_conn, slot_name)
    end)

    {:ok, %Postgrex.Result{rows: publication_rows}} =
      Postgrex.query(
        source_conn,
        "SELECT pubname FROM pg_publication WHERE pubname LIKE $1",
        [prefix <> "%pub"]
      )

    Enum.each(publication_rows, fn [publication_name] ->
      _ = Postgrex.query(source_conn, ~s|DROP PUBLICATION IF EXISTS "#{publication_name}"|, [])
    end)
  end

  defp assert_eventually(fun, opts \\ []) when is_function(fun, 0) do
    timeout_ms = Keyword.get(opts, :timeout_ms, 10_000)
    interval_ms = Keyword.get(opts, :interval_ms, 100)
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_assert_eventually(fun, deadline, interval_ms, nil)
  end

  defp assert_eventually_value(fun, opts \\ []) when is_function(fun, 0) do
    timeout_ms = Keyword.get(opts, :timeout_ms, 10_000)
    interval_ms = Keyword.get(opts, :interval_ms, 100)
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_assert_eventually_value(fun, deadline, interval_ms, nil)
  end

  defp do_assert_eventually(fun, deadline, interval_ms, _last_failure) do
    try do
      if fun.() do
        :ok
      else
        retry_assert_eventually(fun, deadline, interval_ms, :returned_false)
      end
    rescue
      exception in [ExUnit.AssertionError] ->
        reraise exception, __STACKTRACE__

      exception ->
        retry_assert_eventually(fun, deadline, interval_ms, Exception.format(:error, exception))
    catch
      kind, reason ->
        retry_assert_eventually(fun, deadline, interval_ms, {kind, reason})
    end
  end

  defp do_assert_eventually_value(fun, deadline, interval_ms, _last_failure) do
    try do
      case fun.() do
        {:ok, value} -> value
        {:retry, failure} -> retry_assert_eventually_value(fun, deadline, interval_ms, failure)
        :retry -> retry_assert_eventually_value(fun, deadline, interval_ms, :retry)
        other -> retry_assert_eventually_value(fun, deadline, interval_ms, other)
      end
    rescue
      exception in [ExUnit.AssertionError] ->
        reraise exception, __STACKTRACE__

      exception ->
        retry_assert_eventually_value(
          fun,
          deadline,
          interval_ms,
          Exception.format(:error, exception)
        )
    catch
      kind, reason ->
        retry_assert_eventually_value(fun, deadline, interval_ms, {kind, reason})
    end
  end

  defp retry_assert_eventually(fun, deadline, interval_ms, failure) do
    if System.monotonic_time(:millisecond) < deadline do
      Process.sleep(interval_ms)
      do_assert_eventually(fun, deadline, interval_ms, failure)
    else
      flunk("condition not met before timeout: #{inspect(failure)}")
    end
  end

  defp retry_assert_eventually_value(fun, deadline, interval_ms, failure) do
    if System.monotonic_time(:millisecond) < deadline do
      Process.sleep(interval_ms)
      do_assert_eventually_value(fun, deadline, interval_ms, failure)
    else
      flunk("value not observed before timeout: #{inspect(failure)}")
    end
  end
end
