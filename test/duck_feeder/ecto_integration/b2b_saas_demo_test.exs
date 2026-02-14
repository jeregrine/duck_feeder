defmodule DuckFeeder.EctoIntegration.B2BSaaSDemoTest do
  use ExUnit.Case, async: false

  alias DuckFeeder.{Meta, Runtime}
  alias DuckFeeder.CDC.{ConnectionOptions, Setup}

  @moduletag :ecto_integration

  @org_table "ecto_demo_organizations"
  @user_table "ecto_demo_users"
  @subscription_table "ecto_demo_subscriptions"
  @invoice_table "ecto_demo_invoices"

  @source_tables [@org_table, @user_table, @subscription_table, @invoice_table]

  defmodule SourceRepo do
    use Ecto.Repo,
      otp_app: :duck_feeder,
      adapter: Ecto.Adapters.Postgres
  end

  defmodule Organization do
    use Ecto.Schema
    import Ecto.Changeset

    schema "ecto_demo_organizations" do
      field(:name, :string)
      field(:plan_tier, :string)
      timestamps(type: :utc_datetime_usec)
    end

    def changeset(struct, attrs) do
      struct
      |> cast(attrs, [:name, :plan_tier])
      |> validate_required([:name, :plan_tier])
    end
  end

  defmodule User do
    use Ecto.Schema
    import Ecto.Changeset

    schema "ecto_demo_users" do
      field(:email, :string)
      field(:name, :string)
      belongs_to(:organization, Organization)
      timestamps(type: :utc_datetime_usec)
    end

    def changeset(struct, attrs) do
      struct
      |> cast(attrs, [:email, :name, :organization_id])
      |> validate_required([:email, :name, :organization_id])
    end
  end

  defmodule Subscription do
    use Ecto.Schema
    import Ecto.Changeset

    schema "ecto_demo_subscriptions" do
      field(:status, :string)
      field(:seat_count, :integer)
      belongs_to(:organization, Organization)
      timestamps(type: :utc_datetime_usec)
    end

    def changeset(struct, attrs) do
      struct
      |> cast(attrs, [:status, :seat_count, :organization_id])
      |> validate_required([:status, :seat_count, :organization_id])
    end
  end

  defmodule Invoice do
    use Ecto.Schema
    import Ecto.Changeset

    schema "ecto_demo_invoices" do
      field(:status, :string)
      field(:amount_cents, :integer)
      belongs_to(:subscription, Subscription)
      timestamps(type: :utc_datetime_usec)
    end

    def changeset(struct, attrs) do
      struct
      |> cast(attrs, [:status, :amount_cents, :subscription_id])
      |> validate_required([:status, :amount_cents, :subscription_id])
    end
  end

  defmodule LocalFilesystemStorage do
    @behaviour DuckFeeder.Storage.Adapter

    @impl true
    def put_file(config, local_path, %{key: key}, _opts) do
      root_dir = Map.fetch!(config, :root_dir)
      destination = Path.join(root_dir, key)

      File.mkdir_p!(Path.dirname(destination))
      File.cp!(local_path, destination)

      {:ok, %{etag: "itest-local-etag", version_id: nil, size: File.stat!(destination).size}}
    end

    @impl true
    def head_object(config, %{key: key}) do
      root_dir = Map.fetch!(config, :root_dir)
      path = Path.join(root_dir, key)

      case File.stat(path) do
        {:ok, stat} -> {:ok, %{size: stat.size}}
        {:error, reason} -> {:error, reason}
      end
    end

    @impl true
    def delete_object(config, %{key: key}) do
      root_dir = Map.fetch!(config, :root_dir)
      path = Path.join(root_dir, key)

      case File.rm(path) do
        :ok -> :ok
        {:error, :enoent} -> :ok
        {:error, reason} -> {:error, reason}
      end
    end
  end

  setup_all do
    integration_config = Application.get_env(:duck_feeder, :integration, [])
    meta_url = Keyword.get(integration_config, :meta_database_url)
    source_url = Keyword.get(integration_config, :source_database_url)

    assert is_binary(meta_url) and meta_url != "" and is_binary(source_url) and source_url != "",
           "set :duck_feeder, :integration, meta_database_url/source_database_url in config/test.exs"

    {:ok, meta_conn_opts} = ConnectionOptions.parse_url(meta_url)
    {:ok, source_conn_opts} = ConnectionOptions.parse_url(source_url)

    {:ok, meta_conn} = Postgrex.start_link(meta_conn_opts ++ [types: DuckFeeder.Postgrex.Types])

    {:ok, source_conn} =
      Postgrex.start_link(source_conn_opts ++ [types: DuckFeeder.Postgrex.Types])

    assert {:ok, _} =
             Postgrex.query(meta_conn, "DROP SCHEMA IF EXISTS ducklake_metadata CASCADE", [])

    assert {:ok, _} =
             Postgrex.query(meta_conn, "DROP SCHEMA IF EXISTS duckfeeder_meta CASCADE", [])

    assert :ok = Meta.bootstrap(meta_conn)

    :ok = ensure_duckdb_adbc_driver!()

    {:ok, _repo} =
      SourceRepo.start_link(
        source_conn_opts ++
          [
            name: SourceRepo,
            pool_size: 4,
            stacktrace: true,
            show_sensitive_data_on_connection_error: true,
            log: false
          ]
      )

    on_exit(fn ->
      case Process.whereis(SourceRepo) do
        pid when is_pid(pid) -> safe_stop(pid)
        _ -> :ok
      end

      safe_stop(source_conn)
      safe_stop(meta_conn)
    end)

    {:ok, meta_conn: meta_conn, source_conn: source_conn, source_url: source_url}
  end

  setup %{meta_conn: meta_conn, source_conn: source_conn, source_url: source_url} do
    unique =
      "#{System.system_time(:microsecond)}_#{System.unique_integer([:positive, :monotonic])}"

    source_name = "ecto_demo_source_#{unique}"
    slot_name = "ecto_demo_slot_#{System.unique_integer([:positive, :monotonic])}"
    publication_name = "ecto_demo_pub_#{System.unique_integer([:positive, :monotonic])}"

    recreate_source_schema!(source_conn)

    assert {:ok, source_id} =
             Meta.register_source(meta_conn, %{
               name: source_name,
               connection_info: %{"dsn" => source_url},
               slot_name: slot_name,
               publication_name: publication_name,
               status: "active"
             })

    designated_by_source_table =
      for source_table <- @source_tables, into: %{} do
        target_table = source_table

        assert {:ok, designated_table_id} =
                 Meta.register_designated_table(meta_conn, %{
                   source_id: source_id,
                   source_schema: "public",
                   source_table: source_table,
                   target_schema: "raw",
                   target_table: target_table,
                   mode: "cdc_changelog"
                 })

        {source_table, designated_table_id}
      end

    on_exit(fn ->
      if Process.alive?(source_conn) do
        _ = Setup.drop_slot(source_conn, slot_name)
        _ = Postgrex.query(source_conn, "DROP PUBLICATION IF EXISTS \"#{publication_name}\"", [])
        drop_source_schema!(source_conn)
      end
    end)

    {:ok, source_name: source_name, designated_by_source_table: designated_by_source_table}
  end

  test "ecto b2b saas writes stream to parquet and are queryable via adbc duckdb", %{
    meta_conn: meta_conn,
    source_name: source_name,
    designated_by_source_table: designated_by_source_table
  } do
    local_data_root =
      Path.join(
        System.tmp_dir!(),
        "duck_feeder_ecto_demo_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(local_data_root)

    storage = %{
      provider: :s3,
      bucket: "bucket",
      adapter: LocalFilesystemStorage,
      root_dir: local_data_root
    }

    assert {:ok, %{service_pid: service_pid, cdc_pid: cdc_pid}} =
             Runtime.start_stream(meta_conn, source_name, storage,
               observer_pid: self(),
               writer: %{format: :parquet, datetime_encoding: :unix_microseconds},
               committer_module: DuckFeeder.DuckLake.Committer.Postgres,
               pipeline_opts: %{max_rows: 1, max_bytes: 10_000, flush_interval_ms: 60_000},
               bootstrap_replication?: true,
               auto_reconnect: false,
               sync_connect: true
             )

    on_exit(fn ->
      safe_stop(cdc_pid)
      safe_stop(service_pid)
    end)

    Process.sleep(200)

    {:ok, org} =
      %Organization{}
      |> Organization.changeset(%{name: "Acme Inc", plan_tier: "pro"})
      |> SourceRepo.insert()

    {:ok, user} =
      %User{}
      |> User.changeset(%{email: "ops@acme.example", name: "Alice", organization_id: org.id})
      |> SourceRepo.insert()

    {:ok, subscription} =
      %Subscription{}
      |> Subscription.changeset(%{organization_id: org.id, status: "active", seat_count: 15})
      |> SourceRepo.insert()

    {:ok, invoice} =
      %Invoice{}
      |> Invoice.changeset(%{
        subscription_id: subscription.id,
        status: "open",
        amount_cents: 12_500
      })
      |> SourceRepo.insert()

    {:ok, _user} =
      user
      |> User.changeset(%{name: "Alice Admin"})
      |> SourceRepo.update()

    {:ok, _subscription} =
      subscription
      |> Subscription.changeset(%{status: "past_due"})
      |> SourceRepo.update()

    {:ok, _invoice} = SourceRepo.delete(invoice)

    expected_ops_by_table = %{
      @org_table => MapSet.new(["I"]),
      @user_table => MapSet.new(["I", "U"]),
      @subscription_table => MapSet.new(["I", "U"]),
      @invoice_table => MapSet.new(["I", "D"])
    }

    assert {:ok, seen_ops_by_table, observed_rows} =
             await_expected_ops(expected_ops_by_table, %{}, [], 20_000)

    Enum.each(expected_ops_by_table, fn {table, expected_ops} ->
      assert MapSet.subset?(expected_ops, Map.get(seen_ops_by_table, table, MapSet.new()))
    end)

    assert Enum.any?(observed_rows, fn {table, row} ->
             table == @user_table and row[:_op] == "U" and
               get_in(row, [:_record, "name"]) == "Alice Admin" and
               get_in(row, [:_old_record, "name"]) == "Alice"
           end)

    assert Enum.any?(observed_rows, fn {table, row} ->
             table == @invoice_table and row[:_op] == "D" and
               get_in(row, [:_old_record, "status"]) == "open"
           end)

    designated_ids = Map.values(designated_by_source_table)

    Enum.each(designated_ids, fn table_id ->
      assert {:ok, %{rows: [[count]]}} =
               Postgrex.query(
                 meta_conn,
                 "SELECT count(*) FROM ducklake_metadata.ducklake_data_file WHERE table_id = $1",
                 [table_id]
               )

      assert count > 0
    end)

    assert {:ok, %{rows: file_rows}} =
             Postgrex.query(
               meta_conn,
               """
               SELECT b.designated_table_id, f.object_key
               FROM duckfeeder_meta.batch_files f
               JOIN duckfeeder_meta.batches b ON b.batch_id = f.batch_id
               WHERE b.designated_table_id = ANY($1::int[])
               ORDER BY f.inserted_at ASC
               """,
               [designated_ids]
             )

    assert length(file_rows) >= 7

    table_by_designated_id =
      Map.new(designated_by_source_table, fn {table, id} -> {id, table} end)

    {:ok, db} = Adbc.Database.start_link(driver: :duckdb)
    {:ok, conn} = Adbc.Connection.start_link(database: db)

    on_exit(fn ->
      safe_stop(conn)
      safe_stop(db)
    end)

    parquet_ops_by_table =
      Enum.reduce(file_rows, %{}, fn [designated_table_id, object_key], acc ->
        source_table = Map.fetch!(table_by_designated_id, designated_table_id)
        full_path = Path.join(local_data_root, object_key)

        assert File.exists?(full_path)

        sql = "SELECT _op FROM read_parquet('#{escape_sql_literal(full_path)}')"

        assert {:ok, result} = Adbc.Connection.query(conn, sql)
        ops = result |> Adbc.Result.to_map() |> Map.get("_op", []) |> Enum.map(&to_string/1)

        merged =
          ops
          |> MapSet.new()
          |> MapSet.union(Map.get(acc, source_table, MapSet.new()))

        Map.put(acc, source_table, merged)
      end)

    Enum.each(expected_ops_by_table, fn {table, expected_ops} ->
      assert MapSet.subset?(expected_ops, Map.get(parquet_ops_by_table, table, MapSet.new()))
    end)
  end

  defp recreate_source_schema!(source_conn) do
    drop_source_schema!(source_conn)

    assert {:ok, _} =
             Postgrex.query(
               source_conn,
               """
               CREATE TABLE public.\"#{@org_table}\" (
                 id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                 name text NOT NULL,
                 plan_tier text NOT NULL,
                 inserted_at timestamptz NOT NULL DEFAULT now(),
                 updated_at timestamptz NOT NULL DEFAULT now()
               )
               """,
               []
             )

    assert {:ok, _} =
             Postgrex.query(
               source_conn,
               """
               CREATE TABLE public.\"#{@user_table}\" (
                 id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                 organization_id BIGINT NOT NULL REFERENCES public.\"#{@org_table}\" (id),
                 email text NOT NULL,
                 name text NOT NULL,
                 inserted_at timestamptz NOT NULL DEFAULT now(),
                 updated_at timestamptz NOT NULL DEFAULT now()
               )
               """,
               []
             )

    assert {:ok, _} =
             Postgrex.query(
               source_conn,
               """
               CREATE TABLE public.\"#{@subscription_table}\" (
                 id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                 organization_id BIGINT NOT NULL REFERENCES public.\"#{@org_table}\" (id),
                 status text NOT NULL,
                 seat_count integer NOT NULL,
                 inserted_at timestamptz NOT NULL DEFAULT now(),
                 updated_at timestamptz NOT NULL DEFAULT now()
               )
               """,
               []
             )

    assert {:ok, _} =
             Postgrex.query(
               source_conn,
               """
               CREATE TABLE public.\"#{@invoice_table}\" (
                 id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                 subscription_id BIGINT NOT NULL REFERENCES public.\"#{@subscription_table}\" (id),
                 status text NOT NULL,
                 amount_cents BIGINT NOT NULL,
                 inserted_at timestamptz NOT NULL DEFAULT now(),
                 updated_at timestamptz NOT NULL DEFAULT now()
               )
               """,
               []
             )

    Enum.each(@source_tables, fn table ->
      assert {:ok, _} =
               Postgrex.query(
                 source_conn,
                 "ALTER TABLE public.\"#{table}\" REPLICA IDENTITY FULL",
                 []
               )
    end)
  end

  defp drop_source_schema!(source_conn) do
    Enum.each(Enum.reverse(@source_tables), fn table ->
      _ = Postgrex.query(source_conn, "DROP TABLE IF EXISTS public.\"#{table}\" CASCADE", [])
    end)
  end

  defp await_expected_ops(expected_ops_by_table, seen_ops_by_table, observed_rows, timeout_ms)
       when is_integer(timeout_ms) and timeout_ms > 0 do
    if expected_ops_seen?(expected_ops_by_table, seen_ops_by_table) do
      {:ok, seen_ops_by_table, observed_rows}
    else
      receive do
        {:duck_feeder_batch_processed, {"raw", target_table}, {:ok, _result}, batch} ->
          table_rows = Enum.map(batch.rows, &{target_table, &1})

          next_seen =
            Enum.reduce(batch.rows, seen_ops_by_table, fn row, acc ->
              op = Map.get(row, :_op)

              Map.update(acc, target_table, MapSet.new([op]), fn existing ->
                MapSet.put(existing, op)
              end)
            end)

          await_expected_ops(
            expected_ops_by_table,
            next_seen,
            observed_rows ++ table_rows,
            timeout_ms
          )
      after
        timeout_ms ->
          {:error, {:await_timeout, seen_ops_by_table}}
      end
    end
  end

  defp expected_ops_seen?(expected_ops_by_table, seen_ops_by_table) do
    Enum.all?(expected_ops_by_table, fn {table, expected_ops} ->
      MapSet.subset?(expected_ops, Map.get(seen_ops_by_table, table, MapSet.new()))
    end)
  end

  defp safe_stop(pid) when is_pid(pid) do
    if Process.alive?(pid) do
      GenServer.stop(pid)
    else
      :ok
    end
  rescue
    _ -> :ok
  catch
    _, _ -> :ok
  end

  defp safe_stop(_), do: :ok

  defp ensure_duckdb_adbc_driver! do
    case Adbc.download_driver(:duckdb) do
      :ok ->
        :ok

      {:error, reason} ->
        raise "failed to download ADBC DuckDB driver: #{reason}"
    end
  end

  defp escape_sql_literal(value) when is_binary(value), do: String.replace(value, "'", "''")
end
