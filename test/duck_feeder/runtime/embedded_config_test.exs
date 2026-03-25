defmodule DuckFeeder.Runtime.EmbeddedConfigTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.Runtime

  defmodule FakeRepo do
    def config do
      [url: "ecto://app_user:app_pass@localhost:5432/app_db"]
    end
  end

  defmodule FakeMetaRepo do
    def config do
      [
        username: "meta_user",
        password: "meta_pass",
        hostname: "meta.local",
        port: 5432,
        database: "meta_db",
        ssl: true
      ]
    end
  end

  defmodule Tenant do
    def __schema__(:source), do: "tenants"
    def __schema__(:prefix), do: "public"
    def __schema__(:primary_key), do: [:id]
    def __schema__(_), do: nil
  end

  defmodule User do
    def __schema__(:source), do: "users"
    def __schema__(:prefix), do: "public"
    def __schema__(:primary_key), do: [:id]
    def __schema__(_), do: nil
  end

  defmodule Invoice do
    def __schema__(:source), do: "invoices"
    def __schema__(:prefix), do: "billing"
    def __schema__(:primary_key), do: [:id]
    def __schema__(_), do: nil
  end

  defmodule DisabledRuntime do
    use DuckFeeder.Runtime, otp_app: :duck_feeder
  end

  test "builds validated runtime config from repo + schemas" do
    config = %{
      enabled: true,
      repo: FakeRepo,
      schemas: [Tenant, User, {Invoice, target_schema: "raw", target_table: "invoice_events"}],
      duckdb: %{
        path: "/tmp/primary.duckdb"
      },
      source_name: "primary"
    }

    assert {:ok, resolved} = Runtime.resolve_app_config(config)

    assert resolved.enabled?
    assert resolved.source_name == "primary"
    assert resolved.validated_config.source.slot_name == "duck_feeder_primary_slot"
    assert resolved.validated_config.source.publication_name == "duck_feeder_primary_pub"

    assert resolved.validated_config.source.postgres_url ==
             "postgres://app_user:app_pass@localhost:5432/app_db"

    assert resolved.validated_config.metadata.postgres_url ==
             "postgres://app_user:app_pass@localhost:5432/app_db"

    assert resolved.duckdb.path == "/tmp/primary.duckdb"

    assert Enum.any?(resolved.validated_config.source.designated_tables, fn table ->
             table.source_table == "tenants" and table.target_schema == "raw"
           end)

    assert Enum.any?(resolved.validated_config.source.designated_tables, fn table ->
             table.source_table == "invoices" and table.target_table == "invoice_events"
           end)
  end

  test "defaults embedded runtime opts to initial snapshot and resume" do
    config = %{
      enabled: true,
      repo: FakeRepo,
      schemas: [Tenant],
      duckdb: %{path: "/tmp/defaults.duckdb"}
    }

    assert {:ok, resolved} = Runtime.resolve_app_config(config)
    assert resolved.runtime_opts[:snapshot_before_stream?] == true
    assert resolved.runtime_opts[:resume_incomplete_snapshot?] == true
  end

  test "preserves explicit embedded runtime opt overrides" do
    config = %{
      enabled: true,
      repo: FakeRepo,
      schemas: [Tenant],
      duckdb: %{path: "/tmp/defaults.duckdb"},
      runtime_opts: [snapshot_before_stream?: false, resume_incomplete_snapshot?: false]
    }

    assert {:ok, resolved} = Runtime.resolve_app_config(config)
    assert resolved.runtime_opts[:snapshot_before_stream?] == false
    assert resolved.runtime_opts[:resume_incomplete_snapshot?] == false
  end

  test "supports metadata_repo override" do
    config = %{
      enabled: true,
      repo: FakeRepo,
      metadata_repo: FakeMetaRepo,
      schemas: [Tenant],
      duckdb: %{
        path: "/tmp/meta.duckdb"
      }
    }

    assert {:ok, resolved} = Runtime.resolve_app_config(config)

    assert resolved.validated_config.metadata.postgres_url ==
             "postgres://meta_user:meta_pass@meta.local:5432/meta_db?sslmode=require"
  end

  test "use DuckFeeder.Runtime with disabled config starts in no-op mode" do
    Application.put_env(:duck_feeder, DisabledRuntime, enabled: false)

    on_exit(fn ->
      Application.delete_env(:duck_feeder, DisabledRuntime)
    end)

    assert {:ok, pid} = DisabledRuntime.start_link()
    assert {:ok, info} = DuckFeeder.Runtime.Embedded.runtime_info(pid)
    assert info.enabled? == false

    GenServer.stop(pid)
  end
end
