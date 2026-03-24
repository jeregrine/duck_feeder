defmodule DuckFeeder.BootstrapTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.Bootstrap

  defmodule FakeMeta do
    def bootstrap(_conn) do
      if pid = Process.get(:test_pid), do: send(pid, :meta_bootstrap)
      :ok
    end
  end

  defmodule FakeMetaError do
    def bootstrap(_conn), do: {:error, :bootstrap_failed}
  end

  defmodule FakeRuntime do
    def start_stream(_meta_conn, source_name, duckdb, start_opts) do
      if pid = Process.get(:test_pid),
        do: send(pid, {:runtime_start_stream, source_name, duckdb, start_opts})

      {:ok,
       %{service_pid: self(), cdc_pid: self(), start_lsn: "0/0", source: %{name: source_name}}}
    end
  end

  setup do
    Process.put(:test_pid, self())
    :ok
  end

  test "bootstraps metadata and resolves runtime source/tables from config" do
    config = %{
      source: %{
        postgres_url:
          "postgres://source_user:source_password@db.example:5432/source_db?sslmode=require&password=query_secret",
        slot_name: "duck_slot",
        publication_name: "duck_pub",
        designated_tables: [
          %{
            source_schema: "public",
            source_table: "users",
            target_schema: "raw",
            target_table: "users",
            mode: "cdc_changelog",
            primary_keys: ["id"]
          }
        ]
      },
      duckdb: %{
        path: "/tmp/source-a.duckdb"
      },
      metadata: %{postgres_url: "postgres://meta"}
    }

    assert {:ok, %{source_name: "source-a", source: source, designated_tables: [table]}} =
             Bootstrap.seed_meta(:meta_conn, config,
               meta_module: FakeMeta,
               source_name: "source-a"
             )

    assert_received :meta_bootstrap

    assert source.name == "source-a"
    assert source.slot_name == "duck_slot"
    assert source.publication_name == "duck_pub"

    assert source.connection_info.postgres_url =~
             "postgres://source_user:source_password@db.example"

    assert table.source_table == "users"
    assert table.target_table == "users"
    assert table.checkpoint_key == "source-a:raw.users"
  end

  test "can seed metadata and start runtime stream from config" do
    config = %{
      source: %{
        postgres_url: "postgres://stream_user:stream_password@localhost:5432/source_db",
        slot_name: "duck_slot",
        publication_name: "duck_pub",
        designated_tables: [
          %{
            source_schema: "public",
            source_table: "users",
            target_schema: "raw",
            target_table: "users",
            mode: "cdc_changelog",
            primary_keys: ["id"]
          }
        ]
      },
      duckdb: %{
        path: "/tmp/source-a.duckdb",
        catalog: "lake"
      },
      metadata: %{postgres_url: "postgres://meta"}
    }

    assert {:ok, result} =
             Bootstrap.seed_and_start_stream(:meta_conn, config,
               runtime_module: FakeRuntime,
               seed_opts: [meta_module: FakeMeta, source_name: "source-a"],
               start_opts: [bootstrap_replication?: false]
             )

    assert result.source_name == "source-a"
    assert result.runtime.start_lsn == "0/0"

    assert_received {:runtime_start_stream, "source-a", duckdb, start_opts}
    assert duckdb.path == "/tmp/source-a.duckdb"
    assert duckdb.catalog == "lake"
    assert start_opts[:meta_module] == FakeMeta
    assert start_opts[:source].name == "source-a"
    assert start_opts[:designated_tables] == result.designated_tables
    assert start_opts[:connection_opts][:hostname] == "localhost"
    assert start_opts[:connection_opts][:database] == "source_db"
    assert start_opts[:connection_opts][:username] == "stream_user"
    assert start_opts[:connection_opts][:password] == "stream_password"
  end

  test "supports explicit table selection and target remapping from Elixir opts" do
    config = %{
      source: %{
        postgres_url: "postgres://source",
        slot_name: "duck_slot",
        publication_name: "duck_pub",
        designated_tables: [
          %{
            source_schema: "public",
            source_table: "users",
            target_schema: "raw",
            target_table: "users",
            mode: "cdc_changelog",
            primary_keys: ["id"]
          },
          %{
            source_schema: "public",
            source_table: "orders",
            target_schema: "raw",
            target_table: "orders",
            mode: "cdc_changelog",
            primary_keys: ["id"]
          }
        ]
      },
      duckdb: %{
        path: "/tmp/source-a.duckdb"
      },
      metadata: %{postgres_url: "postgres://meta"}
    }

    assert {:ok, %{source_name: "source-a", designated_tables: [users, orders]}} =
             Bootstrap.seed_meta(:meta_conn, config,
               meta_module: FakeMeta,
               source_name: "source-a",
               tables: [
                 "users",
                 {"orders_iceberg", "orders"}
               ]
             )

    assert users.source_table == "users"
    assert users.target_table == "users"
    assert users.checkpoint_key == "source-a:raw.users"

    assert orders.source_table == "orders"
    assert orders.target_table == "orders_iceberg"
    assert orders.checkpoint_key == "source-a:raw.orders_iceberg"
  end

  test "returns error for invalid table selection opts" do
    config = %{
      source: %{
        postgres_url: "postgres://source",
        slot_name: "duck_slot",
        publication_name: "duck_pub",
        designated_tables: []
      },
      duckdb: %{
        path: "/tmp/source-a.duckdb"
      },
      metadata: %{postgres_url: "postgres://meta"}
    }

    assert {:error, {:invalid_table_selection, 123}} =
             Bootstrap.seed_meta(:meta_conn, config,
               meta_module: FakeMeta,
               tables: [123]
             )
  end

  test "returns errors when bootstrap fails" do
    config = %{
      source: %{
        postgres_url: "postgres://source",
        slot_name: "duck_slot",
        publication_name: "duck_pub",
        designated_tables: [
          %{
            source_schema: "public",
            source_table: "users",
            target_schema: "raw",
            target_table: "users"
          }
        ]
      },
      duckdb: %{
        path: "/tmp/source-a.duckdb"
      },
      metadata: %{postgres_url: "postgres://meta"}
    }

    assert {:error, :bootstrap_failed} =
             Bootstrap.seed_meta(:meta_conn, config,
               meta_module: FakeMetaError,
               bootstrap_schema?: true
             )
  end
end
