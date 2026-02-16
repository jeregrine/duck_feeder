defmodule DuckFeeder.BootstrapTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.Bootstrap

  defmodule FakeMeta do
    def bootstrap(_conn) do
      if pid = Process.get(:test_pid), do: send(pid, :meta_bootstrap)
      :ok
    end

    def register_source(_conn, attrs) do
      if pid = Process.get(:test_pid), do: send(pid, {:meta_register_source, attrs})
      {:ok, 10}
    end

    def register_designated_table(_conn, attrs) do
      if pid = Process.get(:test_pid), do: send(pid, {:meta_register_designated_table, attrs})
      {:ok, attrs.source_id + map_size(attrs)}
    end
  end

  defmodule FakeMetaError do
    def bootstrap(_conn), do: :ok
    def register_source(_conn, _attrs), do: {:ok, 10}
    def register_designated_table(_conn, _attrs), do: {:error, :insert_failed}
  end

  defmodule FakeRuntime do
    def start_stream(_meta_conn, source_name, storage_config, start_opts) do
      if pid = Process.get(:test_pid),
        do: send(pid, {:runtime_start_stream, source_name, storage_config, start_opts})

      {:ok,
       %{service_pid: self(), cdc_pid: self(), start_lsn: "0/0", source: %{name: source_name}}}
    end
  end

  setup do
    Process.put(:test_pid, self())
    :ok
  end

  test "seeds source and designated tables from runtime config" do
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
      storage: %{
        provider: :s3,
        bucket: "bucket",
        access_key_id: "key",
        secret_access_key: "secret"
      },
      metadata: %{postgres_url: "postgres://meta"}
    }

    assert {:ok, %{source_id: 10, designated_table_ids: [17], source_name: "source-a"}} =
             Bootstrap.seed_meta(:meta_conn, config,
               meta_module: FakeMeta,
               source_name: "source-a",
               connection_info: %{
                 dsn: "postgres://dsn_user:dsn_password@dsn.example:5432/dsn_db",
                 password: "should_not_persist"
               }
             )

    assert_received :meta_bootstrap

    assert_received {:meta_register_source, source_attrs}
    assert source_attrs.name == "source-a"
    assert source_attrs.slot_name == "duck_slot"

    assert source_attrs.connection_info.postgres_url ==
             "postgres://source_user@db.example:5432/source_db?sslmode=require"

    assert source_attrs.connection_info.dsn == "postgres://dsn_user@dsn.example:5432/dsn_db"
    refute Map.has_key?(source_attrs.connection_info, :password)

    assert_received {:meta_register_designated_table, table_attrs}
    assert table_attrs.source_id == 10
    assert table_attrs.source_table == "users"
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
      storage: %{
        provider: :s3,
        bucket: "bucket",
        access_key_id: "key",
        secret_access_key: "secret"
      },
      metadata: %{postgres_url: "postgres://meta"}
    }

    assert {:ok, result} =
             Bootstrap.seed_and_start_stream(:meta_conn, config,
               runtime_module: FakeRuntime,
               seed_opts: [meta_module: FakeMeta, source_name: "source-a"],
               start_opts: [bootstrap_replication?: false]
             )

    assert result.source_id == 10
    assert result.source_name == "source-a"
    assert result.runtime.start_lsn == "0/0"

    assert_received {:runtime_start_stream, "source-a", storage_config, start_opts}
    assert storage_config.provider == :s3
    assert storage_config.bucket == "bucket"
    assert start_opts[:meta_module] == FakeMeta
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
      storage: %{
        provider: :s3,
        bucket: "bucket",
        access_key_id: "key",
        secret_access_key: "secret"
      },
      metadata: %{postgres_url: "postgres://meta"}
    }

    assert {:ok, %{source_id: 10, source_name: "source-a"}} =
             Bootstrap.seed_meta(:meta_conn, config,
               meta_module: FakeMeta,
               source_name: "source-a",
               tables: [
                 "users",
                 {"orders_iceberg", "orders"}
               ]
             )

    assert_received {:meta_register_designated_table, users_attrs}
    assert users_attrs.source_table == "users"
    assert users_attrs.target_table == "users"

    assert_received {:meta_register_designated_table, orders_attrs}
    assert orders_attrs.source_table == "orders"
    assert orders_attrs.target_table == "orders_iceberg"
  end

  test "returns error for invalid table selection opts" do
    config = %{
      source: %{
        postgres_url: "postgres://source",
        slot_name: "duck_slot",
        publication_name: "duck_pub",
        designated_tables: []
      },
      storage: %{
        provider: :s3,
        bucket: "bucket",
        access_key_id: "key",
        secret_access_key: "secret"
      },
      metadata: %{postgres_url: "postgres://meta"}
    }

    assert {:error, {:invalid_table_selection, 123}} =
             Bootstrap.seed_meta(:meta_conn, config,
               meta_module: FakeMeta,
               tables: [123]
             )
  end

  test "returns errors when designated table registration fails" do
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
      storage: %{
        provider: :s3,
        bucket: "bucket",
        access_key_id: "key",
        secret_access_key: "secret"
      },
      metadata: %{postgres_url: "postgres://meta"}
    }

    assert {:error, :insert_failed} =
             Bootstrap.seed_meta(:meta_conn, config,
               meta_module: FakeMetaError,
               bootstrap_schema?: false
             )
  end
end
