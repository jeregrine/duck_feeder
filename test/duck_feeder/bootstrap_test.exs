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

  setup do
    Process.put(:test_pid, self())
    :ok
  end

  test "seeds source and designated tables from runtime config" do
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
               connection_info: %{dsn: "postgres://dsn"}
             )

    assert_received :meta_bootstrap

    assert_received {:meta_register_source, source_attrs}
    assert source_attrs.name == "source-a"
    assert source_attrs.slot_name == "duck_slot"
    assert source_attrs.connection_info.postgres_url == "postgres://source"
    assert source_attrs.connection_info.dsn == "postgres://dsn"

    assert_received {:meta_register_designated_table, table_attrs}
    assert table_attrs.source_id == 10
    assert table_attrs.source_table == "users"
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
