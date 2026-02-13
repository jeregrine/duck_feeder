defmodule DuckFeeder.ConfigTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.Config

  test "validates and normalizes s3 config" do
    config = %{
      source: %{
        postgres_url: "postgres://source",
        slot_name: "duck_feeder_slot",
        publication_name: "duck_feeder_pub",
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
        bucket: "ducklake-data",
        prefix: "prod",
        access_key_id: "key",
        secret_access_key: "secret",
        endpoint: "https://s3.example.test",
        force_path_style: true
      },
      metadata: %{postgres_url: "postgres://meta"},
      ingest: %{max_rows: 5_000}
    }

    assert {:ok, validated} = Config.validate(config)

    assert validated.source.publication_name == "duck_feeder_pub"
    assert validated.storage.provider == :s3
    assert validated.ingest.max_rows == 5_000

    storage_config = Config.storage_config(validated)

    assert storage_config.provider == :s3
    assert storage_config.bucket == "ducklake-data"
    assert storage_config.access_key_id == "key"
    assert storage_config.secret_access_key == "secret"
    assert storage_config.force_path_style == true
  end

  test "accepts gcs token function" do
    token_fun = fn -> "token" end

    config = %{
      source: %{
        postgres_url: "postgres://source",
        slot_name: "slot",
        publication_name: "pub",
        designated_tables: []
      },
      storage: %{
        provider: :gcs,
        bucket: "ducklake-data",
        token_fun: token_fun
      },
      metadata: %{postgres_url: "postgres://meta"}
    }

    assert {:ok, validated} = Config.validate(config)

    storage_config = Config.storage_config(validated)

    assert storage_config.provider == :gcs
    assert storage_config.token_fun == token_fun
    assert storage_config.base_url == "https://storage.googleapis.com"
  end

  test "rejects s3 config without access key id" do
    config = %{
      source: %{
        postgres_url: "postgres://source",
        slot_name: "slot",
        publication_name: "pub",
        designated_tables: []
      },
      storage: %{
        provider: :s3,
        bucket: "ducklake-data",
        secret_access_key: "secret"
      },
      metadata: %{postgres_url: "postgres://meta"}
    }

    assert {:error, %ArgumentError{message: message}} = Config.validate(config)
    assert message =~ "storage.access_key_id"
  end

  test "rejects unsupported designated table mode" do
    config = %{
      source: %{
        postgres_url: "postgres://source",
        slot_name: "slot",
        publication_name: "pub",
        designated_tables: [
          %{
            source_schema: "public",
            source_table: "users",
            target_schema: "raw",
            target_table: "users",
            mode: "full_replace"
          }
        ]
      },
      storage: %{
        provider: :gcs,
        bucket: "ducklake-data",
        token: "token"
      },
      metadata: %{postgres_url: "postgres://meta"}
    }

    assert {:error, %ArgumentError{message: message}} = Config.validate(config)
    assert message =~ "mode"
    assert message =~ "cdc_changelog"
  end
end
