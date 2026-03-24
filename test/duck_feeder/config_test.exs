defmodule DuckFeeder.ConfigTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.Config

  test "validates and normalizes DuckDB config" do
    setup_fun = fn _conn -> :ok end

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
      duckdb: %{
        path: "/tmp/duck_feeder.duckdb",
        catalog: "lake",
        setup_sql: ["INSTALL ducklake", "LOAD ducklake"],
        setup_fun: setup_fun
      },
      metadata: %{postgres_url: "postgres://meta"},
      ingest: %{max_rows: 5_000}
    }

    assert {:ok, validated} = Config.validate(config)

    assert validated.source.publication_name == "duck_feeder_pub"
    assert validated.duckdb.path == "/tmp/duck_feeder.duckdb"
    assert validated.duckdb.catalog == "lake"
    assert validated.ingest.max_rows == 5_000

    duckdb = Config.duckdb(validated)
    assert duckdb.path == "/tmp/duck_feeder.duckdb"
    assert duckdb.catalog == "lake"
    assert duckdb.setup_sql == ["INSTALL ducklake", "LOAD ducklake"]
    assert duckdb.setup_fun == setup_fun

    assert Config.duckdb_config(validated) == duckdb
  end

  test "preserves duckdb.setup_sql lists" do
    config = %{
      source: %{
        postgres_url: "postgres://source",
        slot_name: "slot",
        publication_name: "pub",
        designated_tables: []
      },
      duckdb: %{
        setup_sql: ["INSTALL ducklake", "LOAD ducklake"]
      },
      metadata: %{postgres_url: "postgres://meta"}
    }

    assert {:ok, validated} = Config.validate(config)
    assert validated.duckdb.setup_sql == ["INSTALL ducklake", "LOAD ducklake"]
  end

  test "rejects unknown string keys without atomizing" do
    config = %{
      "evil_key" => "boom",
      source: %{
        postgres_url: "postgres://source",
        slot_name: "slot",
        publication_name: "pub",
        designated_tables: []
      },
      duckdb: %{
        path: "/tmp/duck_feeder.duckdb"
      },
      metadata: %{postgres_url: "postgres://meta"}
    }

    assert {:error, %ArgumentError{message: message}} = Config.validate(config)
    assert message =~ "unknown config key"
  end

  test "requires duckdb config" do
    config = %{
      source: %{
        postgres_url: "postgres://source",
        slot_name: "slot",
        publication_name: "pub",
        designated_tables: []
      },
      metadata: %{postgres_url: "postgres://meta"}
    }

    assert {:error, error} = Config.validate(config)
    assert Exception.message(error) =~ "required"
    assert Exception.message(error) =~ "duckdb"
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
      duckdb: %{
        path: "/tmp/duck_feeder.duckdb"
      },
      metadata: %{postgres_url: "postgres://meta"}
    }

    assert {:error, %ArgumentError{message: message}} = Config.validate(config)
    assert message =~ "mode"
    assert message =~ "cdc_changelog"
  end
end
