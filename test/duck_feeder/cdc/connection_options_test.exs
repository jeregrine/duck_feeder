defmodule DuckFeeder.CDC.ConnectionOptionsTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.CDC.ConnectionOptions

  test "parses postgres url" do
    assert {:ok, opts} =
             ConnectionOptions.parse_url(
               "postgres://user:pass@localhost:5433/duck_feeder?sslmode=require"
             )

    assert opts[:hostname] == "localhost"
    assert opts[:port] == 5433
    assert opts[:database] == "duck_feeder"
    assert opts[:username] == "user"
    assert opts[:password] == "pass"
    assert opts[:ssl] == true
  end

  test "resolves from source connection_info url and applies overrides" do
    source = %{
      connection_info: %{
        "dsn" => "postgres://user:pass@localhost:5432/duck_feeder?sslmode=disable"
      }
    }

    assert {:ok, opts} =
             ConnectionOptions.resolve(source, connection_overrides: [hostname: "db.internal"])

    assert opts[:hostname] == "db.internal"
    assert opts[:database] == "duck_feeder"
    assert opts[:ssl] == false
  end

  test "accepts known string override keys from maps" do
    source = %{
      connection_info: %{
        "dsn" => "postgres://user:pass@localhost:5432/duck_feeder?sslmode=disable"
      }
    }

    assert {:ok, opts} =
             ConnectionOptions.resolve(source,
               connection_overrides: %{"hostname" => "db.override.internal"}
             )

    assert opts[:hostname] == "db.override.internal"
  end

  test "resolves from source connection_info host fields" do
    source = %{
      connection_info: %{
        host: "localhost",
        port: "5434",
        database: "duck_feeder",
        username: "postgres",
        password: "secret",
        ssl: true
      }
    }

    assert {:ok, opts} = ConnectionOptions.resolve(source)

    assert opts[:hostname] == "localhost"
    assert opts[:port] == 5434
    assert opts[:database] == "duck_feeder"
    assert opts[:username] == "postgres"
    assert opts[:password] == "secret"
    assert opts[:ssl] == true
  end

  test "returns explicit connection_opts when provided" do
    explicit = [hostname: "direct", port: 5432, database: "db", username: "u"]

    assert {:ok, ^explicit} =
             ConnectionOptions.resolve(%{connection_info: %{}}, connection_opts: explicit)
  end

  test "rejects unknown connection override keys" do
    source = %{
      connection_info: %{
        "dsn" => "postgres://user:pass@localhost:5432/duck_feeder?sslmode=disable"
      }
    }

    assert {:error, {:invalid_connection_override_key, "evil_key"}} =
             ConnectionOptions.resolve(source, connection_overrides: %{"evil_key" => "x"})
  end

  test "rejects invalid connection_overrides types" do
    source = %{
      connection_info: %{
        "dsn" => "postgres://user:pass@localhost:5432/duck_feeder?sslmode=disable"
      }
    }

    assert {:error, {:invalid_option, :connection_overrides, :bad}} =
             ConnectionOptions.resolve(source, connection_overrides: :bad)
  end

  test "returns errors for invalid info" do
    assert {:error, :invalid_scheme} = ConnectionOptions.parse_url("http://localhost/db")
    assert {:error, :missing_connection_info} = ConnectionOptions.resolve(%{connection_info: %{}})
  end
end
