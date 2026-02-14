defmodule DuckFeeder.MigrationTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.Migrations

  defmodule FakeRepo do
    def query(sql, params, _opts) do
      cond do
        String.starts_with?(String.trim(sql), "CREATE SCHEMA IF NOT EXISTS duckfeeder_meta") ->
          {:ok, %{rows: []}}

        String.contains?(sql, "CREATE TABLE IF NOT EXISTS duckfeeder_meta.migration_versions") ->
          {:ok, %{rows: []}}

        String.starts_with?(
          String.trim(sql),
          "SELECT version FROM duckfeeder_meta.migration_versions"
        ) ->
          case Process.get(:migration_version) do
            nil -> {:ok, %{rows: []}}
            version -> {:ok, %{rows: [[version]]}}
          end

        String.contains?(sql, "INSERT INTO duckfeeder_meta.migration_versions") ->
          [version] = params
          Process.put(:migration_version, version)
          {:ok, %{rows: []}}

        String.starts_with?(String.trim(sql), "DROP SCHEMA IF EXISTS ducklake_metadata") ->
          send(self(), {:migration_query, :drop_ducklake})
          {:ok, %{rows: []}}

        String.starts_with?(String.trim(sql), "DROP SCHEMA IF EXISTS duckfeeder_meta") ->
          send(self(), {:migration_query, :drop_duckfeeder})
          {:ok, %{rows: []}}

        true ->
          count = Process.get(:bootstrap_queries, 0)
          Process.put(:bootstrap_queries, count + 1)
          {:ok, %{rows: []}}
      end
    end
  end

  setup do
    Process.delete(:migration_version)
    Process.delete(:bootstrap_queries)
    :ok
  end

  test "up applies bootstrap statements and records version" do
    assert :ok = Migrations.up(repo: FakeRepo)

    assert Process.get(:migration_version) == Migrations.current_version()
    assert Process.get(:bootstrap_queries, 0) > 0
  end

  test "up is no-op when already at current version" do
    Process.put(:migration_version, Migrations.current_version())

    assert :ok = Migrations.up(repo: FakeRepo)
    assert Process.get(:bootstrap_queries, 0) == 0
  end

  test "down drops metadata schemas" do
    assert :ok = Migrations.down(repo: FakeRepo)

    assert_received {:migration_query, :drop_ducklake}
    assert_received {:migration_query, :drop_duckfeeder}
  end
end
