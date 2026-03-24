defmodule DuckFeeder.Meta.StoreIntegrationTest do
  use ExUnit.Case, async: false

  alias DuckFeeder.CDC.ConnectionOptions
  alias DuckFeeder.Meta

  @moduletag :integration

  setup_all do
    integration_config = Application.get_env(:duck_feeder, :integration, [])
    pg_url = Keyword.get(integration_config, :meta_database_url)

    assert is_binary(pg_url) and pg_url != "",
           "set :duck_feeder, :integration, meta_database_url in config/test.exs"

    {:ok, conn_opts} = ConnectionOptions.parse_url(pg_url)
    {:ok, conn} = Postgrex.start_link(conn_opts ++ [types: DuckFeeder.Postgrex.Types])
    assert {:ok, _} = Postgrex.query(conn, "DROP SCHEMA IF EXISTS duckfeeder_meta CASCADE", [])
    assert :ok = Meta.bootstrap(conn)

    on_exit(fn ->
      GenServer.stop(conn)
    end)

    {:ok, conn: conn}
  end

  setup %{conn: conn} do
    unique =
      "#{System.system_time(:microsecond)}_#{System.unique_integer([:positive, :monotonic])}"

    source_name = "itest_source_#{unique}"
    checkpoint_key = "#{source_name}:raw.users"

    {:ok, conn: conn, source_name: source_name, checkpoint_key: checkpoint_key}
  end

  test "fetch_start_lsn default and checkpoint behavior", %{
    conn: conn,
    checkpoint_key: checkpoint_key
  } do
    assert {:ok, "0/5"} = Meta.fetch_start_lsn(conn, [checkpoint_key], "0/5")

    assert {:ok, "0/16B6A98"} = Meta.upsert_checkpoint(conn, checkpoint_key, "0/16B6A98")

    assert {:ok, "0/16B6A98"} = Meta.fetch_start_lsn(conn, [checkpoint_key])
  end

  test "fetch_start_lsn returns minimum checkpoint across tables", %{
    conn: conn,
    source_name: source_name
  } do
    key_a = "#{source_name}:raw.users"
    key_b = "#{source_name}:raw.orders"

    assert {:ok, "0/20"} = Meta.upsert_checkpoint(conn, key_a, "0/20")
    assert {:ok, "0/30"} = Meta.upsert_checkpoint(conn, key_b, "0/30")

    assert {:ok, "0/20"} = Meta.fetch_start_lsn(conn, [key_a, key_b])
  end

  test "checkpoint roundtrip", %{conn: conn, checkpoint_key: checkpoint_key} do
    assert {:ok, "0/0"} = Meta.fetch_checkpoint(conn, checkpoint_key)

    assert {:ok, "0/16B6A98"} = Meta.upsert_checkpoint(conn, checkpoint_key, "0/16B6A98")

    assert {:ok, "0/16B6A98"} = Meta.fetch_checkpoint(conn, checkpoint_key)
  end

  test "snapshot handoff state roundtrip", %{conn: conn, source_name: source_name} do
    assert {:ok, nil} = Meta.fetch_snapshot_handoff(conn, source_name)

    assert {:ok, "0/16B6A98"} =
             Meta.mark_snapshot_handoff_pending(conn, source_name, "0/16B6A98")

    assert {:ok, pending} = Meta.fetch_snapshot_handoff(conn, source_name)
    assert pending.source_name == source_name
    assert pending.state == :pending
    assert pending.boundary_lsn == "0/16B6A98"
    assert is_nil(pending.completed_at)

    assert {:ok, "0/16B6C40"} =
             Meta.mark_snapshot_handoff_complete(conn, source_name, "0/16B6C40")

    assert {:ok, complete} = Meta.fetch_snapshot_handoff(conn, source_name)
    assert complete.source_name == source_name
    assert complete.state == :complete
    assert complete.boundary_lsn == "0/16B6C40"
    assert %DateTime{} = complete.completed_at

    assert :ok = Meta.clear_snapshot_handoff(conn, source_name)
    assert {:ok, nil} = Meta.fetch_snapshot_handoff(conn, source_name)
  end
end
