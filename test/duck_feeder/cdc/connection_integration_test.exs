defmodule DuckFeeder.CDC.ConnectionIntegrationTest do
  use ExUnit.Case, async: false

  alias DuckFeeder.CDC.{Bootstrap, Connection, ConnectionOptions, Event, Setup}

  @moduletag :integration

  setup_all do
    integration_config = Application.get_env(:duck_feeder, :integration, [])
    pg_url = Keyword.get(integration_config, :source_database_url)

    assert is_binary(pg_url) and pg_url != "",
           "set :duck_feeder, :integration, source_database_url in config/test.exs"

    {:ok, conn_opts} = ConnectionOptions.parse_url(pg_url)
    {:ok, conn} = Postgrex.start_link(conn_opts ++ [types: DuckFeeder.Postgrex.Types])

    on_exit(fn ->
      _ = GenServer.stop(conn)
    end)

    {:ok, conn: conn, pg_url: pg_url}
  end

  setup %{conn: conn} do
    unique =
      "#{System.system_time(:microsecond)}_#{System.unique_integer([:positive, :monotonic])}"

    table = "duck_feeder_cdc_itest_#{unique}"
    publication = "duck_feeder_pub_#{unique}"
    slot = "duck_feeder_slot_#{unique}"

    assert {:ok, _} =
             Postgrex.query(
               conn,
               "CREATE TABLE public.\"#{table}\" (id integer PRIMARY KEY, name text)",
               []
             )

    assert {:ok, _} =
             Postgrex.query(conn, "ALTER TABLE public.\"#{table}\" REPLICA IDENTITY FULL", [])

    on_exit(fn ->
      _ = Setup.drop_slot(conn, slot)
      _ = Postgrex.query(conn, "DROP PUBLICATION IF EXISTS \"#{publication}\"", [])
      _ = Postgrex.query(conn, "DROP TABLE IF EXISTS public.\"#{table}\"", [])
    end)

    {:ok, conn: conn, table: table, publication: publication, slot: slot}
  end

  test "streams insert transaction events", %{
    conn: conn,
    table: table,
    publication: publication,
    slot: slot,
    pg_url: pg_url
  } do
    {:ok, cdc_conn} = start_cdc_connection(conn, pg_url, table, publication, slot)

    assert {:ok, _} =
             Postgrex.query(
               conn,
               "INSERT INTO public.\"#{table}\" (id, name) VALUES (1, 'duck')",
               []
             )

    assert_receive {:duck_feeder_cdc_event, %Event.Relation{table: ^table}}, 5_000
    assert_receive {:duck_feeder_cdc_event, %Event.Begin{}}, 5_000

    assert_receive {:duck_feeder_cdc_event,
                    %Event.Insert{record: %{"id" => "1", "name" => "duck"}}},
                   5_000

    assert_receive {:duck_feeder_cdc_event, %Event.Commit{}}, 5_000

    GenServer.stop(cdc_conn)
  end

  test "streams additive schema change relation metadata before row changes", %{
    conn: conn,
    table: table,
    publication: publication,
    slot: slot,
    pg_url: pg_url
  } do
    {:ok, cdc_conn} = start_cdc_connection(conn, pg_url, table, publication, slot)

    assert {:ok, _} =
             Postgrex.query(conn, "ALTER TABLE public.\"#{table}\" ADD COLUMN email text", [])

    assert {:ok, _} =
             Postgrex.query(
               conn,
               "INSERT INTO public.\"#{table}\" (id, name, email) VALUES (1, 'duck', 'duck@example.com')",
               []
             )

    assert_receive {:duck_feeder_cdc_event, %Event.Relation{table: ^table, columns: columns}},
                   5_000

    assert Enum.map(columns, & &1.name) == ["id", "name", "email"]

    assert_receive {:duck_feeder_cdc_event, %Event.Begin{}}, 5_000

    assert_receive {:duck_feeder_cdc_event,
                    %Event.Insert{
                      record: %{
                        "id" => "1",
                        "name" => "duck",
                        "email" => "duck@example.com"
                      }
                    }},
                   5_000

    assert_receive {:duck_feeder_cdc_event, %Event.Commit{}}, 5_000

    GenServer.stop(cdc_conn)
  end

  defp start_cdc_connection(conn, pg_url, table, publication, slot) do
    assert {:ok, bootstrap} =
             Bootstrap.bootstrap(conn, %{
               publication_name: publication,
               slot_name: slot,
               designated_tables: [%{source_schema: "public", source_table: table}]
             })

    assert {:ok, connection_opts} = ConnectionOptions.parse_url(pg_url)

    assert {:ok, cdc_conn} =
             Connection.start_link(
               connection_opts: connection_opts,
               slot_name: slot,
               publication_name: publication,
               start_lsn: bootstrap.start_lsn,
               event_sink: self(),
               auto_reconnect: false,
               status_interval_ms: 500
             )

    Process.sleep(100)

    {:ok, cdc_conn}
  end
end
