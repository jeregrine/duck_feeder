defmodule DuckFeeder.CDC.ConnectionIntegrationTest do
  use ExUnit.Case, async: false

  alias DuckFeeder.CDC.{Bootstrap, Connection, ConnectionOptions, Event, Setup}

  @pg_url System.get_env("DUCK_FEEDER_SOURCE_DATABASE_URL")

  @moduletag :integration
  @moduletag skip: if(is_nil(@pg_url), do: "set DUCK_FEEDER_SOURCE_DATABASE_URL", else: false)

  setup_all do
    {:ok, conn} = Postgrex.start_link(url: @pg_url)

    unique = System.unique_integer([:positive, :monotonic])
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
      _ = GenServer.stop(conn)
    end)

    {:ok, conn: conn, table: table, publication: publication, slot: slot}
  end

  test "streams insert transaction events", %{
    conn: conn,
    table: table,
    publication: publication,
    slot: slot
  } do
    assert {:ok, bootstrap} =
             Bootstrap.bootstrap(conn, %{
               publication_name: publication,
               slot_name: slot,
               designated_tables: [%{source_schema: "public", source_table: table}]
             })

    assert {:ok, connection_opts} = ConnectionOptions.parse_url(@pg_url)

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

    # Give replication stream a moment to initialize.
    Process.sleep(100)

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
end
