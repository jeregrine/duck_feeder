defmodule DuckFeeder.Meta.StoreIntegrationTest do
  use ExUnit.Case, async: false

  alias DuckFeeder.Meta

  @pg_url System.get_env("DUCK_FEEDER_META_DATABASE_URL")

  @moduletag :integration
  @moduletag skip: if(is_nil(@pg_url), do: "set DUCK_FEEDER_META_DATABASE_URL", else: false)

  setup_all do
    {:ok, conn} = Postgrex.start_link(url: @pg_url)
    assert :ok = Meta.bootstrap(conn)

    on_exit(fn ->
      GenServer.stop(conn)
    end)

    {:ok, conn: conn}
  end

  setup %{conn: conn} do
    unique = System.unique_integer([:positive, :monotonic])

    {:ok, source_id} =
      Meta.register_source(conn, %{
        name: "itest_source_#{unique}",
        connection_info: %{"dsn" => "postgres://source/#{unique}"},
        slot_name: "slot_#{unique}",
        publication_name: "pub_#{unique}",
        status: "active"
      })

    {:ok, designated_table_id} =
      Meta.register_designated_table(conn, %{
        source_id: source_id,
        source_schema: "public",
        source_table: "users_#{unique}",
        target_schema: "raw",
        target_table: "users_#{unique}",
        mode: "cdc_changelog",
        primary_keys: ["id"]
      })

    {:ok,
     conn: conn, source_id: source_id, designated_table_id: designated_table_id, unique: unique}
  end

  test "checkpoint roundtrip", %{conn: conn, designated_table_id: designated_table_id} do
    assert {:ok, "0/0"} = Meta.fetch_checkpoint(conn, designated_table_id)

    assert {:ok, "0/16B6A98"} =
             Meta.upsert_checkpoint(conn, designated_table_id, "0/16B6A98")

    assert {:ok, "0/16B6A98"} = Meta.fetch_checkpoint(conn, designated_table_id)
  end

  test "batch lifecycle, retry path, and invalid transition", %{
    conn: conn,
    designated_table_id: designated_table_id,
    unique: unique
  } do
    batch_id = "batch_#{unique}"

    attrs = %{
      batch_id: batch_id,
      designated_table_id: designated_table_id,
      lsn_start: "0/16B6A98",
      lsn_end: "0/16B6AF0",
      state: :pending
    }

    assert {:ok, %{batch_id: ^batch_id, inserted?: true, state: :pending}} =
             Meta.insert_batch(conn, attrs)

    assert {:ok, %{batch_id: ^batch_id, inserted?: false, state: :pending}} =
             Meta.insert_batch(conn, attrs)

    assert {:ok, %{from: :pending, to: :encoded}} =
             Meta.transition_batch(conn, batch_id, :encoded)

    assert {:ok, %{from: :encoded, to: :failed}} = Meta.transition_batch(conn, batch_id, :failed)

    assert {:ok, %{from: :failed, to: :pending}} = Meta.transition_batch(conn, batch_id, :pending)

    assert {:ok, %{from: :pending, to: :encoded}} =
             Meta.transition_batch(conn, batch_id, :encoded)

    assert {:ok, %{from: :encoded, to: :uploaded}} =
             Meta.transition_batch(conn, batch_id, :uploaded)

    assert {:ok, %{from: :uploaded, to: :committed}} =
             Meta.transition_batch(conn, batch_id, :committed)

    assert {:error, {:invalid_batch_transition, :committed, :pending}} =
             Meta.transition_batch(conn, batch_id, :pending)

    assert {:ok, %Postgrex.Result{rows: [[1, "committed"]]}} =
             Postgrex.query(
               conn,
               "SELECT retry_count, state FROM duckfeeder_meta.batches WHERE batch_id = $1",
               [batch_id]
             )
  end

  test "put_batch_file upserts existing object key", %{
    conn: conn,
    designated_table_id: designated_table_id,
    unique: unique
  } do
    batch_id = "batch_file_#{unique}"

    assert {:ok, _} =
             Meta.insert_batch(conn, %{
               batch_id: batch_id,
               designated_table_id: designated_table_id,
               lsn_start: "0/16B6B10",
               lsn_end: "0/16B6B90",
               state: :uploaded
             })

    assert {:ok, _id1} =
             Meta.put_batch_file(conn, %{
               batch_id: batch_id,
               object_key: "raw/users/part-0001.parquet",
               row_count: 10,
               file_size: 100,
               checksum: "abc",
               etag: "etag-1"
             })

    assert {:ok, _id2} =
             Meta.put_batch_file(conn, %{
               batch_id: batch_id,
               object_key: "raw/users/part-0001.parquet",
               row_count: 20,
               file_size: 200,
               checksum: "def",
               etag: "etag-2"
             })

    assert {:ok, %Postgrex.Result{rows: [[20, 200, "def", "etag-2"]]}} =
             Postgrex.query(
               conn,
               """
               SELECT row_count, file_size, checksum, etag
               FROM duckfeeder_meta.batch_files
               WHERE batch_id = $1 AND object_key = $2
               """,
               [batch_id, "raw/users/part-0001.parquet"]
             )
  end
end
