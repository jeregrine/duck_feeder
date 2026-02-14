defmodule DuckFeeder.Meta.StoreIntegrationTest do
  use ExUnit.Case, async: false

  alias DuckFeeder.Meta
  alias DuckFeeder.CDC.ConnectionOptions

  @moduletag :integration

  setup_all do
    integration_config = Application.get_env(:duck_feeder, :integration, [])
    pg_url = Keyword.get(integration_config, :meta_database_url)

    assert is_binary(pg_url) and pg_url != "",
           "set :duck_feeder, :integration, meta_database_url in config/test.exs"

    {:ok, conn_opts} = ConnectionOptions.parse_url(pg_url)
    {:ok, conn} = Postgrex.start_link(conn_opts ++ [types: DuckFeeder.Postgrex.Types])
    assert {:ok, _} = Postgrex.query(conn, "DROP SCHEMA IF EXISTS ducklake_metadata CASCADE", [])
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

  test "get_source and list_designated_tables", %{
    conn: conn,
    source_id: source_id,
    designated_table_id: designated_table_id,
    unique: unique
  } do
    source_name = "itest_source_#{unique}"

    assert {:ok, %{id: ^source_id, name: ^source_name}} = Meta.get_source(conn, source_name)

    assert {:ok, designated_tables} = Meta.list_designated_tables(conn, source_id: source_id)
    assert [%{id: ^designated_table_id}] = designated_tables

    assert {:error, {:source_not_found, "missing-source"}} =
             Meta.get_source(conn, "missing-source")
  end

  test "fetch_source_start_lsn default and checkpoint behavior", %{
    conn: conn,
    source_id: source_id,
    designated_table_id: designated_table_id
  } do
    assert {:ok, "0/5"} = Meta.fetch_source_start_lsn(conn, source_id, "0/5")

    assert {:ok, "0/16B6A98"} =
             Meta.upsert_checkpoint(conn, designated_table_id, "0/16B6A98")

    assert {:ok, "0/16B6A98"} = Meta.fetch_source_start_lsn(conn, source_id)
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

  test "commit_uploaded_batch advances checkpoint and is idempotent", %{
    conn: conn,
    designated_table_id: designated_table_id,
    unique: unique
  } do
    batch_id = "batch_commit_#{unique}"

    assert {:ok, _} =
             Meta.insert_batch(conn, %{
               batch_id: batch_id,
               designated_table_id: designated_table_id,
               lsn_start: "0/16B6C10",
               lsn_end: "0/16B6D00",
               state: :uploaded
             })

    assert {:ok, "0/0"} = Meta.fetch_checkpoint(conn, designated_table_id)

    assert {:ok,
            %{
              batch_id: ^batch_id,
              checkpoint_lsn: "0/16B6D00",
              committed?: true,
              already_committed?: false
            }} = Meta.commit_uploaded_batch(conn, batch_id)

    assert {:ok,
            %{
              batch_id: ^batch_id,
              checkpoint_lsn: "0/16B6D00",
              committed?: true,
              already_committed?: true
            }} = Meta.commit_uploaded_batch(conn, batch_id)

    assert {:ok, :committed} = Meta.get_batch_state(conn, batch_id)
    assert {:ok, "0/16B6D00"} = Meta.fetch_checkpoint(conn, designated_table_id)

    low_batch_id = "batch_commit_low_#{unique}"

    assert {:ok, _} =
             Meta.insert_batch(conn, %{
               batch_id: low_batch_id,
               designated_table_id: designated_table_id,
               lsn_start: "0/16B6A10",
               lsn_end: "0/16B6B00",
               state: :uploaded
             })

    assert {:ok, %{checkpoint_lsn: "0/16B6D00"}} =
             Meta.commit_uploaded_batch(conn, low_batch_id)

    assert {:ok, "0/16B6D00"} = Meta.fetch_checkpoint(conn, designated_table_id)
  end

  test "commit_uploaded_batch rejects non-uploaded states", %{
    conn: conn,
    designated_table_id: designated_table_id,
    unique: unique
  } do
    batch_id = "batch_commit_invalid_#{unique}"

    assert {:ok, _} =
             Meta.insert_batch(conn, %{
               batch_id: batch_id,
               designated_table_id: designated_table_id,
               lsn_start: "0/16B6C10",
               lsn_end: "0/16B6C40",
               state: :pending
             })

    assert {:error, {:invalid_batch_commit_state, :pending}} =
             Meta.commit_uploaded_batch(conn, batch_id)
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

    assert {:ok, [%{batch_id: ^batch_id, object_key: "raw/users/part-0001.parquet"}]} =
             Meta.list_batch_files(conn, batch_id)

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
