defmodule DuckFeeder.ServiceTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.CDC.Event
  alias DuckFeeder.Service

  defmodule FakeMeta do
    def build_batch_id(_designated_table_id, _lsn_start, _lsn_end, _indexes), do: "batch-service"

    def insert_batch(_conn, attrs) do
      Process.put({:designated_table_id, attrs.batch_id}, attrs.designated_table_id)
      {:ok, %{batch_id: attrs.batch_id, inserted?: true, state: :pending}}
    end

    def transition_batch(_conn, batch_id, to_state, _opts \\ []) do
      {:ok, %{batch_id: batch_id, from: :pending, to: to_state}}
    end

    def put_batch_file(_conn, _attrs), do: {:ok, 1}

    def commit_uploaded_batch(_conn, batch_id) do
      {:ok,
       %{
         batch_id: batch_id,
         designated_table_id: Process.get({:designated_table_id, batch_id}),
         checkpoint_lsn: "0/120"
       }}
    end

    def upsert_checkpoint(_conn, _designated_table_id, lsn), do: {:ok, lsn}
  end

  defmodule FakeSink do
    @behaviour DuckFeeder.Sink

    @impl true
    def process_batch(context, table, batch) do
      if is_pid(context.meta_conn) do
        send(context.meta_conn, {:fake_sink_batch, context, table, batch})
      end

      {:ok,
       %{
         status: :committed,
         batch_id: "sink-service",
         designated_table_id: Map.fetch!(context.designated_table_by_target, table),
         checkpoint_lsn: batch.lsn_end
       }}
    end
  end

  test "runs end-to-end from CDC event to processed batch" do
    designated_tables = [
      %{
        id: 1,
        source_schema: "public",
        source_table: "users",
        target_schema: "raw",
        target_table: "users"
      }
    ]

    {:ok, service} =
      Service.start_link(
        designated_tables: designated_tables,
        meta_conn: self(),
        sink_module: FakeSink,
        pipeline_opts: %{max_rows: 1, max_bytes: 10_000, flush_interval_ms: 60_000},
        observer_pid: self()
      )

    assert :ok = Service.attach_cdc(service, self())

    assert :buffering =
             Service.push_event(service, %Event.Relation{id: 1, schema: "public", table: "users"})

    assert :buffering =
             Service.push_event(service, %Event.Begin{xid: 700, final_lsn: "0/100"})

    assert :buffering =
             Service.push_event(service, %Event.Insert{relation_id: 1, record: %{"id" => "1"}})

    assert {:committed, %{xid: 700}} =
             Service.push_event(service, %Event.Commit{xid: 700, end_lsn: "0/120"})

    assert_receive {:fake_sink_batch, _context, {"raw", "users"}, batch}, 1_000
    assert_receive {:duck_feeder_batch_processed, {"raw", "users"}, {:ok, result}, ^batch}, 1_000

    assert_receive {:duck_feeder_ack_lsn, "0/120"}, 1_000

    assert result.status == :committed
    assert result.batch_id == "sink-service"
    assert batch.row_count == 1

    refute Service.in_transaction?(service)
  end

  test "auto-selects DuckDB sink and starts internal DuckDB connection without legacy storage" do
    path =
      Path.join(
        System.tmp_dir!(),
        "duck_feeder_service_#{System.unique_integer([:positive])}.duckdb"
      )

    on_exit(fn ->
      _ = File.rm(path)
    end)

    designated_tables = [
      %{
        id: 1,
        source_schema: "public",
        source_table: "users",
        target_schema: "raw",
        target_table: "users",
        primary_keys: ["id"]
      }
    ]

    {:ok, service} =
      Service.start_link(
        designated_tables: designated_tables,
        meta_conn: self(),
        meta_module: FakeMeta,
        duckdb: %{path: path},
        pipeline_opts: %{max_rows: 1, max_bytes: 10_000, flush_interval_ms: 60_000},
        observer_pid: self()
      )

    assert :ok = Service.attach_cdc(service, self())

    assert :buffering =
             Service.push_event(service, %Event.Relation{id: 1, schema: "public", table: "users"})

    assert :buffering =
             Service.push_event(service, %Event.Begin{xid: 702, final_lsn: "0/100"})

    assert :buffering =
             Service.push_event(service, %Event.Insert{
               relation_id: 1,
               record: %{"id" => 3, "name" => "duck"}
             })

    assert {:committed, %{xid: 702}} =
             Service.push_event(service, %Event.Commit{xid: 702, end_lsn: "0/120"})

    assert_receive {:duck_feeder_batch_processed, {"raw", "users"}, {:ok, result}, _batch},
                   1_000

    assert_receive {:duck_feeder_ack_lsn, "0/120"}, 1_000
    assert result.status == :committed
    assert result.checkpoint_lsn == "0/120"

    assert %{"id" => [3], "name" => ["duck"]} =
             query_duckdb_file(path, "SELECT id, name FROM raw.users ORDER BY id")
  end

  test "supports custom sink module without legacy storage config" do
    designated_tables = [
      %{
        id: 1,
        source_schema: "public",
        source_table: "users",
        target_schema: "raw",
        target_table: "users"
      }
    ]

    {:ok, service} =
      Service.start_link(
        designated_tables: designated_tables,
        meta_conn: self(),
        sink_module: FakeSink,
        pipeline_opts: %{max_rows: 1, max_bytes: 10_000, flush_interval_ms: 60_000},
        observer_pid: self()
      )

    assert :ok = Service.attach_cdc(service, self())

    assert :buffering =
             Service.push_event(service, %Event.Relation{id: 1, schema: "public", table: "users"})

    assert :buffering =
             Service.push_event(service, %Event.Begin{xid: 702, final_lsn: "0/100"})

    assert :buffering =
             Service.push_event(service, %Event.Insert{relation_id: 1, record: %{"id" => "3"}})

    assert {:committed, %{xid: 702}} =
             Service.push_event(service, %Event.Commit{xid: 702, end_lsn: "0/120"})

    assert_receive {:fake_sink_batch, context, {"raw", "users"}, batch}, 1_000
    refute Map.has_key?(context, :storage)
    assert context.sink_module == FakeSink
    assert batch.row_count == 1

    assert_receive {:duck_feeder_batch_processed, {"raw", "users"}, {:ok, result}, _batch},
                   1_000

    assert_receive {:duck_feeder_ack_lsn, "0/120"}, 1_000

    assert result.status == :committed
    assert result.batch_id == "sink-service"
  end

  test "attach_cdc emits latest checkpoint ack after prior commits" do
    designated_tables = [
      %{
        id: 1,
        source_schema: "public",
        source_table: "users",
        target_schema: "raw",
        target_table: "users"
      }
    ]

    {:ok, service} =
      Service.start_link(
        designated_tables: designated_tables,
        meta_conn: self(),
        sink_module: FakeSink,
        pipeline_opts: %{max_rows: 1, max_bytes: 10_000, flush_interval_ms: 60_000},
        observer_pid: self()
      )

    assert :buffering =
             Service.push_event(service, %Event.Relation{id: 1, schema: "public", table: "users"})

    assert :buffering =
             Service.push_event(service, %Event.Begin{xid: 701, final_lsn: "0/100"})

    assert :buffering =
             Service.push_event(service, %Event.Insert{relation_id: 1, record: %{"id" => "2"}})

    assert {:committed, %{xid: 701}} =
             Service.push_event(service, %Event.Commit{xid: 701, end_lsn: "0/120"})

    assert_receive {:duck_feeder_batch_processed, {"raw", "users"}, {:ok, _result}, _batch}, 1_000

    assert :ok = Service.attach_cdc(service, self())
    assert_receive {:duck_feeder_ack_lsn, "0/120"}, 1_000
  end

  test "ingests pre-tagged snapshot rows without re-wrapping metadata into _record" do
    designated_table = %{
      id: 1,
      source_schema: "public",
      source_table: "users",
      target_schema: "raw",
      target_table: "users"
    }

    {:ok, service} =
      Service.start_link(
        designated_tables: [designated_table],
        meta_conn: self(),
        sink_module: FakeSink,
        pipeline_opts: %{max_rows: 1, max_bytes: 10_000, flush_interval_ms: 60_000},
        observer_pid: self()
      )

    tagged_row = %{
      "id" => 1,
      "name" => "snapshot-duck",
      _op: "R",
      _commit_lsn: "0/10",
      _xid: 0,
      _source_ts: nil,
      _ingest_ts: DateTime.utc_now(),
      _relation_schema: "public",
      _relation_table: "users"
    }

    assert :ok = Service.ingest_snapshot_row(service, designated_table, tagged_row)

    assert_receive {:duck_feeder_batch_processed, {"raw", "users"}, {:ok, _result}, batch}, 1_000

    [row] = batch.rows
    assert row._op == "R"
    assert row._record["id"] == 1
    refute Map.has_key?(row._record, "_op")
    refute Map.has_key?(row._record, "_commit_lsn")
  end

  test "returns CDC validation errors" do
    {:ok, service} =
      Service.start_link(
        designated_tables: [],
        meta_conn: self(),
        sink_module: FakeSink,
        observer_pid: self()
      )

    assert :buffering =
             Service.push_event(service, %Event.Relation{id: 1, schema: "public", table: "users"})

    assert {:error, :change_outside_transaction} =
             Service.push_event(service, %Event.Insert{relation_id: 1, record: %{}})
  end

  defp query_duckdb_file(path, sql) do
    {:ok, db} = Adbc.Database.start_link(driver: :duckdb, path: path)
    {:ok, conn} = Adbc.Connection.start_link(database: db)

    try do
      conn
      |> Adbc.Connection.query!(sql)
      |> Adbc.Result.to_map()
    after
      safe_stop(conn)
      safe_stop(db)
    end
  end

  defp safe_stop(pid) when is_pid(pid) do
    _ = GenServer.stop(pid)
    :ok
  catch
    :exit, _reason -> :ok
  end
end
