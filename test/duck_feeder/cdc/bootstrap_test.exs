defmodule DuckFeeder.CDC.BootstrapTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.CDC.Bootstrap

  defmodule SetupCreated do
    def ensure_publication(_conn, _publication_name, _designated_tables, _opts),
      do: {:ok, :created}

    def ensure_slot(_conn, slot_name, _plugin, _opts),
      do: {:ok, {:created, %{slot_name: slot_name, lsn: "0/50"}}}
  end

  defmodule SetupExisting do
    def ensure_publication(_conn, _publication_name, _designated_tables, _opts),
      do: {:ok, :exists}

    def ensure_slot(_conn, _slot_name, _plugin, _opts), do: {:ok, :exists}
  end

  test "bootstrap uses created slot lsn as start lsn" do
    query_fun = fn _conn, "SELECT pg_current_wal_lsn()::text", [] ->
      {:ok, %Postgrex.Result{rows: [["0/60"]]}}
    end

    assert {:ok, result} =
             Bootstrap.bootstrap(
               :conn,
               %{
                 publication_name: "duck_pub",
                 slot_name: "duck_slot",
                 designated_tables: [%{source_schema: "public", source_table: "users"}]
               },
               setup_module: SetupCreated,
               query_fun: query_fun
             )

    assert result.publication == :created
    assert result.slot == {:created, %{slot_name: "duck_slot", lsn: "0/50"}}
    assert result.current_lsn == "0/60"
    assert result.start_lsn == "0/50"
    assert result.start_replication_sql =~ "LOGICAL 0/50"
  end

  test "bootstrap uses current lsn when slot already exists" do
    query_fun = fn _conn, "SELECT pg_current_wal_lsn()::text", [] ->
      {:ok, %Postgrex.Result{rows: [["0/60"]]}}
    end

    assert {:ok, result} =
             Bootstrap.bootstrap(
               :conn,
               %{
                 publication_name: "duck_pub",
                 slot_name: "duck_slot",
                 designated_tables: [%{source_schema: "public", source_table: "users"}]
               },
               setup_module: SetupExisting,
               query_fun: query_fun
             )

    assert result.publication == :exists
    assert result.slot == :exists
    assert result.start_lsn == "0/60"
  end

  test "returns error when required keys are missing" do
    assert {:error, {:missing_required, :publication_name}} =
             Bootstrap.bootstrap(:conn, %{slot_name: "s", designated_tables: []},
               setup_module: SetupExisting,
               query_fun: fn _, _, _ -> {:ok, %Postgrex.Result{rows: [["0/0"]]}} end
             )
  end
end
