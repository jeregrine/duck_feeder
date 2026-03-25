defmodule DuckFeeder.TelemetryForwarderTest do
  use ExUnit.Case, async: false

  alias DuckFeeder.TelemetryForwarder

  test "forwards non-duck-feeder telemetry events as raw rows" do
    caller = self()

    append_fun = fn _stream, _table, row, _opts ->
      send(caller, {:append_row, row})
      :ok
    end

    handler_id = "duck-feeder-forwarder-raw-#{System.unique_integer([:positive])}"

    {:ok, forwarder} =
      TelemetryForwarder.start_link(
        stream: self(),
        table: "app_events",
        events: [[:acme, :custom, :event]],
        include_duck_feeder_events?: false,
        handler_id: handler_id,
        append_fun: append_fun
      )

    on_exit(fn ->
      if Process.alive?(forwarder), do: GenServer.stop(forwarder)
    end)

    :telemetry.execute([:acme, :custom, :event], %{duration: 12}, %{tenant: "t1"})

    assert_receive {:append_row, row}, 500
    assert row["type"] == "telemetry_event"
    assert row["event"] == "acme.custom.event"
    assert row["measurements"]["duration"] == 12
    assert row["metadata"]["tenant"] == "t1"
  end

  test "summarizes duck_feeder events in debounce windows" do
    caller = self()

    append_fun = fn _stream, _table, row, _opts ->
      send(caller, {:append_row, row})
      :ok
    end

    handler_id = "duck-feeder-forwarder-summary-#{System.unique_integer([:positive])}"
    unique_source = "summary-#{System.unique_integer([:positive])}"

    {:ok, forwarder} =
      TelemetryForwarder.start_link(
        stream: self(),
        table: "app_events",
        events: [[:duck_feeder, :cdc, :lag]],
        include_duck_feeder_events?: false,
        summarize_duck_feeder?: true,
        summary_debounce_ms: 20,
        summary_max_window_ms: 200,
        handler_id: handler_id,
        append_fun: append_fun
      )

    on_exit(fn ->
      if Process.alive?(forwarder), do: GenServer.stop(forwarder)
    end)

    DuckFeeder.Telemetry.cdc_lag(%{lag_bytes: 10}, %{source: unique_source})
    DuckFeeder.Telemetry.cdc_lag(%{lag_bytes: 20}, %{source: unique_source})

    assert_receive {:append_row,
                    %{"type" => "duck_feeder_summary", "source" => ^unique_source} = row},
                   500

    assert row["event"] == "duck_feeder.cdc.lag"
    assert row["count"] == 2
    assert row["measurements"]["lag_bytes"]["max"] == 20
    assert row["measurements"]["lag_bytes"]["min"] == 10
    assert row["measurements"]["lag_bytes"]["count"] == 2
  end

  test "suppresses duck_feeder recursion during summary flush" do
    caller = self()
    handler_id = "duck-feeder-forwarder-recursion-#{System.unique_integer([:positive])}"
    unique_source = "recursive-#{System.unique_integer([:positive])}"

    append_fun = fn _stream, _table, row, _opts ->
      send(caller, {:append_row, row})

      if row["type"] == "duck_feeder_summary" do
        DuckFeeder.Telemetry.cdc_lag(%{lag_bytes: 999}, %{source: "self-generated"})
      end

      :ok
    end

    {:ok, forwarder} =
      TelemetryForwarder.start_link(
        stream: self(),
        table: "app_events",
        events: [[:duck_feeder, :cdc, :lag]],
        include_duck_feeder_events?: false,
        summarize_duck_feeder?: true,
        summary_debounce_ms: 10,
        summary_max_window_ms: 200,
        summary_suppress_own_events_ms: 150,
        handler_id: handler_id,
        append_fun: append_fun
      )

    on_exit(fn ->
      if Process.alive?(forwarder), do: GenServer.stop(forwarder)
    end)

    DuckFeeder.Telemetry.cdc_lag(%{lag_bytes: 11}, %{source: unique_source})

    assert_receive {:append_row, %{"type" => "duck_feeder_summary", "source" => ^unique_source}},
                   500

    refute_receive {:append_row,
                    %{"type" => "duck_feeder_summary", "source" => "self-generated"}},
                   250
  end

  test "marks large metadata payloads as truncated and supports configurable limits" do
    caller = self()

    append_fun = fn _stream, _table, row, _opts ->
      send(caller, {:append_row, row})
      :ok
    end

    handler_id = "duck-feeder-forwarder-truncation-#{System.unique_integer([:positive])}"

    {:ok, forwarder} =
      TelemetryForwarder.start_link(
        stream: self(),
        table: "app_events",
        events: [[:acme, :custom, :event]],
        include_duck_feeder_events?: false,
        handler_id: handler_id,
        append_fun: append_fun,
        normalize_term_max_items: 2,
        normalize_term_max_depth: 3
      )

    on_exit(fn ->
      if Process.alive?(forwarder), do: GenServer.stop(forwarder)
    end)

    :telemetry.execute(
      [:acme, :custom, :event],
      %{durations: [1, 2, 3]},
      %{values: [10, 20, 30], tags: %{a: 1, b: 2, c: 3}}
    )

    assert_receive {:append_row, row}, 500

    assert row["measurements"]["durations"]["__duck_feeder_type__"] == "list"
    assert row["measurements"]["durations"]["__duck_feeder_truncated__"] == true
    assert row["measurements"]["durations"]["__duck_feeder_original_count__"] == 3
    assert row["measurements"]["durations"]["__duck_feeder_items__"] == [1, 2]

    assert row["metadata"]["values"]["__duck_feeder_truncated__"] == true
    assert row["metadata"]["tags"]["__duck_feeder_truncated__"] == true
    assert row["metadata"]["tags"]["__duck_feeder_original_count__"] == 3
  end

  test "requires at least one telemetry event to attach" do
    handler_id = "duck-feeder-forwarder-empty-#{System.unique_integer([:positive])}"

    assert {:error, {:invalid_option, :events, :empty}} =
             GenServer.start(
               TelemetryForwarder,
               stream: self(),
               events: [],
               include_duck_feeder_events?: false,
               handler_id: handler_id,
               append_fun: fn _, _, _, _ -> :ok end
             )
  end
end
