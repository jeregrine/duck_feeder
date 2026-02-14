Mix.Task.run("app.start")
Code.require_file("support/fake_components.exs", __DIR__)

alias DuckFeeder.AppendStream
alias DuckFeeder.Bench.{FakeMeta, FakeStorage, FakeWriter}

{:ok, _} = FakeMeta.start_link()
FakeMeta.reset()

counter = :atomics.new(1, signed: false)

observer =
  spawn(fn ->
    receive_loop = fn receive_loop ->
      receive do
        {:duck_feeder_append_batch_processed, _table, {:ok, _result}, batch} ->
          _ = :atomics.add_get(counter, 1, batch.row_count)
          receive_loop.(receive_loop)

        {:reset_counter, caller} ->
          :ok = :atomics.put(counter, 1, 0)
          send(caller, :counter_reset)
          receive_loop.(receive_loop)
      end
    end

    receive_loop.(receive_loop)
  end)

storage = %{provider: :s3, bucket: "bench", adapter: FakeStorage}

designated_tables = [
  %{id: 1, target_schema: "raw", target_table: "events_a"},
  %{id: 2, target_schema: "raw", target_table: "events_b"},
  %{id: 3, target_schema: "raw", target_table: "events_c"},
  %{id: 4, target_schema: "raw", target_table: "events_d"}
]

{:ok, append_stream} =
  AppendStream.start_link(
    designated_tables: designated_tables,
    meta_conn: :bench,
    meta_module: FakeMeta,
    writer: %{adapter: FakeWriter},
    storage: storage,
    pipeline_opts: %{max_rows: 200, max_bytes: 10_000_000, flush_interval_ms: 60_000},
    observer_pid: observer
  )

flush_tables = fn ->
  Enum.each(["events_a", "events_b", "events_c", "events_d"], fn table ->
    _ = AppendStream.flush_table(append_stream, table)
  end)
end

await_rows = fn expected_rows ->
  wait = fn wait, attempts_left ->
    received = :atomics.get(counter, 1)

    cond do
      received >= expected_rows ->
        :ok

      attempts_left <= 0 ->
        raise "append stream bench timed out waiting for processed rows (received=#{received}, expected=#{expected_rows})"

      true ->
        Process.sleep(10)
        wait.(wait, attempts_left - 1)
    end
  end

  wait.(wait, 1_000)
end

run_case = fn fun, expected_rows ->
  FakeMeta.reset()
  send(observer, {:reset_counter, self()})

  receive do
    :counter_reset -> :ok
  after
    1_000 -> raise "append stream bench failed to reset observer counter"
  end

  fun.()
  flush_tables.()
  await_rows.(expected_rows)
end

quick? = (System.get_env("DUCK_FEEDER_BENCH_QUICK") || "0") in ["1", "true", "TRUE"]

Benchee.run(
  %{
    "append_stream_single_writer_1k" => fn ->
      run_case.(
        fn ->
          for i <- 1..1_000 do
            :ok = AppendStream.append(append_stream, "events_a", %{"id" => i, "v" => "x"})
          end
        end,
        1_000
      )
    end,
    "append_stream_multi_writer_4x250" => fn ->
      run_case.(
        fn ->
          0..3
          |> Task.async_stream(
            fn worker ->
              table = Enum.at(["events_a", "events_b", "events_c", "events_d"], worker)

              for i <- 1..250 do
                :ok = AppendStream.append(append_stream, table, %{"id" => i, "w" => worker})
              end
            end,
            ordered: false,
            timeout: :infinity,
            max_concurrency: 4
          )
          |> Stream.run()
        end,
        1_000
      )
    end
  },
  time: if(quick?, do: 1, else: 6),
  memory_time: if(quick?, do: 0, else: 2),
  warmup: if(quick?, do: 0, else: 2),
  formatters: [{Benchee.Formatters.Console, extended_statistics: true}]
)

GenServer.stop(append_stream)
Process.exit(observer, :normal)
