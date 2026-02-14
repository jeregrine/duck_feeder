Mix.Task.run("app.start")
Code.require_file("support/fake_components.exs", __DIR__)

alias DuckFeeder.BatchProcessor
alias DuckFeeder.Bench.{FakeMeta, FakeStorage, FakeWriter}

{:ok, _} = FakeMeta.start_link()

storage = %{provider: :s3, bucket: "bench", adapter: FakeStorage}

context = %{
  meta_conn: :bench,
  designated_table_by_target: %{{"raw", "users"} => 1},
  writer: %{adapter: FakeWriter},
  storage: storage,
  object_prefix: "bench",
  meta_module: FakeMeta
}

build_batch = fn lsn_counter, row_count ->
  rows =
    for i <- 1..row_count do
      %{
        _op: "I",
        _record: %{"id" => i, "name" => "duck"},
        _commit_lsn: "0/#{Integer.to_string(lsn_counter, 16)}"
      }
    end

  %{
    rows: rows,
    row_count: row_count,
    lsn_start: "0/#{Integer.to_string(lsn_counter, 16)}",
    lsn_end: "0/#{Integer.to_string(lsn_counter + row_count, 16)}"
  }
end

run_once = fn row_count ->
  FakeMeta.reset()
  lsn_counter = System.unique_integer([:positive])
  batch = build_batch.(lsn_counter, row_count)

  case BatchProcessor.process_batch(context, {"raw", "users"}, batch) do
    {:ok, %{status: status, row_count: result_count}}
    when status in [:committed, :already_committed] and result_count == row_count ->
      :ok

    other ->
      raise "unexpected batch processor benchmark result: #{inspect(other)}"
  end
end

quick? = (System.get_env("DUCK_FEEDER_BENCH_QUICK") || "0") in ["1", "true", "TRUE"]

Benchee.run(
  %{
    "cdc_single_writer_batch_500_rows" => fn -> run_once.(500) end,
    "cdc_single_writer_batch_1k_rows" => fn -> run_once.(1_000) end
  },
  time: if(quick?, do: 1, else: 6),
  memory_time: if(quick?, do: 0, else: 2),
  warmup: if(quick?, do: 0, else: 2),
  formatters: [{Benchee.Formatters.Console, extended_statistics: true}]
)
