defmodule DuckFeeder.Bench.FakeMeta do
  @moduledoc false

  def start_link do
    case Process.whereis(__MODULE__) do
      nil -> Agent.start_link(fn -> %{batches: %{}, seq: 0} end, name: __MODULE__)
      pid when is_pid(pid) -> {:ok, pid}
    end
  end

  def reset do
    Agent.update(__MODULE__, fn _ -> %{batches: %{}, seq: 0} end)
  end

  def build_batch_id(designated_table_id, lsn_start, lsn_end, _indexes) do
    "bench_#{designated_table_id}_#{String.replace(lsn_start, "/", "_")}_#{String.replace(lsn_end, "/", "_")}_#{System.unique_integer([:positive])}"
  end

  def insert_batch(_conn, attrs) do
    Agent.get_and_update(__MODULE__, fn state ->
      batch_id = attrs.batch_id

      case Map.get(state.batches, batch_id) do
        nil ->
          batch = %{
            batch_id: batch_id,
            designated_table_id: attrs.designated_table_id,
            lsn_end: attrs.lsn_end,
            state: attrs.state
          }

          new_state = put_in(state.batches[batch_id], batch)
          {{:ok, %{batch_id: batch_id, inserted?: true, state: attrs.state}}, new_state}

        existing ->
          {{:ok, %{batch_id: batch_id, inserted?: false, state: existing.state}}, state}
      end
    end)
  end

  def transition_batch(_conn, batch_id, to_state, _opts \\ []) do
    Agent.get_and_update(__MODULE__, fn state ->
      batch = Map.fetch!(state.batches, batch_id)
      updated = %{batch | state: to_state}
      new_state = put_in(state.batches[batch_id], updated)
      {{:ok, %{batch_id: batch_id, from: batch.state, to: to_state}}, new_state}
    end)
  end

  def put_batch_file(_conn, _attrs), do: {:ok, 1}

  def commit_uploaded_batch(_conn, batch_id) do
    Agent.get_and_update(__MODULE__, fn state ->
      batch = Map.fetch!(state.batches, batch_id)
      updated = %{batch | state: :committed}
      new_state = put_in(state.batches[batch_id], updated)

      result =
        {:ok,
         %{
           batch_id: batch_id,
           designated_table_id: batch.designated_table_id,
           checkpoint_lsn: batch.lsn_end
         }}

      {result, new_state}
    end)
  end
end

defmodule DuckFeeder.Bench.FakeWriter do
  @moduledoc false
  @behaviour DuckFeeder.Writer.Adapter

  @impl true
  def write_batch(_config, %{rows: rows}, _opts) do
    {:ok,
     %{
       local_path: "/tmp/duck_feeder_bench_noop",
       row_count: length(rows),
       file_size_bytes: max(length(rows), 1),
       format: :jsonl
     }}
  end

  @impl true
  def cleanup(_config, _write_result), do: :ok
end

defmodule DuckFeeder.Bench.FakeStorage do
  @moduledoc false
  @behaviour DuckFeeder.Storage.Adapter

  @impl true
  def put_file(_config, _local_path, _object_ref, _opts),
    do: {:ok, %{etag: "bench", version_id: nil, size: 1}}

  @impl true
  def head_object(_config, _object_ref), do: {:ok, %{}}

  @impl true
  def delete_object(_config, _object_ref), do: :ok
end
