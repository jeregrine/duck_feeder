defmodule DuckFeeder.Runtime.SnapshotSpool do
  @moduledoc false

  alias DuckFeeder.CDC.Lsn

  @spec collector() ::
          {:ok, (map(), map() -> :ok | {:error, term()}), (-> list() | tuple())}
          | {:error, term()}
  def collector do
    open_snapshot_spool_file(5)
  end

  @spec cleanup_rows_source(term()) :: :ok
  def cleanup_rows_source([]), do: :ok

  def cleanup_rows_source({:spooled_snapshot_rows, path, _row_count}) when is_binary(path),
    do: safe_delete_snapshot_spool(path)

  def cleanup_rows_source({:spooled_snapshot_rows, path, _skip_count, _row_count})
      when is_binary(path),
      do: safe_delete_snapshot_spool(path)

  def cleanup_rows_source(_other), do: :ok

  @spec replay_plan(String.t(), String.t() | nil, term()) ::
          {:ok, %{rows: list() | tuple(), snapshot_lsn_start: String.t() | nil}}
          | {:error, term()}
  def replay_plan(meta_start_lsn, nil, rows_source) when is_binary(meta_start_lsn) do
    _ = cleanup_rows_source(rows_source)
    {:ok, %{rows: [], snapshot_lsn_start: nil}}
  end

  def replay_plan(meta_start_lsn, boundary_lsn, rows_source)
      when is_binary(meta_start_lsn) and is_binary(boundary_lsn) do
    row_count = snapshot_row_source_count(rows_source)

    case Lsn.compare(meta_start_lsn, boundary_lsn) do
      :lt ->
        with {:ok, snapshot_lsn_start_counter} <-
               snapshot_lsn_start_counter(boundary_lsn, row_count),
             {:ok, replayed_count} <-
               replayed_snapshot_row_count(meta_start_lsn, snapshot_lsn_start_counter, row_count),
             {:ok, remaining_rows_source} <-
               snapshot_remaining_rows_source(rows_source, replayed_count) do
          snapshot_lsn_start = Lsn.to_string(snapshot_lsn_start_counter + replayed_count)

          {:ok, %{rows: remaining_rows_source, snapshot_lsn_start: snapshot_lsn_start}}
        end

      :eq ->
        _ = cleanup_rows_source(rows_source)
        {:ok, %{rows: [], snapshot_lsn_start: nil}}

      :gt ->
        _ = cleanup_rows_source(rows_source)
        {:ok, %{rows: [], snapshot_lsn_start: nil}}

      {:error, reason} ->
        _ = cleanup_rows_source(rows_source)
        {:error, {:invalid_snapshot_handoff_lsn, reason}}
    end
  end

  @spec replay_rows(list() | tuple(), (map(), map() -> :ok | {:error, term()})) ::
          :ok | {:error, term()}
  def replay_rows([], _replay_fun), do: :ok

  def replay_rows(rows, replay_fun) when is_list(rows) and is_function(replay_fun, 2) do
    Enum.reduce_while(rows, :ok, fn {designated_table, row}, :ok ->
      case replay_fun.(designated_table, row) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {:snapshot_replay_failed, reason}}}
      end
    end)
  end

  def replay_rows({:spooled_snapshot_rows, path, skip_count, row_count}, replay_fun)
      when is_binary(path) and is_integer(skip_count) and skip_count >= 0 and
             is_integer(row_count) and row_count >= 0 and is_function(replay_fun, 2) do
    replay_result =
      path
      |> File.stream!([], :line)
      |> Stream.drop(skip_count)
      |> Enum.reduce_while(:ok, fn line, :ok ->
        with {:ok, {designated_table, row}} <- decode_snapshot_spooled_row(line),
             :ok <- replay_fun.(designated_table, row) do
          {:cont, :ok}
        else
          {:error, reason} -> {:halt, {:error, {:snapshot_replay_failed, reason}}}
        end
      end)

    case {replay_result, safe_delete_snapshot_spool(path)} do
      {:ok, :ok} -> :ok
      {{:error, _reason} = error, :ok} -> error
      {:ok, {:error, reason}} -> {:error, {:snapshot_replay_failed, reason}}
      {{:error, _reason} = error, {:error, _delete_reason}} -> error
    end
  rescue
    exception ->
      delete_result = safe_delete_snapshot_spool(path)

      case delete_result do
        :ok -> {:error, {:snapshot_replay_failed, {:snapshot_spool_exception, exception}}}
        {:error, reason} -> {:error, {:snapshot_replay_failed, reason}}
      end
  catch
    kind, reason ->
      delete_result = safe_delete_snapshot_spool(path)

      case delete_result do
        :ok -> {:error, {:snapshot_replay_failed, {:snapshot_spool_throw, kind, reason}}}
        {:error, delete_reason} -> {:error, {:snapshot_replay_failed, delete_reason}}
      end
  end

  defp open_snapshot_spool_file(remaining_attempts) when is_integer(remaining_attempts) do
    path = snapshot_spool_path()

    case File.open(path, [:write, :binary, :exclusive]) do
      {:ok, io_device} ->
        counter = :atomics.new(1, [])

        row_handler = fn designated_table, row ->
          snapshot_spool_push(io_device, counter, designated_table, row)
        end

        collect_rows = fn ->
          :ok = File.close(io_device)
          {:spooled_snapshot_rows, path, :atomics.get(counter, 1)}
        end

        {:ok, row_handler, collect_rows}

      {:error, :eexist} when remaining_attempts > 1 ->
        open_snapshot_spool_file(remaining_attempts - 1)

      {:error, reason} ->
        {:error, {:snapshot_collector_start_failed, reason}}
    end
  end

  defp snapshot_spool_path do
    suffix = Base.url_encode64(:crypto.strong_rand_bytes(16), padding: false)
    Path.join(System.tmp_dir!(), "duck_feeder_snapshot_rows_#{suffix}.spool")
  end

  defp snapshot_spool_push(io_device, counter, designated_table, row)
       when is_pid(io_device) and is_reference(counter) do
    encoded =
      {designated_table, row}
      |> :erlang.term_to_binary()
      |> Base.encode64()

    case IO.binwrite(io_device, encoded <> "\n") do
      :ok ->
        _ = :atomics.add_get(counter, 1, 1)
        :ok

      {:error, reason} ->
        {:error, {:snapshot_collector_push_failed, reason}}
    end
  rescue
    exception ->
      {:error, {:snapshot_collector_push_exception, exception}}
  catch
    :exit, reason ->
      {:error, {:snapshot_collector_push_exit, reason}}

    kind, reason ->
      {:error, {:snapshot_collector_push_throw, kind, reason}}
  end

  defp safe_delete_snapshot_spool(path) when is_binary(path) do
    case File.rm(path) do
      :ok -> :ok
      {:error, :enoent} -> :ok
      {:error, reason} -> {:error, {:snapshot_spool_delete_failed, reason}}
    end
  end

  defp snapshot_row_source_count(rows) when is_list(rows), do: length(rows)

  defp snapshot_row_source_count({:spooled_snapshot_rows, _path, row_count})
       when is_integer(row_count) and row_count >= 0,
       do: row_count

  defp snapshot_row_source_count(_rows_source), do: 0

  defp snapshot_remaining_rows_source(rows, replayed_count)
       when is_list(rows) and is_integer(replayed_count) and replayed_count >= 0 do
    {:ok, Enum.drop(rows, replayed_count)}
  end

  defp snapshot_remaining_rows_source({:spooled_snapshot_rows, path, row_count}, replayed_count)
       when is_binary(path) and is_integer(row_count) and row_count >= 0 and
              is_integer(replayed_count) and replayed_count >= 0 do
    if replayed_count >= row_count do
      _ = safe_delete_snapshot_spool(path)
      {:ok, []}
    else
      {:ok, {:spooled_snapshot_rows, path, replayed_count, row_count}}
    end
  end

  defp snapshot_remaining_rows_source(rows_source, _replayed_count),
    do: {:error, {:invalid_snapshot_rows_source, rows_source}}

  defp snapshot_lsn_start_counter(boundary_lsn, row_count)
       when is_binary(boundary_lsn) and is_integer(row_count) and row_count >= 0 do
    with {:ok, boundary} <- Lsn.parse(boundary_lsn) do
      {:ok, max(boundary - row_count, 0)}
    end
  end

  defp replayed_snapshot_row_count(meta_start_lsn, snapshot_lsn_start_counter, row_count)
       when is_binary(meta_start_lsn) and is_integer(snapshot_lsn_start_counter) and
              is_integer(row_count) and row_count >= 0 do
    with {:ok, meta_counter} <- Lsn.parse(meta_start_lsn) do
      replayed = max(meta_counter - snapshot_lsn_start_counter, 0)
      {:ok, min(replayed, row_count)}
    end
  end

  defp decode_snapshot_spooled_row(line) when is_binary(line) do
    trimmed = String.trim(line)

    with {:ok, binary} <- Base.decode64(trimmed),
         {designated_table, row} <- :erlang.binary_to_term(binary, [:safe]) do
      {:ok, {designated_table, row}}
    else
      :error -> {:error, {:invalid_snapshot_spool_row, trimmed}}
      other -> {:error, {:invalid_snapshot_spool_row, other}}
    end
  rescue
    exception ->
      {:error, {:invalid_snapshot_spool_row, exception}}
  end
end
