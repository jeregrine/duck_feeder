defmodule DuckFeeder.BatchProcessor do
  @moduledoc """
  Processes flushed table batches through write, upload, and meta commit steps.
  """

  alias DuckFeeder.{Meta, Storage, Writer}

  @type context :: %{
          required(:meta_conn) => term(),
          required(:designated_table_by_target) => %{
            optional({String.t(), String.t()}) => pos_integer()
          },
          required(:writer) => map(),
          required(:storage) => map(),
          optional(:object_prefix) => String.t(),
          optional(:meta_module) => module()
        }

  @type batch :: %{
          required(:rows) => [map()],
          required(:lsn_start) => String.t(),
          required(:lsn_end) => String.t(),
          optional(:row_count) => non_neg_integer()
        }

  @spec process_batch(context(), {String.t(), String.t()}, batch()) ::
          {:ok, map()} | {:error, term()}
  def process_batch(context, table, batch)
      when is_map(context) and is_tuple(table) and is_map(batch) do
    result = do_process_batch(context, table, batch)
    DuckFeeder.Telemetry.batch_processed(table, result)
    result
  end

  defp do_process_batch(context, table, batch) do
    meta = Map.get(context, :meta_module, Meta)
    conn = Map.fetch!(context, :meta_conn)

    with {:ok, designated_table_id} <- designated_table_id(context, table),
         {:ok, batch_id} <- build_batch_id(meta, designated_table_id, batch),
         {:ok, insert_result} <-
           meta.insert_batch(conn, %{
             batch_id: batch_id,
             designated_table_id: designated_table_id,
             lsn_start: batch.lsn_start,
             lsn_end: batch.lsn_end,
             state: :pending
           }) do
      if insert_result.state == :committed do
        {:ok,
         %{
           status: :already_committed,
           batch_id: batch_id,
           designated_table_id: designated_table_id,
           checkpoint_lsn: batch.lsn_end
         }}
      else
        process_uncommitted_batch(
          context,
          meta,
          conn,
          table,
          batch,
          batch_id,
          insert_result.state
        )
      end
    end
  end

  defp process_uncommitted_batch(context, meta, conn, table, batch, batch_id, current_state) do
    with {:ok, :encoded} <- advance_to(meta, conn, batch_id, current_state, :encoded),
         {:ok, write_result} <- Writer.write_batch(context.writer, %{rows: batch.rows}) do
      result =
        finalize_written_batch(context, meta, conn, table, batch, batch_id, write_result)

      _ = Writer.cleanup(context.writer, write_result)

      case result do
        {:ok, _} = ok ->
          ok

        {:error, reason} = error ->
          mark_failed(meta, conn, batch_id, reason)
          error
      end
    else
      {:error, reason} = error ->
        mark_failed(meta, conn, batch_id, reason)
        error
    end
  end

  defp finalize_written_batch(context, meta, conn, table, batch, batch_id, write_result) do
    with {:ok, object_key} <- object_key(context, table, batch, batch_id, write_result.format),
         {:ok, upload_result} <-
           Storage.put_file(context.storage, write_result.local_path, object_key),
         {:ok, _id} <-
           meta.put_batch_file(conn, %{
             batch_id: batch_id,
             object_key: object_key,
             row_count: Map.get(batch, :row_count, length(batch.rows)),
             file_size: write_result.file_size_bytes,
             etag: upload_result.etag,
             checksum: nil
           }),
         {:ok, :uploaded} <- advance_to(meta, conn, batch_id, :encoded, :uploaded),
         {:ok, commit_result} <- meta.commit_uploaded_batch(conn, batch_id) do
      {:ok,
       %{
         status: :committed,
         batch_id: batch_id,
         designated_table_id: commit_result.designated_table_id,
         checkpoint_lsn: commit_result.checkpoint_lsn,
         object_key: object_key,
         row_count: write_result.row_count,
         file_size_bytes: write_result.file_size_bytes
       }}
    end
  end

  defp designated_table_id(context, table) do
    context
    |> Map.fetch!(:designated_table_by_target)
    |> Map.get(table)
    |> case do
      nil -> {:error, {:unknown_target_table, table}}
      id -> {:ok, id}
    end
  end

  defp build_batch_id(meta, designated_table_id, batch) do
    if function_exported?(meta, :build_batch_id, 4) do
      {:ok, meta.build_batch_id(designated_table_id, batch.lsn_start, batch.lsn_end, [0])}
    else
      {:ok, Meta.build_batch_id(designated_table_id, batch.lsn_start, batch.lsn_end, [0])}
    end
  end

  defp advance_to(meta, conn, batch_id, current_state, target_state)

  defp advance_to(_meta, _conn, _batch_id, state, state), do: {:ok, state}

  defp advance_to(_meta, _conn, _batch_id, :committed, target_state)
       when target_state != :committed do
    {:error, {:invalid_batch_state_path, :committed, target_state}}
  end

  defp advance_to(meta, conn, batch_id, current_state, target_state) do
    next_state = next_state(current_state)

    with {:ok, %{to: ^next_state}} <- meta.transition_batch(conn, batch_id, next_state) do
      advance_to(meta, conn, batch_id, next_state, target_state)
    end
  end

  defp next_state(:pending), do: :encoded
  defp next_state(:encoded), do: :uploaded
  defp next_state(:uploaded), do: :committed
  defp next_state(:failed), do: :pending
  defp next_state(other), do: other

  defp object_key(context, {schema, table}, batch, batch_id, format) do
    prefix = Map.get(context, :object_prefix, "duck_feeder")
    lsn_start = sanitize_lsn(batch.lsn_start)
    lsn_end = sanitize_lsn(batch.lsn_end)
    extension = Atom.to_string(format)

    {:ok, "#{prefix}/#{schema}.#{table}/lsn_#{lsn_start}_#{lsn_end}/#{batch_id}.#{extension}"}
  end

  defp sanitize_lsn(lsn) when is_binary(lsn), do: String.replace(lsn, "/", "_")

  defp mark_failed(meta, conn, batch_id, reason) do
    _ = meta.transition_batch(conn, batch_id, :failed, error_message: inspect(reason))
    :ok
  end
end
