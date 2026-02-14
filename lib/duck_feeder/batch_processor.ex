defmodule DuckFeeder.BatchProcessor do
  @moduledoc """
  Processes flushed table batches through write, upload, and meta commit steps.
  """

  alias DuckFeeder.{Meta, Storage, Writer}
  alias DuckFeeder.DuckLake.Committer.Noop, as: NoopCommitter

  @type context :: %{
          required(:meta_conn) => term(),
          required(:designated_table_by_target) => %{
            optional({String.t(), String.t()}) => pos_integer()
          },
          required(:writer) => map(),
          required(:storage) => map(),
          optional(:object_prefix) => String.t(),
          optional(:meta_module) => module(),
          optional(:committer_module) => module(),
          optional(:committer_opts) => keyword()
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
    committer_module = Map.get(context, :committer_module, NoopCommitter)
    committer_opts = Map.get(context, :committer_opts, [])

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
         {:ok, prepared_committer_opts} <-
           prepare_delete_file_opts(
             context,
             meta,
             conn,
             table,
             batch,
             batch_id,
             object_key,
             write_result,
             committer_opts
           ),
         {:ok, :uploaded} <- advance_to(meta, conn, batch_id, :encoded, :uploaded),
         {:ok, commit_result} <-
           committer_module.commit_batch(
             conn,
             batch_id,
             Keyword.merge(
               prepared_committer_opts,
               meta_module: meta,
               table: table,
               batch: batch,
               object_key: object_key,
               write_result: write_result
             )
           ) do
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

  defp prepare_delete_file_opts(
         context,
         meta,
         conn,
         table,
         batch,
         batch_id,
         object_key,
         write_result,
         committer_opts
       )
       when is_list(committer_opts) do
    delete_files = resolve_delete_files(committer_opts, table, batch, write_result)
    validate_delete_files? = Keyword.get(committer_opts, :validate_delete_files?, false)

    with {:ok, prepared_delete_files} <-
           materialize_delete_files(
             context,
             meta,
             conn,
             batch_id,
             object_key,
             delete_files,
             validate_delete_files?
           ) do
      {:ok,
       committer_opts
       |> Keyword.delete(:delete_files_fun)
       |> Keyword.delete(:validate_delete_files?)
       |> Keyword.put(:delete_files, prepared_delete_files)
       |> maybe_put_keyword(
         :replace_data_file_ids,
         keyword_bigint_list(committer_opts, :replace_data_file_ids)
       )}
    end
  end

  defp resolve_delete_files(committer_opts, table, batch, write_result) do
    case Keyword.get(committer_opts, :delete_files_fun) do
      fun when is_function(fun, 3) ->
        fun.(table, batch, write_result)

      _ ->
        Keyword.get(committer_opts, :delete_files, [])
    end
    |> List.wrap()
  end

  defp materialize_delete_files(_context, _meta, _conn, _batch_id, _object_key, [], _validate?),
    do: {:ok, []}

  defp materialize_delete_files(
         context,
         meta,
         conn,
         batch_id,
         object_key,
         descriptors,
         validate?
       ) do
    descriptors
    |> Enum.with_index(1)
    |> Enum.reduce_while({:ok, []}, fn {descriptor, index}, {:ok, acc} ->
      case materialize_delete_file(
             context,
             meta,
             conn,
             batch_id,
             object_key,
             descriptor,
             index,
             validate?
           ) do
        {:ok, prepared} -> {:cont, {:ok, [prepared | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, prepared} -> {:ok, Enum.reverse(prepared)}
      {:error, _reason} = error -> error
    end
  end

  defp materialize_delete_file(
         context,
         meta,
         conn,
         batch_id,
         object_key,
         descriptor,
         index,
         validate?
       )
       when is_map(descriptor) do
    path = descriptor_get(descriptor, :path)
    local_path = descriptor_get(descriptor, :local_path)
    rows = descriptor_get(descriptor, :rows, [])

    cond do
      is_list(rows) and rows != [] ->
        materialize_delete_file_from_rows(
          context,
          meta,
          conn,
          batch_id,
          object_key,
          descriptor,
          rows,
          index,
          validate?
        )

      is_binary(local_path) and local_path != "" ->
        materialize_delete_file_from_local_path(
          context,
          meta,
          conn,
          batch_id,
          object_key,
          descriptor,
          local_path,
          index,
          validate?
        )

      is_binary(path) and path != "" ->
        metadata = descriptor_to_delete_file_metadata(descriptor, path)

        with :ok <- maybe_validate_delete_file(context.storage, metadata, validate?) do
          {:ok, metadata}
        end

      true ->
        {:error, {:invalid_delete_file_descriptor, descriptor}}
    end
  end

  defp materialize_delete_file(
         _context,
         _meta,
         _conn,
         _batch_id,
         _object_key,
         descriptor,
         _index,
         _validate?
       ),
       do: {:error, {:invalid_delete_file_descriptor, descriptor}}

  defp materialize_delete_file_from_rows(
         context,
         meta,
         conn,
         batch_id,
         object_key,
         descriptor,
         rows,
         index,
         validate?
       ) do
    if Enum.all?(rows, &is_map/1) do
      writer_config = writer_config_for_delete_file(context.writer, descriptor)

      with {:ok, delete_write_result} <- Writer.write_batch(writer_config, %{rows: rows}) do
        delete_path =
          descriptor_get(descriptor, :path) ||
            default_delete_file_path(object_key, index, delete_write_result.format)

        delete_count =
          normalize_non_neg_integer(
            descriptor_get(descriptor, :delete_count, delete_write_result.row_count),
            delete_write_result.row_count
          )

        result =
          with {:ok, upload_result} <-
                 Storage.put_file(context.storage, delete_write_result.local_path, delete_path),
               {:ok, _id} <-
                 meta.put_batch_file(conn, %{
                   batch_id: batch_id,
                   object_key: delete_path,
                   row_count: delete_count,
                   file_size: delete_write_result.file_size_bytes,
                   etag: upload_result.etag,
                   checksum: nil
                 }),
               :ok <-
                 maybe_validate_delete_file(
                   context.storage,
                   %{path: delete_path, path_is_relative: true},
                   validate?
                 ) do
            {:ok,
             descriptor_to_delete_file_metadata(
               descriptor,
               delete_path,
               delete_count,
               delete_write_result.file_size_bytes,
               Atom.to_string(delete_write_result.format)
             )}
          end

        _ = Writer.cleanup(writer_config, delete_write_result)
        result
      end
    else
      {:error, {:invalid_delete_file_rows, descriptor}}
    end
  end

  defp materialize_delete_file_from_local_path(
         context,
         meta,
         conn,
         batch_id,
         object_key,
         descriptor,
         local_path,
         index,
         validate?
       ) do
    with {:ok, stat} <- File.stat(local_path) do
      delete_path =
        descriptor_get(descriptor, :path) ||
          default_delete_file_path(
            object_key,
            index,
            normalize_file_format(descriptor_get(descriptor, :format), :parquet)
          )

      delete_count = normalize_non_neg_integer(descriptor_get(descriptor, :delete_count, 0), 0)

      file_size =
        normalize_non_neg_integer(
          descriptor_get(descriptor, :file_size_bytes, stat.size),
          stat.size
        )

      with {:ok, upload_result} <- Storage.put_file(context.storage, local_path, delete_path),
           {:ok, _id} <-
             meta.put_batch_file(conn, %{
               batch_id: batch_id,
               object_key: delete_path,
               row_count: delete_count,
               file_size: file_size,
               etag: upload_result.etag,
               checksum: nil
             }),
           :ok <-
             maybe_validate_delete_file(
               context.storage,
               %{path: delete_path, path_is_relative: true},
               validate?
             ) do
        {:ok,
         descriptor_to_delete_file_metadata(
           descriptor,
           delete_path,
           delete_count,
           file_size,
           normalize_file_format(descriptor_get(descriptor, :format), :parquet)
           |> Atom.to_string()
         )}
      end
    end
  end

  defp descriptor_to_delete_file_metadata(descriptor, path),
    do: descriptor_to_delete_file_metadata(descriptor, path, nil, nil, nil)

  defp descriptor_to_delete_file_metadata(descriptor, path, delete_count, file_size, format) do
    %{
      path: path,
      data_file_id: normalize_optional_non_neg_integer(descriptor_get(descriptor, :data_file_id)),
      path_is_relative: descriptor_get(descriptor, :path_is_relative, true),
      format: normalize_format_string(format || descriptor_get(descriptor, :format, "parquet")),
      delete_count:
        normalize_non_neg_integer(
          if(is_nil(delete_count),
            do: descriptor_get(descriptor, :delete_count, 0),
            else: delete_count
          ),
          0
        ),
      file_size_bytes:
        normalize_non_neg_integer(
          if(is_nil(file_size),
            do: descriptor_get(descriptor, :file_size_bytes, 0),
            else: file_size
          ),
          0
        ),
      footer_size: normalize_non_neg_integer(descriptor_get(descriptor, :footer_size, 0), 0),
      encryption_key: normalize_optional_string(descriptor_get(descriptor, :encryption_key))
    }
  end

  defp maybe_validate_delete_file(_storage, _metadata, false), do: :ok

  defp maybe_validate_delete_file(storage, %{path: path, path_is_relative: true}, true)
       when is_binary(path) and path != "" do
    case Storage.head_object(storage, path) do
      {:ok, _meta} -> :ok
      {:error, reason} -> {:error, {:delete_file_missing, path, reason}}
    end
  end

  defp maybe_validate_delete_file(_storage, _metadata, true), do: :ok

  defp writer_config_for_delete_file(writer_config, descriptor) when is_map(writer_config) do
    case normalize_file_format(descriptor_get(descriptor, :format), nil) do
      nil -> writer_config
      format -> Map.put(writer_config, :format, format)
    end
  end

  defp default_delete_file_path(object_key, index, format) do
    extension = format |> normalize_file_format(:parquet) |> Atom.to_string()
    "#{Path.rootname(object_key)}-deletes-#{index}.#{extension}"
  end

  defp normalize_format_string(value) when is_binary(value), do: String.downcase(value)

  defp normalize_format_string(value) when is_atom(value),
    do: value |> Atom.to_string() |> String.downcase()

  defp normalize_format_string(_value), do: "parquet"

  defp normalize_file_format(value, default)
  defp normalize_file_format(nil, default), do: default
  defp normalize_file_format(:parquet_nif, _default), do: :parquet
  defp normalize_file_format(:parquet, _default), do: :parquet
  defp normalize_file_format(:jsonl, _default), do: :jsonl

  defp normalize_file_format(value, default) when is_binary(value) do
    case String.downcase(value) do
      "parquet" -> :parquet
      "parquet_nif" -> :parquet
      "jsonl" -> :jsonl
      _ -> default
    end
  end

  defp normalize_file_format(_value, default), do: default

  defp normalize_non_neg_integer(value, _default) when is_integer(value) and value >= 0, do: value

  defp normalize_non_neg_integer(value, default) when is_binary(value) do
    case Integer.parse(value) do
      {integer, ""} when integer >= 0 -> integer
      _ -> default
    end
  end

  defp normalize_non_neg_integer(_value, default), do: default

  defp normalize_optional_non_neg_integer(value) when is_integer(value) and value >= 0, do: value

  defp normalize_optional_non_neg_integer(value) when is_binary(value) do
    case Integer.parse(value) do
      {integer, ""} when integer >= 0 -> integer
      _ -> nil
    end
  end

  defp normalize_optional_non_neg_integer(_value), do: nil

  defp normalize_optional_string(value) when is_binary(value) and value != "", do: value
  defp normalize_optional_string(_value), do: nil

  defp descriptor_get(map, key, default \\ nil) when is_map(map) and is_atom(key) do
    Map.get(map, key, Map.get(map, Atom.to_string(key), default))
  end

  defp maybe_put_keyword(keyword, _key, []), do: keyword
  defp maybe_put_keyword(keyword, key, value), do: Keyword.put(keyword, key, value)

  defp keyword_bigint_list(keyword, key) when is_list(keyword) do
    keyword
    |> Keyword.get(key, [])
    |> List.wrap()
    |> Enum.flat_map(fn value ->
      case normalize_optional_non_neg_integer(value) do
        nil -> []
        integer -> [integer]
      end
    end)
    |> Enum.uniq()
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
