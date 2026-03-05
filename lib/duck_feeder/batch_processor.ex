defmodule DuckFeeder.BatchProcessor do
  @moduledoc """
  Processes flushed table batches through write, upload, and meta commit steps.

  Batch state machine:

      pending -> encoded -> uploaded -> committed
                    |
                    +--> failed

  Processing path:

      batch rows
        |
        v
      Writer.write_batch
        |
        v
      Storage.put_file
        |
        v
      Meta.put_batch_file
        |
        v
      committer_module.commit_batch

  Optional poison-row mode (`poison_row_mode: :drop`) can isolate bad rows,
  emit dead-letter signals/telemetry, and continue committing only valid rows.
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

  defp process_uncommitted_batch(context, meta, conn, table, batch, batch_id, :uploaded) do
    case recover_uploaded_batch(context, meta, conn, table, batch, batch_id) do
      {:ok, _committed} = ok ->
        ok

      {:error, reason} = error ->
        mark_failed(meta, conn, batch_id, reason)
        error
    end
  end

  defp process_uncommitted_batch(context, meta, conn, table, batch, batch_id, current_state) do
    with {:ok, :encoded} <- advance_to(meta, conn, batch_id, current_state, :encoded),
         {:ok, write_result, effective_batch} <- write_batch_with_poison_policy(context, batch) do
      result =
        finalize_written_batch(
          context,
          meta,
          conn,
          table,
          effective_batch,
          batch_id,
          write_result
        )

      _ = Writer.cleanup(context.writer, write_result)

      case result do
        {:ok, committed} = ok ->
          dropped_count = max(length(batch.rows) - length(effective_batch.rows), 0)

          if dropped_count > 0 do
            {:ok, Map.put(committed, :dropped_poison_rows, dropped_count)}
          else
            ok
          end

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

    with {:ok, committer_opts} <- normalize_committer_opts(Map.get(context, :committer_opts, [])),
         {:ok, object_key} <- object_key(context, table, batch, batch_id, write_result.format),
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

  defp recover_uploaded_batch(context, meta, conn, table, batch, batch_id) do
    committer_module = Map.get(context, :committer_module, NoopCommitter)

    with {:ok, committer_opts} <- normalize_committer_opts(Map.get(context, :committer_opts, [])),
         {:ok, recovered_file_info} <- recover_uploaded_file_info(meta, conn, batch_id, batch),
         {:ok, prepared_committer_opts} <-
           prepare_recovered_committer_opts(committer_opts, recovered_file_info.delete_files),
         {:ok, commit_result} <-
           committer_module.commit_batch(
             conn,
             batch_id,
             Keyword.merge(
               prepared_committer_opts,
               meta_module: meta,
               table: table,
               batch: batch,
               object_key: recovered_file_info.object_key,
               write_result: recovered_file_info.write_result
             )
           ) do
      {:ok,
       %{
         status: :committed,
         batch_id: batch_id,
         designated_table_id: commit_result.designated_table_id,
         checkpoint_lsn: commit_result.checkpoint_lsn,
         object_key: recovered_file_info.object_key,
         row_count: recovered_file_info.write_result.row_count,
         file_size_bytes: recovered_file_info.write_result.file_size_bytes
       }}
    end
  end

  defp recover_uploaded_file_info(meta, conn, batch_id, batch) do
    fallback_row_count = Map.get(batch, :row_count, length(Map.get(batch, :rows, [])))

    with {:ok, files} <- meta.list_batch_files(conn, batch_id),
         [main_file | delete_files] <- files,
         object_key when is_binary(object_key) and object_key != "" <-
           batch_file_field(main_file, :object_key) do
      row_count =
        normalize_non_neg_integer(
          batch_file_field(main_file, :row_count, fallback_row_count),
          fallback_row_count
        )

      file_size = normalize_non_neg_integer(batch_file_field(main_file, :file_size, 0), 0)

      {:ok,
       %{
         object_key: object_key,
         write_result: %{
           row_count: row_count,
           file_size_bytes: file_size,
           format: infer_file_format_from_key(object_key)
         },
         delete_files: recover_delete_files(delete_files)
       }}
    else
      [] ->
        {:error, {:missing_uploaded_batch_files, batch_id}}

      nil ->
        {:error, {:invalid_uploaded_batch_file, batch_id}}

      {:error, _reason} = error ->
        error

      other ->
        {:error, {:invalid_uploaded_batch_files, batch_id, other}}
    end
  end

  defp prepare_recovered_committer_opts(committer_opts, recovered_delete_files)
       when is_list(committer_opts) do
    {:ok,
     committer_opts
     |> Keyword.delete(:delete_files_fun)
     |> Keyword.delete(:validate_delete_files?)
     |> maybe_put_keyword(:delete_files, recovered_delete_files)
     |> maybe_put_keyword(
       :replace_data_file_ids,
       keyword_bigint_list(committer_opts, :replace_data_file_ids)
     )}
  end

  defp recover_delete_files(files) when is_list(files) do
    Enum.flat_map(files, fn file ->
      case batch_file_field(file, :object_key) do
        path when is_binary(path) and path != "" ->
          [
            %{
              path: path,
              data_file_id: nil,
              path_is_relative: true,
              format: infer_file_format_from_key(path) |> Atom.to_string(),
              delete_count: normalize_non_neg_integer(batch_file_field(file, :row_count, 0), 0),
              file_size_bytes:
                normalize_non_neg_integer(batch_file_field(file, :file_size, 0), 0),
              footer_size: 0,
              encryption_key: nil
            }
          ]

        _ ->
          []
      end
    end)
  end

  defp batch_file_field(file, key, default \\ nil) when is_map(file) and is_atom(key) do
    Map.get(file, key, Map.get(file, Atom.to_string(key), default))
  end

  defp infer_file_format_from_key(object_key) when is_binary(object_key) do
    case object_key |> Path.extname() |> String.downcase() do
      ".jsonl" -> :jsonl
      ".parquet" -> :parquet
      _ -> :parquet
    end
  end

  defp normalize_committer_opts(opts) when is_list(opts), do: {:ok, opts}
  defp normalize_committer_opts(nil), do: {:ok, []}
  defp normalize_committer_opts(other), do: {:error, {:invalid_committer_opts, other}}

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

  defp write_batch_with_poison_policy(context, batch) do
    case Writer.write_batch(context.writer, %{rows: batch.rows}) do
      {:ok, write_result} ->
        {:ok, write_result, batch}

      {:error, reason} = original_error ->
        maybe_recover_poison_rows(context, batch, reason, original_error)
    end
  end

  defp maybe_recover_poison_rows(context, batch, reason, fallback_error) do
    case Map.get(context, :poison_row_mode, :fail) do
      :drop ->
        recover_batch_without_poison_rows(context, batch, reason)

      _ ->
        fallback_error
    end
  end

  defp recover_batch_without_poison_rows(context, batch, batch_error) do
    {valid_rows, dropped_rows} = isolate_rows(context, batch.rows, batch_error)

    case valid_rows do
      [] ->
        {:error, {:all_rows_poisoned, batch_error, dropped_rows}}

      rows ->
        recovered_batch = %{
          batch
          | rows: rows,
            row_count: length(rows)
        }

        case Writer.write_batch(context.writer, %{rows: recovered_batch.rows}) do
          {:ok, write_result} -> {:ok, write_result, recovered_batch}
          {:error, reason} -> {:error, {:poison_recovery_failed, reason, dropped_rows}}
        end
    end
  end

  defp isolate_rows(context, rows, batch_error) when is_list(rows) do
    rows
    |> Enum.with_index(1)
    |> Enum.reduce({[], []}, fn {row, index}, {valid_acc, dropped_acc} ->
      case Writer.write_batch(context.writer, %{rows: [row]}) do
        {:ok, write_result} ->
          _ = Writer.cleanup(context.writer, write_result)
          {[row | valid_acc], dropped_acc}

        {:error, reason} ->
          dropped = %{row: row, index: index, reason: reason, batch_error: batch_error}
          emit_poison_row(context, dropped)
          {valid_acc, [dropped | dropped_acc]}
      end
    end)
    |> then(fn {valid, dropped} -> {Enum.reverse(valid), Enum.reverse(dropped)} end)
  end

  defp emit_poison_row(context, dropped) when is_map(dropped) do
    DuckFeeder.Telemetry.execute(
      [:batch, :poison_row],
      %{count: 1},
      %{
        index: dropped.index,
        reason: dropped.reason,
        batch_error: dropped.batch_error
      }
    )

    case Map.get(context, :poison_row_sink) do
      pid when is_pid(pid) ->
        send(pid, {:duck_feeder_poison_row, dropped})

      fun when is_function(fun, 1) ->
        _ = safe_poison_sink(fun, dropped)

      {module, function, args} when is_atom(module) and is_atom(function) and is_list(args) ->
        _ = safe_poison_sink(fn payload -> apply(module, function, [payload | args]) end, dropped)

      _ ->
        :ok
    end

    :ok
  end

  defp safe_poison_sink(fun, payload) when is_function(fun, 1) do
    fun.(payload)
    :ok
  rescue
    _ -> :ok
  catch
    _, _ -> :ok
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
