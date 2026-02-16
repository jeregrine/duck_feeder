defmodule DuckFeeder.TempFileReaper do
  @moduledoc """
  Best-effort cleanup for stale DuckFeeder writer temp files.

  This is intentionally conservative and fail-open. It can be called opportunistically
  from writers to reduce crash-leftover temp files.
  """

  @table :duck_feeder_temp_file_reaper_last_run

  @default_prefix "duck_feeder_"
  @default_suffixes [".jsonl", ".parquet"]
  @default_stale_after_seconds 24 * 60 * 60
  @default_min_interval_ms 60_000
  @default_max_files 500

  @type summary :: %{
          checked: non_neg_integer(),
          deleted: non_neg_integer(),
          errors: [{String.t(), term()}]
        }

  @spec maybe_reap(map() | keyword(), keyword()) :: :ok | {:error, term()}
  def maybe_reap(config_or_opts, opts \\ []) do
    with {:ok, config} <- normalize_config(config_or_opts, opts) do
      cond do
        not config.enabled? ->
          :ok

        due_reap?(config) ->
          mark_run(config)

          case reap(
                 tmp_dir: config.tmp_dir,
                 prefix: config.prefix,
                 suffixes: config.suffixes,
                 stale_after_seconds: config.stale_after_seconds,
                 max_files: config.max_files,
                 now_posix_seconds: config.now_posix_seconds
               ) do
            {:ok, _summary} -> :ok
            {:error, reason} -> {:error, reason}
          end

        true ->
          :ok
      end
    end
  end

  @spec reap(keyword()) :: {:ok, summary()} | {:error, term()}
  def reap(opts \\ []) when is_list(opts) do
    with {:ok, tmp_dir} <- normalize_tmp_dir(Keyword.get(opts, :tmp_dir, System.tmp_dir!())),
         {:ok, prefix} <- normalize_prefix(Keyword.get(opts, :prefix, @default_prefix)),
         {:ok, suffixes} <- normalize_suffixes(Keyword.get(opts, :suffixes, @default_suffixes)),
         {:ok, stale_after_seconds} <-
           normalize_non_neg_integer(
             Keyword.get(opts, :stale_after_seconds, @default_stale_after_seconds),
             :stale_after_seconds
           ),
         {:ok, max_files} <-
           normalize_positive_integer(
             Keyword.get(opts, :max_files, @default_max_files),
             :max_files
           ),
         {:ok, now_posix_seconds} <-
           normalize_non_neg_integer(
             Keyword.get(opts, :now_posix_seconds, System.os_time(:second)),
             :now_posix_seconds
           ),
         {:ok, entries} <- list_tmp_dir(tmp_dir) do
      cutoff = now_posix_seconds - stale_after_seconds

      {checked, deleted, errors} =
        entries
        |> Enum.filter(&eligible_filename?(&1, prefix, suffixes))
        |> Enum.map(&Path.join(tmp_dir, &1))
        |> Enum.take(max_files)
        |> Enum.reduce({0, 0, []}, fn path, {checked_acc, deleted_acc, errors_acc} ->
          case stale_regular_file?(path, cutoff) do
            {:ok, true} ->
              case File.rm(path) do
                :ok -> {checked_acc + 1, deleted_acc + 1, errors_acc}
                {:error, :enoent} -> {checked_acc + 1, deleted_acc, errors_acc}
                {:error, reason} -> {checked_acc + 1, deleted_acc, [{path, reason} | errors_acc]}
              end

            {:ok, false} ->
              {checked_acc + 1, deleted_acc, errors_acc}

            {:error, reason} ->
              {checked_acc + 1, deleted_acc, [{path, reason} | errors_acc]}
          end
        end)

      {:ok, %{checked: checked, deleted: deleted, errors: Enum.reverse(errors)}}
    end
  end

  defp normalize_config(config_or_opts, opts) do
    adapter_opts =
      case config_or_opts do
        %{} = map -> map |> Map.get(:adapter_opts, %{}) |> Map.new()
        list when is_list(list) -> list |> Keyword.get(:adapter_opts, %{}) |> Map.new()
        _ -> %{}
      end

    enabled? = Keyword.get(opts, :enabled?, Map.get(adapter_opts, :reap_stale_tmp_files?, true))

    with {:ok, tmp_dir} <-
           normalize_tmp_dir(
             Keyword.get(opts, :tmp_dir, Map.get(adapter_opts, :tmp_dir, System.tmp_dir!()))
           ),
         {:ok, prefix} <-
           normalize_prefix(
             Keyword.get(opts, :prefix, Map.get(adapter_opts, :tmp_file_prefix, @default_prefix))
           ),
         {:ok, suffixes} <-
           normalize_suffixes(
             Keyword.get(
               opts,
               :suffixes,
               Map.get(adapter_opts, :tmp_file_suffixes, @default_suffixes)
             )
           ),
         {:ok, stale_after_seconds} <-
           normalize_non_neg_integer(
             Keyword.get(
               opts,
               :stale_after_seconds,
               Map.get(adapter_opts, :tmp_file_stale_after_seconds, @default_stale_after_seconds)
             ),
             :stale_after_seconds
           ),
         {:ok, min_interval_ms} <-
           normalize_non_neg_integer(
             Keyword.get(
               opts,
               :min_interval_ms,
               Map.get(adapter_opts, :tmp_file_reap_interval_ms, @default_min_interval_ms)
             ),
             :min_interval_ms
           ),
         {:ok, max_files} <-
           normalize_positive_integer(
             Keyword.get(
               opts,
               :max_files,
               Map.get(adapter_opts, :tmp_file_reap_max_files, @default_max_files)
             ),
             :max_files
           ),
         {:ok, now_mono_ms} <-
           normalize_non_neg_integer(
             Keyword.get(opts, :now_mono_ms, System.monotonic_time(:millisecond)),
             :now_mono_ms
           ),
         {:ok, now_posix_seconds} <-
           normalize_non_neg_integer(
             Keyword.get(opts, :now_posix_seconds, System.os_time(:second)),
             :now_posix_seconds
           ) do
      {:ok,
       %{
         enabled?: enabled? in [true, 1, "true", "1"],
         tmp_dir: tmp_dir,
         prefix: prefix,
         suffixes: suffixes,
         stale_after_seconds: stale_after_seconds,
         min_interval_ms: min_interval_ms,
         max_files: max_files,
         now_mono_ms: now_mono_ms,
         now_posix_seconds: now_posix_seconds
       }}
    end
  end

  defp due_reap?(config) do
    ensure_table!()

    key = reaper_key(config)

    case :ets.lookup(@table, key) do
      [{^key, last_mono_ms}] when is_integer(last_mono_ms) ->
        config.now_mono_ms - last_mono_ms >= config.min_interval_ms

      _ ->
        true
    end
  end

  defp mark_run(config) do
    ensure_table!()
    :ets.insert(@table, {reaper_key(config), config.now_mono_ms})
    :ok
  end

  defp reaper_key(config) do
    {config.tmp_dir, config.prefix, Enum.sort(config.suffixes)}
  end

  defp ensure_table! do
    case :ets.whereis(@table) do
      :undefined ->
        try do
          :ets.new(@table, [:named_table, :public, :set])
        rescue
          ArgumentError -> :ok
        end

      _tid ->
        :ok
    end

    :ok
  end

  defp list_tmp_dir(tmp_dir) do
    case File.ls(tmp_dir) do
      {:ok, entries} -> {:ok, entries}
      {:error, reason} -> {:error, {:tmp_dir_list_failed, tmp_dir, reason}}
    end
  end

  defp eligible_filename?(filename, prefix, suffixes)
       when is_binary(filename) and is_binary(prefix) and is_list(suffixes) do
    String.starts_with?(filename, prefix) and
      Enum.any?(suffixes, fn suffix -> String.ends_with?(filename, suffix) end)
  end

  defp stale_regular_file?(path, cutoff_posix_seconds) do
    case File.stat(path, time: :posix) do
      {:ok, %File.Stat{type: :regular, mtime: mtime}} when is_integer(mtime) ->
        {:ok, mtime <= cutoff_posix_seconds}

      {:ok, %File.Stat{}} ->
        {:ok, false}

      {:error, :enoent} ->
        {:ok, false}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp normalize_tmp_dir(value) when is_binary(value) and value != "", do: {:ok, value}
  defp normalize_tmp_dir(value), do: {:error, {:invalid_tmp_dir, value}}

  defp normalize_prefix(value) when is_binary(value) and value != "", do: {:ok, value}
  defp normalize_prefix(value), do: {:error, {:invalid_prefix, value}}

  defp normalize_suffixes(value) when is_list(value) do
    suffixes =
      value
      |> Enum.filter(&(is_binary(&1) and &1 != ""))
      |> Enum.uniq()

    if suffixes == [] do
      {:error, {:invalid_suffixes, value}}
    else
      {:ok, suffixes}
    end
  end

  defp normalize_suffixes(value), do: {:error, {:invalid_suffixes, value}}

  defp normalize_positive_integer(value, _key) when is_integer(value) and value > 0,
    do: {:ok, value}

  defp normalize_positive_integer(value, key), do: {:error, {:invalid_option, key, value}}

  defp normalize_non_neg_integer(value, _key) when is_integer(value) and value >= 0,
    do: {:ok, value}

  defp normalize_non_neg_integer(value, key), do: {:error, {:invalid_option, key, value}}
end
