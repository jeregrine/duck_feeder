defmodule DuckFeeder.Reconciler do
  @moduledoc """
  Reconciliation helpers for stale batches.

  Current behavior:
  - retries `uploaded` stale batches via `commit_uploaded_batch/2`
  - optional failed-batch retry path (`cleanup_failed_uploads?: true`) that:
    - deletes known batch objects from storage
    - transitions batch from `failed` back to `pending`
  - optional orphan detection during failed cleanup:
    - `require_failed_batch_files?: true` returns an error when a failed batch has no known files
  - optional run safety controls:
    - `max_batches: positive_integer()` to cap work per reconcile call
    - `stop_on_error?: true` to halt after the first batch error
  """

  alias DuckFeeder.{Meta, Storage}

  @type context :: %{
          required(:meta_conn) => term(),
          optional(:meta_module) => module(),
          optional(:storage) => map(),
          optional(:storage_module) => module()
        }

  @type summary :: %{
          checked: non_neg_integer(),
          committed: [String.t()],
          retried: [String.t()],
          skipped: [String.t()],
          errors: [{String.t(), term()}]
        }

  @spec reconcile(context(), keyword()) :: {:ok, summary()} | {:error, term()}
  def reconcile(context, opts \\ []) when is_map(context) do
    meta = Map.get(context, :meta_module, Meta)
    conn = Map.fetch!(context, :meta_conn)

    stale_before =
      Keyword.get(opts, :stale_before, DateTime.add(DateTime.utc_now(), -30 * 60, :second))

    states = Keyword.get(opts, :states, [:uploaded, :failed])
    stop_on_error? = Keyword.get(opts, :stop_on_error?, false)

    with {:ok, max_batches} <- normalize_max_batches(Keyword.get(opts, :max_batches)),
         {:ok, stale_batches} <-
           meta.list_stale_batches(
             conn,
             stale_before: stale_before,
             states: states,
             designated_table_id: Keyword.get(opts, :designated_table_id)
           ) do
      summary =
        stale_batches
        |> maybe_limit(max_batches)
        |> Enum.reduce_while(
          %{checked: 0, committed: [], retried: [], skipped: [], errors: []},
          fn batch, acc ->
            acc = %{acc | checked: acc.checked + 1}

            case reconcile_batch(meta, conn, context, opts, batch) do
              {:committed, batch_id} ->
                {:cont, %{acc | committed: [batch_id | acc.committed]}}

              {:retried, batch_id} ->
                {:cont, %{acc | retried: [batch_id | acc.retried]}}

              {:skipped, batch_id} ->
                {:cont, %{acc | skipped: [batch_id | acc.skipped]}}

              {:error, batch_id, reason} ->
                next_acc = %{acc | errors: [{batch_id, reason} | acc.errors]}

                if stop_on_error? do
                  {:halt, next_acc}
                else
                  {:cont, next_acc}
                end
            end
          end
        )
        |> finalize_summary()

      {:ok, summary}
    end
  end

  defp reconcile_batch(meta, conn, context, opts, %{batch_id: batch_id, state: "uploaded"}) do
    with :ok <- maybe_verify_uploaded_batch(meta, conn, context, opts, batch_id),
         {:ok, _result} <- meta.commit_uploaded_batch(conn, batch_id) do
      {:committed, batch_id}
    else
      {:error, reason} -> {:error, batch_id, reason}
    end
  end

  defp reconcile_batch(meta, conn, context, opts, %{batch_id: batch_id, state: "failed"}) do
    if Keyword.get(opts, :cleanup_failed_uploads?, false) do
      case cleanup_failed_batch(meta, conn, context, batch_id, opts) do
        :ok -> {:retried, batch_id}
        {:error, reason} -> {:error, batch_id, reason}
      end
    else
      {:skipped, batch_id}
    end
  end

  defp reconcile_batch(_meta, _conn, _context, _opts, %{batch_id: batch_id, state: state}) do
    {:skipped, "#{batch_id}:#{state}"}
  end

  defp cleanup_failed_batch(meta, conn, context, batch_id, opts) do
    with {:ok, files} <- meta.list_batch_files(conn, batch_id),
         :ok <- maybe_require_failed_batch_files(files, batch_id, opts),
         :ok <- delete_batch_files(context, files),
         {:ok, %{to: :pending}} <-
           meta.transition_batch(conn, batch_id, :pending, error_message: nil) do
      :ok
    end
  end

  defp maybe_verify_uploaded_batch(meta, conn, context, opts, batch_id) do
    if Keyword.get(opts, :verify_uploaded_objects?, false) do
      with {:ok, files} <- meta.list_batch_files(conn, batch_id),
           :ok <- ensure_batch_files_present(files, batch_id),
           :ok <- verify_batch_files(context, files, batch_id) do
        :ok
      end
    else
      :ok
    end
  end

  defp ensure_batch_files_present([], batch_id),
    do: {:error, {:missing_batch_files, batch_id}}

  defp ensure_batch_files_present(files, _batch_id) when is_list(files), do: :ok

  defp maybe_require_failed_batch_files(files, batch_id, opts) when is_list(files) do
    if Keyword.get(opts, :require_failed_batch_files?, false) do
      ensure_batch_files_present(files, batch_id)
    else
      :ok
    end
  end

  defp verify_batch_files(context, files, batch_id) when is_list(files) do
    storage = Map.get(context, :storage)
    storage_module = Map.get(context, :storage_module, Storage)
    meta = Map.get(context, :meta_module, Meta)
    conn = Map.fetch!(context, :meta_conn)

    if is_map(storage) do
      Enum.reduce_while(files, :ok, fn file, :ok ->
        case storage_module.head_object(storage, file.object_key) do
          {:ok, _meta} ->
            {:cont, :ok}

          {:error, reason} ->
            error = {:missing_uploaded_object, file.object_key, reason}
            _ = meta.transition_batch(conn, batch_id, :failed, error_message: inspect(error))
            {:halt, {:error, error}}
        end
      end)
    else
      {:error, :missing_storage_for_uploaded_verification}
    end
  end

  defp delete_batch_files(context, files) when is_list(files) do
    storage = Map.get(context, :storage)
    storage_module = Map.get(context, :storage_module, Storage)

    if is_map(storage) do
      files
      |> Enum.reduce_while(:ok, fn file, :ok ->
        case storage_module.delete_object(storage, file.object_key) do
          :ok -> {:cont, :ok}
          {:error, reason} -> {:halt, {:error, {:delete_failed, file.object_key, reason}}}
        end
      end)
    else
      {:error, :missing_storage_for_failed_cleanup}
    end
  end

  defp maybe_limit(batches, nil), do: batches
  defp maybe_limit(batches, max_batches), do: Enum.take(batches, max_batches)

  defp normalize_max_batches(nil), do: {:ok, nil}

  defp normalize_max_batches(max_batches) when is_integer(max_batches) and max_batches > 0,
    do: {:ok, max_batches}

  defp normalize_max_batches(other), do: {:error, {:invalid_max_batches, other}}

  defp finalize_summary(summary) do
    %{
      checked: summary.checked,
      committed: Enum.reverse(summary.committed),
      retried: Enum.reverse(summary.retried),
      skipped: Enum.reverse(summary.skipped),
      errors: Enum.reverse(summary.errors)
    }
  end
end
