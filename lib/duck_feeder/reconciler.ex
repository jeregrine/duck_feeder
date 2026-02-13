defmodule DuckFeeder.Reconciler do
  @moduledoc """
  Reconciliation helpers for stale batches.

  Current behavior:
  - retries `uploaded` stale batches via `commit_uploaded_batch/2`
  - optional failed-batch retry path (`cleanup_failed_uploads?: true`) that:
    - deletes known batch objects from storage
    - transitions batch from `failed` back to `pending`
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

    with {:ok, stale_batches} <-
           meta.list_stale_batches(conn, stale_before: stale_before, states: states) do
      summary =
        Enum.reduce(
          stale_batches,
          %{checked: 0, committed: [], retried: [], skipped: [], errors: []},
          fn batch, acc ->
            acc = %{acc | checked: acc.checked + 1}

            case reconcile_batch(meta, conn, context, opts, batch) do
              {:committed, batch_id} ->
                %{acc | committed: [batch_id | acc.committed]}

              {:retried, batch_id} ->
                %{acc | retried: [batch_id | acc.retried]}

              {:skipped, batch_id} ->
                %{acc | skipped: [batch_id | acc.skipped]}

              {:error, batch_id, reason} ->
                %{acc | errors: [{batch_id, reason} | acc.errors]}
            end
          end
        )
        |> finalize_summary()

      {:ok, summary}
    end
  end

  defp reconcile_batch(meta, conn, _context, _opts, %{batch_id: batch_id, state: "uploaded"}) do
    case meta.commit_uploaded_batch(conn, batch_id) do
      {:ok, _result} -> {:committed, batch_id}
      {:error, reason} -> {:error, batch_id, reason}
    end
  end

  defp reconcile_batch(meta, conn, context, opts, %{batch_id: batch_id, state: "failed"}) do
    if Keyword.get(opts, :cleanup_failed_uploads?, false) do
      case cleanup_failed_batch(meta, conn, context, batch_id) do
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

  defp cleanup_failed_batch(meta, conn, context, batch_id) do
    with {:ok, files} <- meta.list_batch_files(conn, batch_id),
         :ok <- delete_batch_files(context, files),
         {:ok, %{to: :pending}} <-
           meta.transition_batch(conn, batch_id, :pending, error_message: nil) do
      :ok
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
