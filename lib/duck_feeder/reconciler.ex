defmodule DuckFeeder.Reconciler do
  @moduledoc """
  Reconciliation helpers for stale batches.

  Current behavior:
  - retries `uploaded` stale batches via `commit_uploaded_batch/2`
  - keeps `failed` stale batches as-is for operator-driven retry policies
  """

  alias DuckFeeder.Meta

  @type context :: %{
          required(:meta_conn) => term(),
          optional(:meta_module) => module()
        }

  @type summary :: %{
          checked: non_neg_integer(),
          committed: [String.t()],
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
          %{checked: 0, committed: [], skipped: [], errors: []},
          fn batch, acc ->
            acc = %{acc | checked: acc.checked + 1}

            case reconcile_batch(meta, conn, batch) do
              {:committed, batch_id} ->
                %{acc | committed: [batch_id | acc.committed]}

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

  defp reconcile_batch(meta, conn, %{batch_id: batch_id, state: "uploaded"}) do
    case meta.commit_uploaded_batch(conn, batch_id) do
      {:ok, _result} -> {:committed, batch_id}
      {:error, reason} -> {:error, batch_id, reason}
    end
  end

  defp reconcile_batch(_meta, _conn, %{batch_id: batch_id, state: "failed"}) do
    {:skipped, batch_id}
  end

  defp reconcile_batch(_meta, _conn, %{batch_id: batch_id, state: state}) do
    {:skipped, "#{batch_id}:#{state}"}
  end

  defp finalize_summary(summary) do
    %{
      checked: summary.checked,
      committed: Enum.reverse(summary.committed),
      skipped: Enum.reverse(summary.skipped),
      errors: Enum.reverse(summary.errors)
    }
  end
end
