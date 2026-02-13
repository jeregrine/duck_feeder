defmodule DuckFeeder.ReconcilerTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.Reconciler

  defmodule FakeMeta do
    def list_stale_batches(_conn, _opts) do
      {:ok,
       [
         %{batch_id: "b1", state: "uploaded"},
         %{batch_id: "b2", state: "failed"},
         %{batch_id: "b3", state: "uploaded"}
       ]}
    end

    def commit_uploaded_batch(_conn, "b1"), do: {:ok, %{batch_id: "b1"}}
    def commit_uploaded_batch(_conn, "b3"), do: {:error, :commit_failed}
  end

  test "reconciles stale batches" do
    assert {:ok, summary} =
             Reconciler.reconcile(%{meta_conn: :fake, meta_module: FakeMeta})

    assert summary.checked == 3
    assert summary.committed == ["b1"]
    assert summary.skipped == ["b2"]
    assert summary.errors == [{"b3", :commit_failed}]
  end
end
