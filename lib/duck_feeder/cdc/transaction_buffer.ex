defmodule DuckFeeder.CDC.TransactionBuffer do
  @moduledoc """
  Stateful transaction-aware CDC buffer.

  Consumes normalized events and returns a committed transaction payload when
  commit is observed.
  """

  alias DuckFeeder.CDC.Event

  defmodule State do
    @enforce_keys [:max_changes]
    defstruct relations: %{}, current_txn: nil, max_changes: nil

    @type t :: %__MODULE__{
            relations: %{optional(integer()) => Event.Relation.t()},
            current_txn: map() | nil,
            max_changes: pos_integer() | nil
          }
  end

  @type transaction :: %{
          xid: non_neg_integer(),
          begin_lsn: String.t(),
          end_lsn: String.t(),
          begin_timestamp: DateTime.t() | nil,
          commit_timestamp: DateTime.t() | nil,
          changes: [map()],
          change_count: non_neg_integer()
        }

  @spec new(keyword()) :: State.t()
  def new(opts \\ []) do
    %State{max_changes: Keyword.get(opts, :max_changes)}
  end

  @spec in_transaction?(State.t()) :: boolean()
  def in_transaction?(%State{current_txn: txn}), do: not is_nil(txn)

  @doc """
  Handles one CDC event.

  Returns:
  - `{:buffering, state}` when no commit is emitted
  - `{:ok, transaction, state}` when a transaction commits
  - `{:error, reason}` for invalid event sequence/state
  """
  @spec handle_event(State.t(), Event.t()) ::
          {:buffering, State.t()} | {:ok, transaction(), State.t()} | {:error, term()}
  def handle_event(%State{} = state, %Event.Relation{} = relation) do
    {:buffering, put_in(state.relations[relation.id], relation)}
  end

  def handle_event(%State{current_txn: nil} = state, %Event.Begin{} = begin_event) do
    txn = %{
      xid: begin_event.xid,
      begin_lsn: begin_event.final_lsn,
      begin_timestamp: begin_event.timestamp,
      changes: [],
      change_count: 0
    }

    {:buffering, %{state | current_txn: txn}}
  end

  def handle_event(%State{current_txn: _txn}, %Event.Begin{} = begin_event) do
    {:error, {:unexpected_begin, begin_event.xid}}
  end

  def handle_event(%State{} = state, %Event.Insert{} = insert_event) do
    with {:ok, relation} <- fetch_relation(state, insert_event.relation_id),
         {:ok, state} <- append_change(state, insert_change(relation, insert_event)) do
      {:buffering, state}
    end
  end

  def handle_event(%State{} = state, %Event.Update{} = update_event) do
    with {:ok, relation} <- fetch_relation(state, update_event.relation_id),
         {:ok, state} <- append_change(state, update_change(relation, update_event)) do
      {:buffering, state}
    end
  end

  def handle_event(%State{} = state, %Event.Delete{} = delete_event) do
    with {:ok, relation} <- fetch_relation(state, delete_event.relation_id),
         {:ok, state} <- append_change(state, delete_change(relation, delete_event)) do
      {:buffering, state}
    end
  end

  def handle_event(%State{} = state, %Event.Truncate{} = truncate_event) do
    Enum.reduce_while(truncate_event.relation_ids, {:ok, state}, fn relation_id,
                                                                    {:ok, acc_state} ->
      with {:ok, relation} <- fetch_relation(acc_state, relation_id),
           {:ok, next_state} <- append_change(acc_state, truncate_change(relation)) do
        {:cont, {:ok, next_state}}
      else
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, next_state} -> {:buffering, next_state}
      {:error, _reason} = error -> error
    end
  end

  def handle_event(%State{current_txn: nil}, %Event.Commit{} = commit_event) do
    {:error, {:unexpected_commit, commit_event.xid}}
  end

  def handle_event(%State{current_txn: txn} = state, %Event.Commit{} = commit_event) do
    if txn.xid != commit_event.xid do
      {:error, {:xid_mismatch, txn.xid, commit_event.xid}}
    else
      transaction = %{
        xid: txn.xid,
        begin_lsn: txn.begin_lsn,
        end_lsn: commit_event.end_lsn,
        begin_timestamp: txn.begin_timestamp,
        commit_timestamp: commit_event.timestamp,
        changes: Enum.reverse(txn.changes),
        change_count: txn.change_count
      }

      {:ok, transaction, %{state | current_txn: nil}}
    end
  end

  defp fetch_relation(%State{relations: relations}, relation_id) do
    case Map.get(relations, relation_id) do
      nil -> {:error, {:unknown_relation, relation_id}}
      relation -> {:ok, relation}
    end
  end

  defp append_change(%State{current_txn: nil}, _change), do: {:error, :change_outside_transaction}

  defp append_change(%State{current_txn: txn, max_changes: max_changes} = state, change) do
    change_count = txn.change_count + 1

    cond do
      is_integer(max_changes) and max_changes > 0 and change_count > max_changes ->
        {:error, {:exceeded_max_changes, max_changes}}

      true ->
        {:ok,
         %{
           state
           | current_txn: %{txn | changes: [change | txn.changes], change_count: change_count}
         }}
    end
  end

  defp insert_change(relation, insert_event) do
    %{
      op: :insert,
      relation_id: relation.id,
      relation: {relation.schema, relation.table},
      record: insert_event.record
    }
  end

  defp update_change(relation, update_event) do
    %{
      op: :update,
      relation_id: relation.id,
      relation: {relation.schema, relation.table},
      old_record: update_event.old_record,
      record: update_event.record
    }
  end

  defp delete_change(relation, delete_event) do
    %{
      op: :delete,
      relation_id: relation.id,
      relation: {relation.schema, relation.table},
      old_record: delete_event.old_record
    }
  end

  defp truncate_change(relation) do
    %{
      op: :truncate,
      relation_id: relation.id,
      relation: {relation.schema, relation.table}
    }
  end
end
