defmodule DuckFeeder.CDC.LogicalReplication.Converter do
  @moduledoc """
  Converts pgoutput wire messages into normalized `DuckFeeder.CDC.Event` structs.
  """

  alias DuckFeeder.CDC.{Event, Lsn}
  alias DuckFeeder.CDC.LogicalReplication.Messages

  defmodule State do
    @enforce_keys [:relations]
    defstruct relations: %{}, current_xid: nil

    @type t :: %__MODULE__{
            relations: %{optional(non_neg_integer()) => Messages.Relation.t()},
            current_xid: non_neg_integer() | nil
          }
  end

  @spec new() :: State.t()
  def new, do: %State{relations: %{}}

  @spec convert(Messages.message(), State.t()) ::
          {:ok, Event.t(), State.t()} | {:ignore, State.t()} | {:error, term()}
  def convert(%Messages.Begin{} = begin, %State{} = state) do
    event =
      %Event.Begin{
        xid: begin.xid,
        final_lsn: Lsn.to_string(begin.final_lsn),
        timestamp: begin.commit_timestamp
      }

    {:ok, event, %{state | current_xid: begin.xid}}
  end

  def convert(%Messages.Commit{} = commit, %State{} = state) do
    case state.current_xid do
      nil ->
        {:error, :commit_without_begin}

      xid ->
        event =
          %Event.Commit{
            xid: xid,
            end_lsn: Lsn.to_string(commit.end_lsn),
            timestamp: commit.commit_timestamp
          }

        {:ok, event, %{state | current_xid: nil}}
    end
  end

  def convert(%Messages.Relation{} = relation, %State{} = state) do
    event =
      %Event.Relation{
        id: relation.id,
        schema: relation.namespace,
        table: relation.name,
        columns:
          Enum.map(relation.columns, fn col ->
            %{
              name: col.name,
              flags: col.flags,
              type_oid: col.type_oid,
              type_modifier: col.type_modifier
            }
          end)
      }

    {:ok, event, put_in(state.relations[relation.id], relation)}
  end

  def convert(%Messages.Insert{} = insert, %State{} = state) do
    with {:ok, relation} <- fetch_relation(state, insert.relation_id),
         {:ok, record} <- tuple_to_map(relation.columns, insert.tuple_data) do
      {:ok, %Event.Insert{relation_id: insert.relation_id, record: record}, state}
    end
  end

  def convert(%Messages.Update{} = update, %State{} = state) do
    with {:ok, relation} <- fetch_relation(state, update.relation_id),
         {:ok, old_tuple_data} <- old_tuple(update, relation),
         {:ok, old_record} <- tuple_to_map(relation.columns, old_tuple_data),
         {:ok, record} <-
           tuple_to_map(relation.columns, update.tuple_data, fn
             column_name, :unchanged_toast -> Map.get(old_record, column_name)
             _column_name, value -> value
           end) do
      {:ok,
       %Event.Update{relation_id: update.relation_id, old_record: old_record, record: record},
       state}
    end
  end

  def convert(%Messages.Delete{} = delete, %State{} = state) do
    with {:ok, relation} <- fetch_relation(state, delete.relation_id),
         tuple <- delete.old_tuple_data || delete.changed_key_tuple_data || [],
         {:ok, old_record} <- tuple_to_map(relation.columns, tuple) do
      {:ok, %Event.Delete{relation_id: delete.relation_id, old_record: old_record}, state}
    end
  end

  def convert(%Messages.Truncate{} = truncate, %State{} = state) do
    {:ok, %Event.Truncate{relation_ids: truncate.truncated_relations}, state}
  end

  def convert(%Messages.Origin{}, %State{} = state), do: {:ignore, state}
  def convert(%Messages.Type{}, %State{} = state), do: {:ignore, state}
  def convert(%Messages.Message{}, %State{} = state), do: {:ignore, state}
  def convert(%Messages.Unsupported{}, %State{} = state), do: {:ignore, state}

  defp old_tuple(%Messages.Update{old_tuple_data: old_tuple}, _relation) when is_list(old_tuple),
    do: {:ok, old_tuple}

  defp old_tuple(%Messages.Update{changed_key_tuple_data: old_tuple}, _relation)
       when is_list(old_tuple),
       do: {:ok, old_tuple}

  defp old_tuple(%Messages.Update{}, relation) do
    {:error, {:replica_identity_not_full, {relation.namespace, relation.name}}}
  end

  defp fetch_relation(%State{relations: relations}, relation_id) do
    case Map.get(relations, relation_id) do
      nil -> {:error, {:unknown_relation, relation_id}}
      relation -> {:ok, relation}
    end
  end

  defp tuple_to_map(columns, values, mapper \\ fn _name, value -> value end)

  defp tuple_to_map(columns, values, mapper) when is_list(columns) and is_list(values) do
    if length(values) == length(columns) do
      row =
        columns
        |> Enum.zip(values)
        |> Map.new(fn {%{name: name}, value} ->
          {name, mapper.(name, value)}
        end)

      {:ok, row}
    else
      {:error, {:column_value_count_mismatch, length(columns), length(values)}}
    end
  end
end
