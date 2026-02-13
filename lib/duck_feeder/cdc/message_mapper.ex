defmodule DuckFeeder.CDC.MessageMapper do
  @moduledoc """
  Maps generic decoded replication messages into `DuckFeeder.CDC.Event` structs.

  This keeps wire-format decoding separate from internal event processing.
  """

  alias DuckFeeder.CDC.Event

  @spec map_message(map() | tuple()) :: {:ok, Event.t()} | {:ignore, term()} | {:error, term()}
  def map_message(%{type: :begin} = msg) do
    with {:ok, xid} <- fetch_int(msg, :xid),
         {:ok, final_lsn} <- fetch_str(msg, :final_lsn) do
      {:ok, %Event.Begin{xid: xid, final_lsn: final_lsn, timestamp: msg[:timestamp]}}
    end
  end

  def map_message(%{type: :commit} = msg) do
    with {:ok, xid} <- fetch_int(msg, :xid),
         {:ok, end_lsn} <- fetch_str(msg, :end_lsn) do
      {:ok, %Event.Commit{xid: xid, end_lsn: end_lsn, timestamp: msg[:timestamp]}}
    end
  end

  def map_message(%{type: :relation} = msg) do
    with {:ok, id} <- fetch_int(msg, :id),
         {:ok, schema} <- fetch_str(msg, :schema),
         {:ok, table} <- fetch_str(msg, :table) do
      {:ok,
       %Event.Relation{
         id: id,
         schema: schema,
         table: table,
         columns: Map.get(msg, :columns, [])
       }}
    end
  end

  def map_message(%{type: :insert} = msg) do
    with {:ok, relation_id} <- fetch_int(msg, :relation_id),
         {:ok, record} <- fetch_map(msg, :record) do
      {:ok, %Event.Insert{relation_id: relation_id, record: record}}
    end
  end

  def map_message(%{type: :update} = msg) do
    with {:ok, relation_id} <- fetch_int(msg, :relation_id),
         {:ok, record} <- fetch_map(msg, :record),
         {:ok, old_record} <- fetch_map(msg, :old_record) do
      {:ok, %Event.Update{relation_id: relation_id, record: record, old_record: old_record}}
    end
  end

  def map_message(%{type: :delete} = msg) do
    with {:ok, relation_id} <- fetch_int(msg, :relation_id),
         {:ok, old_record} <- fetch_map(msg, :old_record) do
      {:ok, %Event.Delete{relation_id: relation_id, old_record: old_record}}
    end
  end

  def map_message(%{type: :truncate} = msg) do
    relation_ids = Map.get(msg, :relation_ids)

    if is_list(relation_ids) and Enum.all?(relation_ids, &is_integer/1) do
      {:ok, %Event.Truncate{relation_ids: relation_ids}}
    else
      {:error, {:invalid_field, :relation_ids, relation_ids}}
    end
  end

  def map_message(%{type: :keepalive} = msg), do: {:ignore, msg}
  def map_message(%{type: :origin} = msg), do: {:ignore, msg}

  def map_message({type, payload}) when is_atom(type) and is_map(payload) do
    map_message(Map.put(payload, :type, type))
  end

  def map_message(other), do: {:error, {:unsupported_message, other}}

  defp fetch_int(map, key) do
    case Map.get(map, key) do
      value when is_integer(value) -> {:ok, value}
      other -> {:error, {:invalid_field, key, other}}
    end
  end

  defp fetch_str(map, key) do
    case Map.get(map, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      other -> {:error, {:invalid_field, key, other}}
    end
  end

  defp fetch_map(map, key) do
    case Map.get(map, key) do
      value when is_map(value) -> {:ok, value}
      other -> {:error, {:invalid_field, key, other}}
    end
  end
end
