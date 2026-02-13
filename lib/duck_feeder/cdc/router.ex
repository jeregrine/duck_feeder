defmodule DuckFeeder.CDC.Router do
  @moduledoc """
  Routes committed transaction changes to designated tables.
  """

  @type designated_table :: %{
          required(:source_schema) => String.t(),
          required(:source_table) => String.t(),
          required(:target_schema) => String.t(),
          required(:target_table) => String.t(),
          optional(:id) => integer(),
          optional(:mode) => String.t()
        }

  @type route_key :: {String.t(), String.t()}

  @spec route_transaction(map(), [designated_table()]) :: %{
          xid: non_neg_integer(),
          begin_lsn: String.t(),
          end_lsn: String.t(),
          routes: %{optional(route_key()) => [map()]}
        }
  def route_transaction(transaction, designated_tables)
      when is_map(transaction) and is_list(designated_tables) do
    mapping = build_mapping(designated_tables)

    routes =
      transaction
      |> Map.get(:changes, [])
      |> Enum.reduce(%{}, fn change, acc ->
        case Map.get(mapping, change[:relation]) do
          nil ->
            acc

          designated ->
            target = designated.target

            routed_change =
              change
              |> Map.put(:designated_table_id, designated.id)
              |> Map.put(:target_relation, target)

            Map.update(acc, target, [routed_change], &[routed_change | &1])
        end
      end)
      |> Enum.map(fn {target, changes} -> {target, Enum.reverse(changes)} end)
      |> Map.new()

    %{
      xid: transaction[:xid],
      begin_lsn: transaction[:begin_lsn],
      end_lsn: transaction[:end_lsn],
      routes: routes
    }
  end

  @spec build_mapping([designated_table()]) ::
          %{
            optional({String.t(), String.t()}) => %{
              id: integer() | nil,
              target: {String.t(), String.t()},
              mode: String.t()
            }
          }
  def build_mapping(designated_tables) do
    designated_tables
    |> Enum.reduce(%{}, fn designated_table, acc ->
      source =
        {
          fetch!(designated_table, :source_schema),
          fetch!(designated_table, :source_table)
        }

      target =
        {
          fetch!(designated_table, :target_schema),
          fetch!(designated_table, :target_table)
        }

      Map.put(acc, source, %{
        id: Map.get(designated_table, :id),
        target: target,
        mode: Map.get(designated_table, :mode, "cdc_changelog")
      })
    end)
  end

  defp fetch!(map, key) do
    case Map.get(map, key) do
      nil -> raise ArgumentError, "missing designated table key #{inspect(key)}"
      value -> value
    end
  end
end
