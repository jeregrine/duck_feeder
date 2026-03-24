defmodule DuckFeeder.DesignatedTable do
  @moduledoc false

  @type t :: map()

  @spec put_checkpoint_keys([t()], String.t() | nil) :: [t()]
  def put_checkpoint_keys(designated_tables, prefix \\ nil) when is_list(designated_tables) do
    Enum.map(designated_tables, &put_checkpoint_key(&1, prefix))
  end

  @spec put_checkpoint_key(t(), String.t() | nil) :: t()
  def put_checkpoint_key(designated_table, prefix \\ nil) when is_map(designated_table) do
    Map.put_new(designated_table, :checkpoint_key, checkpoint_key(designated_table, prefix))
  end

  @spec checkpoint_keys([t()], String.t() | nil) :: [String.t()]
  def checkpoint_keys(designated_tables, prefix \\ nil) when is_list(designated_tables) do
    Enum.map(designated_tables, &checkpoint_key(&1, prefix))
  end

  @spec checkpoint_key(t(), String.t() | nil) :: String.t()
  def checkpoint_key(designated_table, prefix \\ nil) when is_map(designated_table) do
    case fetch_value(designated_table, :checkpoint_key) do
      value when is_binary(value) and value != "" ->
        value

      _ ->
        {target_schema, target_table} = target_relation(designated_table)
        base = "#{target_schema}.#{target_table}"

        case prefix do
          value when is_binary(value) and value != "" -> "#{value}:#{base}"
          _ -> base
        end
    end
  end

  @spec target_relation(t()) :: {String.t(), String.t()}
  def target_relation(designated_table) when is_map(designated_table) do
    {fetch_string!(designated_table, :target_schema),
     fetch_string!(designated_table, :target_table)}
  end

  defp fetch_string!(map, key) do
    case fetch_value(map, key) do
      value when is_binary(value) and value != "" -> value
      _ -> raise ArgumentError, "missing designated table key #{inspect(key)}"
    end
  end

  defp fetch_value(map, key) when is_map(map) and is_atom(key) do
    Map.get(map, key) || Map.get(map, Atom.to_string(key))
  end
end
