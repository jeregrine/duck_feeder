defmodule DuckFeeder.DesignatedTable do
  @moduledoc false

  @type t :: map()

  @known_string_keys %{
    "source_schema" => :source_schema,
    "source_table" => :source_table,
    "target_schema" => :target_schema,
    "target_table" => :target_table,
    "mode" => :mode,
    "primary_keys" => :primary_keys,
    "checkpoint_key" => :checkpoint_key
  }

  @spec normalize(t()) :: t()
  def normalize(designated_table) when is_map(designated_table) do
    Enum.reduce(designated_table, %{}, fn
      {key, value}, acc when is_atom(key) ->
        Map.put(acc, key, normalize_value(key, value))

      {key, value}, acc when is_binary(key) ->
        case Map.get(@known_string_keys, key) do
          nil -> Map.put(acc, key, value)
          atom_key -> Map.put(acc, atom_key, normalize_value(atom_key, value))
        end

      {key, value}, acc ->
        Map.put(acc, key, value)
    end)
  end

  @spec put_checkpoint_keys([t()], String.t() | nil) :: [t()]
  def put_checkpoint_keys(designated_tables, prefix \\ nil) when is_list(designated_tables) do
    Enum.map(designated_tables, &put_checkpoint_key(&1, prefix))
  end

  @spec by_target([t()], String.t() | nil) :: %{optional({String.t(), String.t()}) => t()}
  def by_target(designated_tables, prefix \\ nil) when is_list(designated_tables) do
    Enum.reduce(designated_tables, %{}, fn designated_table, acc ->
      normalized_table = put_checkpoint_key(designated_table, prefix)
      target = target_relation(normalized_table)
      Map.put(acc, target, normalized_table)
    end)
  end

  @spec put_checkpoint_key(t(), String.t() | nil) :: t()
  def put_checkpoint_key(designated_table, prefix \\ nil) when is_map(designated_table) do
    designated_table
    |> normalize()
    |> Map.put_new(:checkpoint_key, checkpoint_key(designated_table, prefix))
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

  defp normalize_value(:primary_keys, value) do
    value
    |> List.wrap()
    |> Enum.map(&to_string/1)
  end

  defp normalize_value(key, value)
       when key in [
              :source_schema,
              :source_table,
              :target_schema,
              :target_table,
              :mode,
              :checkpoint_key
            ] and
              not is_nil(value) do
    to_string(value)
  end

  defp normalize_value(_key, value), do: value

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
