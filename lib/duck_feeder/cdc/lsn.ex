defmodule DuckFeeder.CDC.Lsn do
  @moduledoc """
  Postgres LSN helpers.

  LSN textual format is `"HEX_SEGMENT/HEX_OFFSET"`.
  """

  @lsn_regex ~r/\A([0-9A-Fa-f]+)\/([0-9A-Fa-f]+)\z/

  @spec parse(String.t()) :: {:ok, non_neg_integer()} | {:error, term()}
  def parse(lsn) when is_binary(lsn) do
    case Regex.run(@lsn_regex, lsn, capture: :all_but_first) do
      [segment_hex, offset_hex] ->
        with {segment, ""} <- Integer.parse(segment_hex, 16),
             {offset, ""} <- Integer.parse(offset_hex, 16) do
          {:ok, segment * 4_294_967_296 + offset}
        else
          _ -> {:error, {:invalid_lsn, lsn}}
        end

      _ ->
        {:error, {:invalid_lsn, lsn}}
    end
  end

  @spec parse!(String.t()) :: non_neg_integer()
  def parse!(lsn) do
    case parse(lsn) do
      {:ok, value} -> value
      {:error, reason} -> raise ArgumentError, "invalid lsn: #{inspect(lsn)} (#{inspect(reason)})"
    end
  end

  @spec to_string(non_neg_integer()) :: String.t()
  def to_string(value) when is_integer(value) and value >= 0 do
    segment = div(value, 4_294_967_296)
    offset = rem(value, 4_294_967_296)

    Integer.to_string(segment, 16)
    |> String.upcase()
    |> Kernel.<>("/")
    |> Kernel.<>(Integer.to_string(offset, 16) |> String.upcase())
  end

  @spec compare(String.t(), String.t()) :: :lt | :eq | :gt | {:error, term()}
  def compare(lsn_a, lsn_b) when is_binary(lsn_a) and is_binary(lsn_b) do
    with {:ok, a} <- parse(lsn_a),
         {:ok, b} <- parse(lsn_b) do
      cond do
        a < b -> :lt
        a > b -> :gt
        true -> :eq
      end
    end
  end

  @spec max(String.t(), String.t()) :: String.t() | {:error, term()}
  def max(lsn_a, lsn_b) do
    case compare(lsn_a, lsn_b) do
      :lt -> lsn_b
      :gt -> lsn_a
      :eq -> lsn_a
      {:error, _} = error -> error
    end
  end
end
