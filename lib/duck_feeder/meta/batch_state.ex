defmodule DuckFeeder.Meta.BatchState do
  @moduledoc """
  Batch state machine used by `duckfeeder_meta.batches.state`.
  """

  @states [:pending, :encoded, :uploaded, :committed, :failed]

  @allowed_transitions %{
    pending: [:pending, :encoded, :failed],
    encoded: [:encoded, :uploaded, :failed],
    uploaded: [:uploaded, :committed, :failed],
    committed: [:committed],
    failed: [:failed, :pending]
  }

  @state_lookup Map.new(@states, fn state -> {Atom.to_string(state), state} end)

  @type t :: :pending | :encoded | :uploaded | :committed | :failed

  @spec states() :: [t()]
  def states, do: @states

  @spec normalize_state(t() | String.t()) :: {:ok, t()} | {:error, term()}
  def normalize_state(state) when is_atom(state) do
    if state in @states, do: {:ok, state}, else: {:error, {:invalid_batch_state, state}}
  end

  def normalize_state(state) when is_binary(state) do
    case Map.get(@state_lookup, state) do
      nil -> {:error, {:invalid_batch_state, state}}
      normalized -> {:ok, normalized}
    end
  end

  def normalize_state(state), do: {:error, {:invalid_batch_state, state}}

  @spec to_db(t() | String.t()) :: {:ok, String.t()} | {:error, term()}
  def to_db(state) do
    with {:ok, normalized} <- normalize_state(state) do
      {:ok, Atom.to_string(normalized)}
    end
  end

  @spec from_db(String.t() | t()) :: {:ok, t()} | {:error, term()}
  def from_db(state), do: normalize_state(state)

  @spec valid_transition?(t() | String.t(), t() | String.t()) :: boolean()
  def valid_transition?(from_state, to_state) do
    with {:ok, from} <- normalize_state(from_state),
         {:ok, to} <- normalize_state(to_state) do
      to in Map.fetch!(@allowed_transitions, from)
    else
      _ -> false
    end
  end

  @spec validate_transition(t() | String.t(), t() | String.t()) :: :ok | {:error, term()}
  def validate_transition(from_state, to_state) do
    with {:ok, from} <- normalize_state(from_state),
         {:ok, to} <- normalize_state(to_state) do
      if to in Map.fetch!(@allowed_transitions, from) do
        :ok
      else
        {:error, {:invalid_batch_transition, from, to}}
      end
    end
  end
end
