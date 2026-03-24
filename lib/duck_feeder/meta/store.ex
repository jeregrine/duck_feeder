defmodule DuckFeeder.Meta.Store do
  @moduledoc """
  PostgreSQL-backed control-plane store (`duckfeeder_meta`).

  This module keeps only the durable runtime state needed for restart
  correctness in Postgres.
  """

  alias DuckFeeder.CDC.Lsn
  alias DuckFeeder.Meta.SQL

  @fetch_start_lsn_sql """
  SELECT
    COUNT(*)::bigint AS matched_count,
    MIN(last_committed_lsn)::text AS min_lsn
  FROM duckfeeder_meta.checkpoints
  WHERE checkpoint_key = ANY($1::text[])
  """

  @fetch_checkpoint_sql """
  SELECT last_committed_lsn::text
  FROM duckfeeder_meta.checkpoints
  WHERE checkpoint_key = $1
  """

  @fetch_snapshot_handoff_sql """
  SELECT state, boundary_lsn::text, started_at, completed_at, updated_at
  FROM duckfeeder_meta.snapshot_handoffs
  WHERE source_name = $1
  """

  @upsert_snapshot_handoff_pending_sql """
  INSERT INTO duckfeeder_meta.snapshot_handoffs
    (source_name, state, boundary_lsn, started_at, completed_at, updated_at)
  VALUES
    ($1, 'pending', $2::pg_lsn, now(), NULL, now())
  ON CONFLICT (source_name) DO UPDATE SET
    state = 'pending',
    boundary_lsn = EXCLUDED.boundary_lsn,
    started_at = now(),
    completed_at = NULL,
    updated_at = now()
  RETURNING boundary_lsn::text
  """

  @upsert_snapshot_handoff_complete_sql """
  INSERT INTO duckfeeder_meta.snapshot_handoffs
    (source_name, state, boundary_lsn, started_at, completed_at, updated_at)
  VALUES
    ($1, 'complete', $2::pg_lsn, now(), now(), now())
  ON CONFLICT (source_name) DO UPDATE SET
    state = 'complete',
    boundary_lsn = EXCLUDED.boundary_lsn,
    completed_at = now(),
    updated_at = now()
  RETURNING boundary_lsn::text
  """

  @delete_snapshot_handoff_sql """
  DELETE FROM duckfeeder_meta.snapshot_handoffs
  WHERE source_name = $1
  """

  @upsert_checkpoint_sql """
  INSERT INTO duckfeeder_meta.checkpoints (checkpoint_key, last_committed_lsn, updated_at)
  VALUES ($1, $2::pg_lsn, now())
  ON CONFLICT (checkpoint_key) DO UPDATE SET
    last_committed_lsn = EXCLUDED.last_committed_lsn,
    updated_at = now()
  RETURNING last_committed_lsn::text
  """

  @type conn :: pid()

  @spec bootstrap(conn()) :: :ok | {:error, term()}
  def bootstrap(conn) do
    SQL.bootstrap_statements()
    |> Enum.reduce_while(:ok, fn statement, :ok ->
      case Postgrex.query(conn, statement, []) do
        {:ok, _} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {:bootstrap_failed, reason, statement}}}
      end
    end)
  end

  @spec fetch_start_lsn(conn(), [String.t()], String.t()) :: {:ok, String.t()} | {:error, term()}
  def fetch_start_lsn(conn, checkpoint_keys, default_lsn \\ "0/0")
      when is_list(checkpoint_keys) and is_binary(default_lsn) do
    with {:ok, normalized_keys} <- normalize_checkpoint_keys(checkpoint_keys),
         {:ok, result} <- query(conn, @fetch_start_lsn_sql, [normalized_keys]),
         {:ok, lsn} <- fetch_start_lsn_value(result, normalized_keys, default_lsn) do
      {:ok, lsn}
    end
  end

  @spec fetch_checkpoint(conn(), String.t()) :: {:ok, String.t()} | {:error, term()}
  def fetch_checkpoint(conn, checkpoint_key) when is_binary(checkpoint_key) do
    with {:ok, result} <- query(conn, @fetch_checkpoint_sql, [checkpoint_key]) do
      case result.rows do
        [[lsn]] -> {:ok, lsn}
        [] -> {:ok, "0/0"}
      end
    end
  end

  @spec upsert_checkpoint(conn(), String.t(), String.t()) :: {:ok, String.t()} | {:error, term()}
  def upsert_checkpoint(conn, checkpoint_key, lsn)
      when is_binary(checkpoint_key) and is_binary(lsn) do
    with {:ok, lsn_param} <- lsn_param(lsn),
         {:ok, result} <- query(conn, @upsert_checkpoint_sql, [checkpoint_key, lsn_param]),
         {:ok, committed_lsn} <- single_value(result) do
      {:ok, committed_lsn}
    end
  end

  @spec fetch_snapshot_handoff(conn(), String.t()) :: {:ok, map() | nil} | {:error, term()}
  def fetch_snapshot_handoff(conn, source_name)
      when is_binary(source_name) and source_name != "" do
    with {:ok, result} <- query(conn, @fetch_snapshot_handoff_sql, [source_name]) do
      case result.rows do
        [[state, boundary_lsn, started_at, completed_at, updated_at]] ->
          with {:ok, normalized_state} <- snapshot_handoff_state_from_db(state) do
            {:ok,
             %{
               source_name: source_name,
               state: normalized_state,
               boundary_lsn: boundary_lsn,
               started_at: started_at,
               completed_at: completed_at,
               updated_at: updated_at
             }}
          end

        [] ->
          {:ok, nil}
      end
    end
  end

  @spec mark_snapshot_handoff_pending(conn(), String.t(), String.t()) ::
          {:ok, String.t()} | {:error, term()}
  def mark_snapshot_handoff_pending(conn, source_name, boundary_lsn)
      when is_binary(source_name) and source_name != "" and is_binary(boundary_lsn) do
    with {:ok, boundary_param} <- lsn_param(boundary_lsn),
         {:ok, result} <-
           query(conn, @upsert_snapshot_handoff_pending_sql, [source_name, boundary_param]),
         {:ok, committed_lsn} <- single_value(result) do
      {:ok, committed_lsn}
    end
  end

  @spec mark_snapshot_handoff_complete(conn(), String.t(), String.t()) ::
          {:ok, String.t()} | {:error, term()}
  def mark_snapshot_handoff_complete(conn, source_name, boundary_lsn)
      when is_binary(source_name) and source_name != "" and is_binary(boundary_lsn) do
    with {:ok, boundary_param} <- lsn_param(boundary_lsn),
         {:ok, result} <-
           query(conn, @upsert_snapshot_handoff_complete_sql, [source_name, boundary_param]),
         {:ok, committed_lsn} <- single_value(result) do
      {:ok, committed_lsn}
    end
  end

  @spec clear_snapshot_handoff(conn(), String.t()) :: :ok | {:error, term()}
  def clear_snapshot_handoff(conn, source_name)
      when is_binary(source_name) and source_name != "" do
    case query(conn, @delete_snapshot_handoff_sql, [source_name]) do
      {:ok, _result} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp query(conn, sql, params) do
    case Postgrex.query(conn, sql, params) do
      {:ok, result} -> {:ok, result}
      {:error, reason} -> {:error, {:postgres_query_failed, reason}}
    end
  end

  defp single_value(%Postgrex.Result{rows: [[value]]}), do: {:ok, value}
  defp single_value(%Postgrex.Result{rows: rows}), do: {:error, {:unexpected_rows, rows}}

  defp fetch_start_lsn_value(
         %Postgrex.Result{rows: [[matched_count, min_lsn]]},
         checkpoint_keys,
         default_lsn
       ) do
    expected_count = length(checkpoint_keys)

    cond do
      matched_count < expected_count ->
        {:ok, default_lsn}

      is_binary(min_lsn) and min_lsn != "" ->
        {:ok, min_lsn}

      true ->
        {:ok, default_lsn}
    end
  end

  defp fetch_start_lsn_value(%Postgrex.Result{rows: rows}, _checkpoint_keys, _default_lsn),
    do: {:error, {:unexpected_rows, rows}}

  defp lsn_param(lsn) when is_binary(lsn) do
    case Lsn.parse(lsn) do
      {:ok, parsed} -> {:ok, parsed}
      {:error, _reason} = error -> error
    end
  end

  defp lsn_param(lsn) when is_integer(lsn) and lsn >= 0, do: {:ok, lsn}
  defp lsn_param(other), do: {:error, {:invalid_lsn, other}}

  defp normalize_checkpoint_keys(checkpoint_keys) do
    checkpoint_keys =
      checkpoint_keys
      |> Enum.filter(&(is_binary(&1) and &1 != ""))
      |> Enum.uniq()

    {:ok, checkpoint_keys}
  end

  defp snapshot_handoff_state_from_db("pending"), do: {:ok, :pending}
  defp snapshot_handoff_state_from_db("complete"), do: {:ok, :complete}

  defp snapshot_handoff_state_from_db(other),
    do: {:error, {:invalid_snapshot_handoff_state, other}}
end
