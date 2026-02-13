defmodule DuckFeeder.Meta.Store do
  @moduledoc """
  PostgreSQL-backed control-plane store (`duckfeeder_meta`).

  This module keeps the checkpoint and batch state machine persistence in Postgres.
  """

  alias DuckFeeder.Meta.{BatchState, SQL}

  @register_source_sql """
  INSERT INTO duckfeeder_meta.sources
    (name, connection_info, slot_name, publication_name, status, inserted_at, updated_at)
  VALUES
    ($1, $2, $3, $4, $5, now(), now())
  ON CONFLICT (name) DO UPDATE SET
    connection_info = EXCLUDED.connection_info,
    slot_name = EXCLUDED.slot_name,
    publication_name = EXCLUDED.publication_name,
    status = EXCLUDED.status,
    updated_at = now()
  RETURNING id
  """

  @register_designated_table_sql """
  INSERT INTO duckfeeder_meta.designated_tables
    (
      source_id,
      source_schema,
      source_table,
      target_schema,
      target_table,
      mode,
      primary_keys,
      partition_config,
      inserted_at,
      updated_at
    )
  VALUES
    ($1, $2, $3, $4, $5, $6, $7, $8, now(), now())
  ON CONFLICT (source_id, source_schema, source_table) DO UPDATE SET
    target_schema = EXCLUDED.target_schema,
    target_table = EXCLUDED.target_table,
    mode = EXCLUDED.mode,
    primary_keys = EXCLUDED.primary_keys,
    partition_config = EXCLUDED.partition_config,
    updated_at = now()
  RETURNING id
  """

  @fetch_checkpoint_sql """
  SELECT last_committed_lsn::text
  FROM duckfeeder_meta.checkpoints
  WHERE designated_table_id = $1
  """

  @upsert_checkpoint_sql """
  INSERT INTO duckfeeder_meta.checkpoints (designated_table_id, last_committed_lsn, updated_at)
  VALUES ($1, $2::pg_lsn, now())
  ON CONFLICT (designated_table_id) DO UPDATE SET
    last_committed_lsn = EXCLUDED.last_committed_lsn,
    updated_at = now()
  RETURNING last_committed_lsn::text
  """

  @insert_batch_sql """
  INSERT INTO duckfeeder_meta.batches
    (batch_id, designated_table_id, lsn_start, lsn_end, state, error_message, retry_count, inserted_at, updated_at)
  VALUES
    ($1, $2, $3::pg_lsn, $4::pg_lsn, $5, $6, $7, now(), now())
  ON CONFLICT (batch_id) DO NOTHING
  RETURNING state
  """

  @get_batch_state_sql """
  SELECT state
  FROM duckfeeder_meta.batches
  WHERE batch_id = $1
  """

  @lock_batch_state_sql """
  SELECT state
  FROM duckfeeder_meta.batches
  WHERE batch_id = $1
  FOR UPDATE
  """

  @transition_batch_sql """
  UPDATE duckfeeder_meta.batches
  SET
    state = $2,
    error_message = $3,
    retry_count = retry_count + CASE WHEN $2 = 'failed' THEN 1 ELSE 0 END,
    updated_at = now()
  WHERE batch_id = $1
  RETURNING state
  """

  @insert_batch_file_sql """
  INSERT INTO duckfeeder_meta.batch_files
    (batch_id, object_key, row_count, file_size, checksum, etag, inserted_at)
  VALUES
    ($1, $2, $3, $4, $5, $6, now())
  ON CONFLICT (batch_id, object_key) DO UPDATE SET
    row_count = EXCLUDED.row_count,
    file_size = EXCLUDED.file_size,
    checksum = EXCLUDED.checksum,
    etag = EXCLUDED.etag
  RETURNING id
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

  @spec register_source(conn(), map()) :: {:ok, pos_integer()} | {:error, term()}
  def register_source(conn, attrs) when is_map(attrs) do
    with {:ok, name} <- fetch_required(attrs, :name),
         {:ok, status} <- normalize_source_status(get_attr(attrs, :status, "active")),
         {:ok, result} <-
           query(
             conn,
             @register_source_sql,
             [
               name,
               get_attr(attrs, :connection_info, %{}),
               get_attr(attrs, :slot_name),
               get_attr(attrs, :publication_name),
               status
             ]
           ),
         {:ok, id} <- single_value(result) do
      {:ok, id}
    end
  end

  @spec register_designated_table(conn(), map()) :: {:ok, pos_integer()} | {:error, term()}
  def register_designated_table(conn, attrs) when is_map(attrs) do
    with {:ok, source_id} <- fetch_required(attrs, :source_id),
         {:ok, source_schema} <- fetch_required(attrs, :source_schema),
         {:ok, source_table} <- fetch_required(attrs, :source_table),
         {:ok, target_schema} <- fetch_required(attrs, :target_schema),
         {:ok, target_table} <- fetch_required(attrs, :target_table),
         {:ok, mode} <- normalize_mode(get_attr(attrs, :mode, "cdc_changelog")),
         {:ok, result} <-
           query(
             conn,
             @register_designated_table_sql,
             [
               source_id,
               source_schema,
               source_table,
               target_schema,
               target_table,
               mode,
               get_attr(attrs, :primary_keys, []),
               get_attr(attrs, :partition_config, %{})
             ]
           ),
         {:ok, id} <- single_value(result) do
      {:ok, id}
    end
  end

  @spec fetch_checkpoint(conn(), pos_integer()) :: {:ok, String.t()} | {:error, term()}
  def fetch_checkpoint(conn, designated_table_id) do
    with {:ok, result} <- query(conn, @fetch_checkpoint_sql, [designated_table_id]) do
      case result.rows do
        [[lsn]] -> {:ok, lsn}
        [] -> {:ok, "0/0"}
      end
    end
  end

  @spec upsert_checkpoint(conn(), pos_integer(), String.t()) ::
          {:ok, String.t()} | {:error, term()}
  def upsert_checkpoint(conn, designated_table_id, lsn) when is_binary(lsn) do
    with {:ok, result} <- query(conn, @upsert_checkpoint_sql, [designated_table_id, lsn]),
         {:ok, committed_lsn} <- single_value(result) do
      {:ok, committed_lsn}
    end
  end

  @spec insert_batch(conn(), map()) :: {:ok, map()} | {:error, term()}
  def insert_batch(conn, attrs) when is_map(attrs) do
    with {:ok, batch_id} <- fetch_required(attrs, :batch_id),
         {:ok, designated_table_id} <- fetch_required(attrs, :designated_table_id),
         {:ok, lsn_start} <- fetch_required(attrs, :lsn_start),
         {:ok, lsn_end} <- fetch_required(attrs, :lsn_end),
         {:ok, state_db} <- BatchState.to_db(get_attr(attrs, :state, :pending)),
         {:ok, result} <-
           query(
             conn,
             @insert_batch_sql,
             [
               batch_id,
               designated_table_id,
               lsn_start,
               lsn_end,
               state_db,
               get_attr(attrs, :error_message),
               get_attr(attrs, :retry_count, 0)
             ]
           ) do
      case result.rows do
        [[inserted_state]] ->
          {:ok, normalized_state} = BatchState.from_db(inserted_state)
          {:ok, %{batch_id: batch_id, state: normalized_state, inserted?: true}}

        [] ->
          with {:ok, existing_state} <- get_batch_state(conn, batch_id) do
            {:ok, %{batch_id: batch_id, state: existing_state, inserted?: false}}
          end
      end
    end
  end

  @spec get_batch_state(conn(), String.t()) :: {:ok, BatchState.t()} | {:error, term()}
  def get_batch_state(conn, batch_id) when is_binary(batch_id) do
    with {:ok, result} <- query(conn, @get_batch_state_sql, [batch_id]) do
      case result.rows do
        [[state]] -> BatchState.from_db(state)
        [] -> {:error, {:batch_not_found, batch_id}}
      end
    end
  end

  @spec transition_batch(conn(), String.t(), BatchState.t() | String.t(), keyword()) ::
          {:ok, %{batch_id: String.t(), from: BatchState.t(), to: BatchState.t()}}
          | {:error, term()}
  def transition_batch(conn, batch_id, to_state, opts \\ []) when is_binary(batch_id) do
    error_message = Keyword.get(opts, :error_message)

    with {:ok, to_state} <- BatchState.normalize_state(to_state) do
      conn
      |> Postgrex.transaction(fn tx_conn ->
        with {:ok, from_state} <- lock_batch_state(tx_conn, batch_id),
             :ok <- BatchState.validate_transition(from_state, to_state),
             {:ok, _} <- update_batch_state(tx_conn, batch_id, to_state, error_message) do
          %{batch_id: batch_id, from: from_state, to: to_state}
        else
          {:error, reason} -> Postgrex.rollback(tx_conn, reason)
        end
      end)
      |> normalize_transaction_result()
    end
  end

  @spec put_batch_file(conn(), map()) :: {:ok, pos_integer()} | {:error, term()}
  def put_batch_file(conn, attrs) when is_map(attrs) do
    with {:ok, batch_id} <- fetch_required(attrs, :batch_id),
         {:ok, object_key} <- fetch_required(attrs, :object_key),
         {:ok, result} <-
           query(
             conn,
             @insert_batch_file_sql,
             [
               batch_id,
               object_key,
               get_attr(attrs, :row_count, 0),
               get_attr(attrs, :file_size, 0),
               get_attr(attrs, :checksum),
               get_attr(attrs, :etag)
             ]
           ),
         {:ok, id} <- single_value(result) do
      {:ok, id}
    end
  end

  defp lock_batch_state(conn, batch_id) do
    with {:ok, result} <- query(conn, @lock_batch_state_sql, [batch_id]) do
      case result.rows do
        [[state]] -> BatchState.from_db(state)
        [] -> {:error, {:batch_not_found, batch_id}}
      end
    end
  end

  defp update_batch_state(conn, batch_id, to_state, error_message) do
    with {:ok, state_db} <- BatchState.to_db(to_state),
         {:ok, result} <- query(conn, @transition_batch_sql, [batch_id, state_db, error_message]),
         {:ok, _state} <- single_value(result) do
      {:ok, to_state}
    end
  end

  defp normalize_transaction_result({:ok, result}), do: {:ok, result}
  defp normalize_transaction_result({:error, reason}), do: {:error, reason}

  defp query(conn, sql, params) do
    case Postgrex.query(conn, sql, params) do
      {:ok, result} -> {:ok, result}
      {:error, reason} -> {:error, {:postgres_query_failed, reason}}
    end
  end

  defp single_value(%Postgrex.Result{rows: [[value]]}), do: {:ok, value}
  defp single_value(%Postgrex.Result{rows: rows}), do: {:error, {:unexpected_rows, rows}}

  defp fetch_required(attrs, key) do
    case get_attr(attrs, key) do
      nil -> {:error, {:missing_required, key}}
      "" -> {:error, {:missing_required, key}}
      value -> {:ok, value}
    end
  end

  defp get_attr(attrs, key, default \\ nil) do
    atom_value = Map.get(attrs, key)
    string_key = Atom.to_string(key)

    cond do
      not is_nil(atom_value) -> atom_value
      Map.has_key?(attrs, key) -> atom_value
      Map.has_key?(attrs, string_key) -> Map.get(attrs, string_key)
      true -> default
    end
  end

  defp normalize_source_status(status) when is_atom(status),
    do: normalize_source_status(Atom.to_string(status))

  defp normalize_source_status(status) when is_binary(status) do
    if status in ["active", "paused", "error"] do
      {:ok, status}
    else
      {:error, {:invalid_source_status, status}}
    end
  end

  defp normalize_source_status(status), do: {:error, {:invalid_source_status, status}}

  defp normalize_mode(mode) when is_atom(mode), do: normalize_mode(Atom.to_string(mode))

  defp normalize_mode(mode) when is_binary(mode) do
    if mode == "cdc_changelog" do
      {:ok, mode}
    else
      {:error, {:invalid_designated_table_mode, mode}}
    end
  end

  defp normalize_mode(mode), do: {:error, {:invalid_designated_table_mode, mode}}
end
