defmodule DuckFeeder.Migrations do
  @moduledoc """
  Migration entrypoint for integrating DuckFeeder schema setup with Ecto migrations.

  Typical Ecto migration usage:

      defmodule MyApp.Repo.Migrations.AddDuckFeeder do
        use Ecto.Migration

        def up, do: DuckFeeder.Migrations.up(repo: repo())
        def down, do: DuckFeeder.Migrations.down(repo: repo())
      end
  """

  defdelegate up(opts \\ []), to: DuckFeeder.Migration
  defdelegate down(opts \\ []), to: DuckFeeder.Migration
  defdelegate migrated_version(opts \\ []), to: DuckFeeder.Migration
  defdelegate current_version(), to: DuckFeeder.Migration
end

defmodule DuckFeeder.Migration do
  @moduledoc false

  @initial_version 1
  @current_version 1

  @version_table_sql """
  CREATE SCHEMA IF NOT EXISTS duckfeeder_meta;
  CREATE TABLE IF NOT EXISTS duckfeeder_meta.migration_versions (
    id BOOLEAN PRIMARY KEY DEFAULT true,
    version INTEGER NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT duckfeeder_single_row CHECK (id)
  )
  """

  @spec initial_version() :: pos_integer()
  def initial_version, do: @initial_version

  @spec current_version() :: pos_integer()
  def current_version, do: @current_version

  @spec up(keyword()) :: :ok
  def up(opts \\ []) when is_list(opts) do
    repo = fetch_repo!(opts)

    if migrated_version(opts) < @current_version do
      run_bootstrap(repo)
      record_version(repo, @current_version)
    end

    :ok
  end

  @spec down(keyword()) :: :ok
  def down(opts \\ []) when is_list(opts) do
    repo = fetch_repo!(opts)

    query!(repo, "DROP SCHEMA IF EXISTS duckfeeder_meta CASCADE")

    :ok
  end

  @spec migrated_version(keyword()) :: non_neg_integer()
  def migrated_version(opts \\ []) when is_list(opts) do
    repo = fetch_repo!(opts)

    ensure_version_table(repo)

    case repo.query("SELECT version FROM duckfeeder_meta.migration_versions WHERE id = true", [],
           log: false
         ) do
      {:ok, %{rows: [[version]]}} when is_integer(version) -> version
      _ -> 0
    end
  rescue
    _ -> 0
  end

  defp run_bootstrap(repo) do
    DuckFeeder.Meta.SQL.bootstrap_statements()
    |> Enum.each(fn statement -> query!(repo, statement) end)
  end

  defp record_version(repo, version) when is_integer(version) do
    ensure_version_table(repo)

    query!(
      repo,
      """
      INSERT INTO duckfeeder_meta.migration_versions (id, version, updated_at)
      VALUES (true, $1, now())
      ON CONFLICT (id) DO UPDATE SET
        version = EXCLUDED.version,
        updated_at = now()
      """,
      [version]
    )
  end

  defp ensure_version_table(repo) do
    @version_table_sql
    |> String.split(";", trim: true)
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.each(fn statement -> query!(repo, statement) end)
  end

  defp fetch_repo!(opts) do
    case Keyword.fetch(opts, :repo) do
      {:ok, repo} when is_atom(repo) ->
        repo

      {:ok, repo} ->
        repo

      :error ->
        raise ArgumentError,
              "missing :repo option (pass repo: MyApp.Repo from your Ecto migration)"
    end
  end

  defp query!(repo, sql, params \\ []) do
    case repo.query(sql, params, log: false) do
      {:ok, _result} -> :ok
      {:error, reason} -> raise "duck_feeder migration query failed: #{Exception.message(reason)}"
    end
  end
end
