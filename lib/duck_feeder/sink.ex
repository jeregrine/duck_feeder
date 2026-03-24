defmodule DuckFeeder.Sink do
  @moduledoc """
  Downstream batch sink behaviour.

  CDC routing, snapshot replay, queueing, and WAL checkpoint discipline live
  above this module. The sink is the seam where a flushed table batch becomes a
  durable downstream commit.

  The default (and currently only) sink is `DuckFeeder.Sink.DuckDB`, which
  writes directly into DuckDB-managed tables and tracks applied batches for
  deduplication.
  """

  @type context :: map()
  @type table :: {String.t(), String.t()}
  @type batch :: map()

  @callback process_batch(context(), table(), batch()) :: {:ok, map()} | {:error, term()}

  @default_module DuckFeeder.Sink.DuckDB

  @spec default_module() :: module()
  def default_module, do: @default_module

  @spec process_batch(context(), table(), batch()) :: {:ok, map()} | {:error, term()}
  def process_batch(context, table, batch)
      when is_map(context) and is_tuple(table) and is_map(batch) do
    with {:ok, sink_module} <- normalize_module(Map.get(context, :sink_module)) do
      sink_module.process_batch(context, table, batch)
    end
  end

  @spec normalize_module(module() | nil | term()) :: {:ok, module()} | {:error, term()}
  def normalize_module(nil), do: {:ok, @default_module}

  def normalize_module(module) when is_atom(module) do
    case Code.ensure_loaded(module) do
      {:module, ^module} ->
        if function_exported?(module, :process_batch, 3) do
          {:ok, module}
        else
          {:error, {:invalid_sink_module, module}}
        end

      {:error, _reason} ->
        {:error, {:invalid_sink_module, module}}
    end
  end

  def normalize_module(other), do: {:error, {:invalid_sink_module, other}}
end
