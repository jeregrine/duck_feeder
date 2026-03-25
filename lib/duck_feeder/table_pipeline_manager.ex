defmodule DuckFeeder.TablePipelineManager do
  @moduledoc false

  alias DuckFeeder.TablePipeline

  @type table :: {String.t(), String.t()}
  @type pipelines :: %{optional(table()) => pid()}

  @spec ensure_started(pipelines(), pid(), table(), pid(), map()) ::
          {:ok, pid(), pipelines()} | {:error, term()}
  def ensure_started(pipelines, pipeline_supervisor, table, sink_pid, pipeline_opts)
      when is_map(pipelines) and is_pid(pipeline_supervisor) and is_tuple(table) and
             is_pid(sink_pid) and is_map(pipeline_opts) do
    case Map.get(pipelines, table) do
      pid when is_pid(pid) ->
        if Process.alive?(pid) do
          {:ok, pid, pipelines}
        else
          start_pipeline(pipelines, pipeline_supervisor, table, sink_pid, pipeline_opts)
        end

      _ ->
        start_pipeline(pipelines, pipeline_supervisor, table, sink_pid, pipeline_opts)
    end
  end

  defp start_pipeline(pipelines, pipeline_supervisor, table, sink_pid, pipeline_opts) do
    opts = pipeline_start_opts(table, sink_pid, pipeline_opts)

    case DynamicSupervisor.start_child(pipeline_supervisor, {TablePipeline, opts}) do
      {:ok, pid} ->
        {:ok, pid, Map.put(pipelines, table, pid)}

      {:error, {:already_started, pid}} ->
        {:ok, pid, Map.put(pipelines, table, pid)}

      {:error, reason} ->
        {:error, {:pipeline_start_failed, table, reason}}
    end
  end

  defp pipeline_start_opts(table, sink_pid, pipeline_opts) do
    [
      table: table,
      sink_pid: sink_pid,
      max_rows: Map.get(pipeline_opts, :max_rows, 10_000),
      max_bytes: Map.get(pipeline_opts, :max_bytes, 128 * 1_024 * 1_024),
      flush_interval_ms: Map.get(pipeline_opts, :flush_interval_ms, 5_000)
    ]
  end
end
