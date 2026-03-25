defmodule DuckFeeder.TestSupport.ProcessHelpers do
  @moduledoc false

  def safe_stop(pid) when is_pid(pid) do
    _ = GenServer.stop(pid)
    :ok
  catch
    :exit, _reason -> :ok
  end

  def safe_stop(_), do: :ok
end
