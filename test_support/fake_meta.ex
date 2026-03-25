defmodule DuckFeeder.TestSupport.FakeMeta do
  @moduledoc false

  def upsert_checkpoint(_conn, _checkpoint_key, lsn), do: {:ok, lsn}
end
