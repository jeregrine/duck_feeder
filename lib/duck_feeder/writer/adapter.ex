defmodule DuckFeeder.Writer.Adapter do
  @moduledoc """
  Write-path adapter behaviour.

  Intended to be implemented by a future Rust-backed Parquet writer adapter.
  """

  @type config :: map()
  @type batch :: %{required(:rows) => [map()]}

  @type write_result :: %{
          required(:local_path) => Path.t(),
          required(:row_count) => non_neg_integer(),
          required(:file_size_bytes) => non_neg_integer(),
          required(:format) => atom()
        }

  @callback write_batch(config(), batch(), keyword()) :: {:ok, write_result()} | {:error, term()}

  @callback cleanup(config(), write_result()) :: :ok | {:error, term()}
end
