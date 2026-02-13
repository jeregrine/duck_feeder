defmodule DuckFeeder.Storage.Adapter do
  @moduledoc """
  Semi-generic object storage behavior used by DuckFeeder.

  This behavior intentionally stays small and only models operations needed by ingest:
  put, head and delete.
  """

  @type config :: map()

  @type object_ref :: %{
          required(:bucket) => String.t(),
          required(:key) => String.t()
        }

  @type put_opts :: keyword()

  @type put_result :: %{
          required(:etag) => String.t() | nil,
          required(:version_id) => String.t() | nil,
          required(:size) => non_neg_integer()
        }

  @callback put_file(config(), local_path :: Path.t(), object_ref(), put_opts()) ::
              {:ok, put_result()} | {:error, term()}

  @callback head_object(config(), object_ref()) :: {:ok, map()} | {:error, term()}

  @callback delete_object(config(), object_ref()) :: :ok | {:error, term()}
end
