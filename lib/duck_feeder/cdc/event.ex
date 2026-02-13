defmodule DuckFeeder.CDC.Event do
  @moduledoc """
  Normalized CDC events used inside DuckFeeder.

  These structs are intentionally independent from wire-level pgoutput decoding so
  we can reuse them in tests and future source adapters.
  """

  defmodule Begin do
    @enforce_keys [:xid, :final_lsn]
    defstruct [:xid, :final_lsn, :timestamp]

    @type t :: %__MODULE__{
            xid: non_neg_integer(),
            final_lsn: String.t(),
            timestamp: DateTime.t() | nil
          }
  end

  defmodule Commit do
    @enforce_keys [:xid, :end_lsn]
    defstruct [:xid, :end_lsn, :timestamp]

    @type t :: %__MODULE__{
            xid: non_neg_integer(),
            end_lsn: String.t(),
            timestamp: DateTime.t() | nil
          }
  end

  defmodule Relation do
    @enforce_keys [:id, :schema, :table]
    defstruct [:id, :schema, :table, columns: []]

    @type t :: %__MODULE__{
            id: integer(),
            schema: String.t(),
            table: String.t(),
            columns: [map()]
          }
  end

  defmodule Insert do
    @enforce_keys [:relation_id, :record]
    defstruct [:relation_id, :record]

    @type t :: %__MODULE__{relation_id: integer(), record: map()}
  end

  defmodule Update do
    @enforce_keys [:relation_id, :record, :old_record]
    defstruct [:relation_id, :record, :old_record]

    @type t :: %__MODULE__{relation_id: integer(), record: map(), old_record: map()}
  end

  defmodule Delete do
    @enforce_keys [:relation_id, :old_record]
    defstruct [:relation_id, :old_record]

    @type t :: %__MODULE__{relation_id: integer(), old_record: map()}
  end

  defmodule Truncate do
    @enforce_keys [:relation_ids]
    defstruct [:relation_ids]

    @type t :: %__MODULE__{relation_ids: [integer()]}
  end

  @type t ::
          Begin.t()
          | Commit.t()
          | Relation.t()
          | Insert.t()
          | Update.t()
          | Delete.t()
          | Truncate.t()
end
