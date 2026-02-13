defmodule DuckFeeder.CDC.LogicalReplication.Messages do
  @moduledoc """
  pgoutput logical replication message structs.

  These are wire-level decoded messages before conversion into
  `DuckFeeder.CDC.Event`.
  """

  @type relation_id :: non_neg_integer()

  defmodule Begin do
    @enforce_keys [:xid, :final_lsn, :commit_timestamp]
    defstruct [:xid, :final_lsn, :commit_timestamp]

    @type t :: %__MODULE__{
            xid: non_neg_integer(),
            final_lsn: non_neg_integer(),
            commit_timestamp: DateTime.t()
          }
  end

  defmodule Commit do
    @enforce_keys [:flags, :lsn, :end_lsn, :commit_timestamp]
    defstruct [:flags, :lsn, :end_lsn, :commit_timestamp]

    @type t :: %__MODULE__{
            flags: [atom()],
            lsn: non_neg_integer(),
            end_lsn: non_neg_integer(),
            commit_timestamp: DateTime.t()
          }
  end

  defmodule Origin do
    @enforce_keys [:origin_commit_lsn, :name]
    defstruct [:origin_commit_lsn, :name]

    @type t :: %__MODULE__{origin_commit_lsn: non_neg_integer(), name: String.t()}
  end

  defmodule Message do
    @enforce_keys [:transactional?, :lsn, :prefix, :content]
    defstruct [:transactional?, :lsn, :prefix, :content]

    @type t :: %__MODULE__{
            transactional?: boolean(),
            lsn: non_neg_integer(),
            prefix: String.t(),
            content: binary()
          }
  end

  defmodule Relation do
    @enforce_keys [:id, :namespace, :name, :replica_identity, :columns]
    defstruct [:id, :namespace, :name, :replica_identity, :columns]

    @type replica_identity :: :default | :nothing | :all_columns | :index

    @type t :: %__MODULE__{
            id: DuckFeeder.CDC.LogicalReplication.Messages.relation_id(),
            namespace: String.t(),
            name: String.t(),
            replica_identity: replica_identity(),
            columns: [DuckFeeder.CDC.LogicalReplication.Messages.Relation.Column.t()]
          }
  end

  defmodule Relation.Column do
    @enforce_keys [:flags, :name, :type_oid, :type_modifier]
    defstruct [:flags, :name, :type_oid, :type_modifier]

    @type t :: %__MODULE__{
            flags: [:key],
            name: String.t(),
            type_oid: non_neg_integer(),
            type_modifier: integer()
          }
  end

  defmodule Insert do
    @enforce_keys [:relation_id, :tuple_data, :bytes]
    defstruct [:relation_id, :tuple_data, :bytes]

    @type t :: %__MODULE__{
            relation_id: DuckFeeder.CDC.LogicalReplication.Messages.relation_id(),
            tuple_data: [binary() | nil | :unchanged_toast],
            bytes: non_neg_integer()
          }
  end

  defmodule Update do
    @enforce_keys [:relation_id, :tuple_data, :bytes]
    defstruct [:relation_id, :tuple_data, :changed_key_tuple_data, :old_tuple_data, :bytes]

    @type t :: %__MODULE__{
            relation_id: DuckFeeder.CDC.LogicalReplication.Messages.relation_id(),
            changed_key_tuple_data: [binary() | nil | :unchanged_toast] | nil,
            old_tuple_data: [binary() | nil | :unchanged_toast] | nil,
            tuple_data: [binary() | nil | :unchanged_toast],
            bytes: non_neg_integer()
          }
  end

  defmodule Delete do
    @enforce_keys [:relation_id, :bytes]
    defstruct [:relation_id, :changed_key_tuple_data, :old_tuple_data, :bytes]

    @type t :: %__MODULE__{
            relation_id: DuckFeeder.CDC.LogicalReplication.Messages.relation_id(),
            changed_key_tuple_data: [binary() | nil | :unchanged_toast] | nil,
            old_tuple_data: [binary() | nil | :unchanged_toast] | nil,
            bytes: non_neg_integer()
          }
  end

  defmodule Truncate do
    @enforce_keys [:number_of_relations, :options, :truncated_relations]
    defstruct [:number_of_relations, :options, :truncated_relations]

    @type t :: %__MODULE__{
            number_of_relations: non_neg_integer(),
            options: [atom()],
            truncated_relations: [DuckFeeder.CDC.LogicalReplication.Messages.relation_id()]
          }
  end

  defmodule Type do
    @enforce_keys [:id, :namespace, :name]
    defstruct [:id, :namespace, :name]

    @type t :: %__MODULE__{id: non_neg_integer(), namespace: String.t(), name: String.t()}
  end

  defmodule Unsupported do
    @enforce_keys [:type, :data]
    defstruct [:type, :data]

    @type t :: %__MODULE__{type: integer() | nil, data: binary()}
  end

  @type message ::
          Begin.t()
          | Commit.t()
          | Origin.t()
          | Message.t()
          | Relation.t()
          | Insert.t()
          | Update.t()
          | Delete.t()
          | Truncate.t()
          | Type.t()
          | Unsupported.t()
end
