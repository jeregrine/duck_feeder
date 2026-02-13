defmodule DuckFeeder do
  @moduledoc """
  DuckFeeder entrypoint.

  Currently exposes storage writes through a semi-generic interface.
  """

  defdelegate validate_config(config), to: DuckFeeder.Config, as: :validate
  defdelegate validate_config!(config), to: DuckFeeder.Config, as: :validate!
  defdelegate seed_meta(meta_conn, config, opts \\ []), to: DuckFeeder.Bootstrap
  defdelegate seed_and_start_stream(meta_conn, config, opts \\ []), to: DuckFeeder.Bootstrap

  defdelegate runtime_child_spec(meta_conn, source_name, storage_config, opts \\ []),
    to: DuckFeeder.Integration

  defdelegate runtime_child_spec_from_config(meta_conn, config, opts \\ []),
    to: DuckFeeder.Integration

  defdelegate write_batch(writer_config, batch, opts \\ []), to: DuckFeeder.Writer

  defdelegate cleanup_written_batch(writer_config, write_result),
    to: DuckFeeder.Writer,
    as: :cleanup

  defdelegate process_batch(context, table, batch), to: DuckFeeder.BatchProcessor

  defdelegate service_options(meta_conn, source_name, storage_config, opts \\ []),
    to: DuckFeeder.Runtime

  defdelegate start_service(meta_conn, source_name, storage_config, opts \\ []),
    to: DuckFeeder.Runtime

  defdelegate start_stream(meta_conn, source_name, storage_config, opts \\ []),
    to: DuckFeeder.Runtime

  defdelegate start_stream_worker(opts), to: DuckFeeder.Runtime.StreamWorker, as: :start_link
  defdelegate stream_worker_info(server), to: DuckFeeder.Runtime.StreamWorker, as: :stream_info
  defdelegate start_runtime_supervisor(opts), to: DuckFeeder.Runtime.Supervisor, as: :start_link

  defdelegate start_runtime_manager(opts), to: DuckFeeder.Runtime.Manager, as: :start_link

  defdelegate start_source_runtime(manager, source_name, opts \\ []),
    to: DuckFeeder.Runtime.Manager,
    as: :start_source

  defdelegate stop_source_runtime(manager, source_name),
    to: DuckFeeder.Runtime.Manager,
    as: :stop_source

  defdelegate list_source_runtimes(manager), to: DuckFeeder.Runtime.Manager, as: :list_sources

  defdelegate start_cdc_connection(opts), to: DuckFeeder.CDC.Connection, as: :start_link

  defdelegate reconcile(context, opts \\ []), to: DuckFeeder.Reconciler
  defdelegate start_reconciler(opts), to: DuckFeeder.Reconciler.Worker, as: :start_link
  defdelegate run_reconcile_once(server), to: DuckFeeder.Reconciler.Worker, as: :run_once

  defdelegate put_file(storage_config, local_path, relative_key, opts \\ []),
    to: DuckFeeder.Storage

  defdelegate head_object(storage_config, relative_key), to: DuckFeeder.Storage
  defdelegate delete_object(storage_config, relative_key), to: DuckFeeder.Storage
end
