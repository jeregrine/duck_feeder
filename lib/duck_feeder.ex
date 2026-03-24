defmodule DuckFeeder do
  @moduledoc """
  DuckFeeder entrypoint.

  This module is the public API facade over runtime/bootstrap/storage/writer/reconcile
  subsystems.

  System flow (CDC path):

      Postgres WAL
          |
          v
      CDC.Connection
          |
          v
      Service -> CDC.Pipeline -> Ingest -> TablePipeline (flush)
          |                                    |
          |<----------- {:duck_feeder_batch, ...} -----------|
          |
          v
      Sink (durable downstream commit)
          |
          v
      checkpoint_lsn persisted in Postgres
          |
          v
      Service -> CDC.Connection ack_lsn

  System flow (append path):

      producer rows
          |
          v
      AppendStream -> TablePipeline (flush)
          |
          v
      Sink (shared downstream commit path)

  Use the delegates in this module when integrating DuckFeeder into your OTP app.
  """

  defdelegate validate_config(config), to: DuckFeeder.Config, as: :validate
  defdelegate validate_config!(config), to: DuckFeeder.Config, as: :validate!
  defdelegate seed_meta(meta_conn, config, opts \\ []), to: DuckFeeder.Bootstrap
  defdelegate seed_and_start_stream(meta_conn, config, opts \\ []), to: DuckFeeder.Bootstrap

  defdelegate migrate_up(opts \\ []), to: DuckFeeder.Migrations, as: :up
  defdelegate migrate_down(opts \\ []), to: DuckFeeder.Migrations, as: :down
  defdelegate migrated_version(opts \\ []), to: DuckFeeder.Migrations

  defdelegate runtime_child_spec(meta_conn, source_name, storage_config, opts \\ []),
    to: DuckFeeder.Integration

  defdelegate runtime_child_spec_from_config(meta_conn, config, opts \\ []),
    to: DuckFeeder.Integration

  defdelegate process_batch(context, table, batch), to: DuckFeeder.Sink

  defdelegate service_options(meta_conn, source_name, storage_config, opts \\ []),
    to: DuckFeeder.Runtime

  defdelegate start_service(meta_conn, source_name, storage_config, opts \\ []),
    to: DuckFeeder.Runtime

  defdelegate start_stream(meta_conn, source_name, storage_config, opts \\ []),
    to: DuckFeeder.Runtime

  defdelegate start_append_stream(opts), to: DuckFeeder.AppendStream, as: :start_link

  defdelegate append_event(server, table, row, opts \\ []),
    to: DuckFeeder.AppendStream,
    as: :append

  defdelegate flush_append_table(server, table), to: DuckFeeder.AppendStream, as: :flush_table

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

  defdelegate start_telemetry_forwarder(opts),
    to: DuckFeeder.TelemetryForwarder,
    as: :start_link

  defdelegate flush_telemetry_forwarder(server),
    to: DuckFeeder.TelemetryForwarder,
    as: :flush_summaries
end
