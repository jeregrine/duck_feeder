import Config

pg_user = System.get_env("USER") || "postgres"

config :duck_feeder, :integration,
  meta_database_url: "postgres://#{pg_user}@localhost:5432/duckfeeder_meta",
  source_database_url: "postgres://#{pg_user}@localhost:5432/duckfeeder_source"
