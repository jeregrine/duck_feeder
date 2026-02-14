import Config

pg_user = System.get_env("USER") || "postgres"

integration_s3_storage =
  case {
    System.get_env("DUCK_FEEDER_ITEST_S3_BUCKET"),
    System.get_env("DUCK_FEEDER_ITEST_S3_ENDPOINT"),
    System.get_env("DUCK_FEEDER_ITEST_S3_ACCESS_KEY_ID"),
    System.get_env("DUCK_FEEDER_ITEST_S3_SECRET_ACCESS_KEY")
  } do
    {bucket, endpoint, access_key_id, secret_access_key}
    when is_binary(bucket) and bucket != "" and is_binary(endpoint) and endpoint != "" and
           is_binary(access_key_id) and access_key_id != "" and is_binary(secret_access_key) and
           secret_access_key != "" ->
      %{
        provider: :s3,
        bucket: bucket,
        endpoint: endpoint,
        access_key_id: access_key_id,
        secret_access_key: secret_access_key,
        region: System.get_env("DUCK_FEEDER_ITEST_S3_REGION") || "us-east-1",
        force_path_style:
          (System.get_env("DUCK_FEEDER_ITEST_S3_FORCE_PATH_STYLE") || "true") in [
            "1",
            "true",
            "TRUE",
            "yes",
            "YES"
          ]
      }

    _ ->
      nil
  end

integration_gcs_storage =
  case {
    System.get_env("DUCK_FEEDER_ITEST_GCS_BUCKET"),
    System.get_env("DUCK_FEEDER_ITEST_GCS_TOKEN")
  } do
    {bucket, token}
    when is_binary(bucket) and bucket != "" and is_binary(token) and token != "" ->
      %{
        provider: :gcs,
        bucket: bucket,
        token: token,
        base_url:
          System.get_env("DUCK_FEEDER_ITEST_GCS_BASE_URL") || "https://storage.googleapis.com"
      }

    _ ->
      nil
  end

config :duck_feeder, :integration,
  meta_database_url: "postgres://#{pg_user}@localhost:5432/duckfeeder_meta",
  source_database_url: "postgres://#{pg_user}@localhost:5432/duckfeeder_source",
  s3_storage: integration_s3_storage,
  gcs_storage: integration_gcs_storage
