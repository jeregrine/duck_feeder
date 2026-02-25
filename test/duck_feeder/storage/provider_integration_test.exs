defmodule DuckFeeder.Storage.ProviderIntegrationTest do
  use ExUnit.Case, async: false

  alias DuckFeeder.Storage

  @moduletag :provider_integration

  @integration_config Application.compile_env(:duck_feeder, :integration, [])
  @s3_storage Keyword.get(@integration_config, :s3_storage)
  @gcs_storage Keyword.get(@integration_config, :gcs_storage)
  @s3_skip if(is_map(@s3_storage), do: false, else: "configure DUCK_FEEDER_ITEST_S3_* env vars")
  @gcs_skip if(is_map(@gcs_storage),
              do: false,
              else: "configure DUCK_FEEDER_ITEST_GCS_* env vars"
            )

  @tag skip: @s3_skip
  test "s3 provider roundtrip put/head/delete" do
    key = "duck_feeder/provider_itest/s3_#{System.unique_integer([:positive])}.txt"
    path = write_temp_file("s3-provider-itest")

    assert {:ok, %{size: size}} = Storage.put_file(@s3_storage, path, key)
    assert size > 0

    assert {:ok, head} = Storage.head_object(@s3_storage, key)
    assert is_map(head)

    assert :ok = Storage.delete_object(@s3_storage, key)
    assert :ok = assert_s3_deleted(@s3_storage, key, 10)

    File.rm(path)
  end

  @tag skip: @gcs_skip
  test "gcs provider roundtrip put/head/delete" do
    key = "duck_feeder/provider_itest/gcs_#{System.unique_integer([:positive])}.txt"
    path = write_temp_file("gcs-provider-itest")

    assert {:ok, %{size: size}} = Storage.put_file(@gcs_storage, path, key)
    assert size > 0

    assert {:ok, head} = Storage.head_object(@gcs_storage, key)
    assert head["name"] == key

    assert :ok = Storage.delete_object(@gcs_storage, key)
    assert :ok = assert_gcs_deleted(@gcs_storage, key, 10)

    File.rm(path)
  end

  defp assert_s3_deleted(_config, _key, 0), do: {:error, :s3_delete_visibility_timeout}

  defp assert_s3_deleted(config, key, attempts_left) do
    case Storage.head_object(config, key) do
      {:error, :not_found} ->
        :ok

      {:ok, _head} ->
        Process.sleep(100)
        assert_s3_deleted(config, key, attempts_left - 1)

      {:error, _reason} = error ->
        error
    end
  end

  defp assert_gcs_deleted(_config, _key, 0), do: {:error, :gcs_delete_visibility_timeout}

  defp assert_gcs_deleted(config, key, attempts_left) do
    case Storage.head_object(config, key) do
      {:error, {:gcs_request_failed, 404, _}} ->
        :ok

      {:ok, _head} ->
        Process.sleep(100)
        assert_gcs_deleted(config, key, attempts_left - 1)

      {:error, _reason} = error ->
        error
    end
  end

  defp write_temp_file(contents) when is_binary(contents) do
    path =
      Path.join(
        System.tmp_dir!(),
        "duck_feeder_provider_storage_test_#{System.unique_integer([:positive])}.txt"
      )

    File.write!(path, contents)
    path
  end
end
