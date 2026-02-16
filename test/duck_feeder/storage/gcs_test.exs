defmodule DuckFeeder.Storage.GCSTest do
  use ExUnit.Case, async: true

  alias DuckFeeder.Storage.GCS

  test "put_file retries transient failures and streams file body" do
    {:ok, attempts} = Agent.start_link(fn -> 0 end)

    request_fun = fn req_opts ->
      attempt = Agent.get_and_update(attempts, fn current -> {current + 1, current + 1} end)

      send(self(), {:gcs_request_body, req_opts[:body]})

      if attempt < 3 do
        {:ok, Req.Response.new(status: 500, body: "transient")}
      else
        {:ok, Req.Response.new(status: 200, body: ~s({"etag":"etag-1","generation":"42"}))}
      end
    end

    config = %{
      provider: :gcs,
      bucket: "bucket",
      token: "token",
      adapter_opts: %{
        request_fun: request_fun,
        retry_max_attempts: 3,
        retry_base_delay_ms: 0,
        retry_jitter_ms: 0,
        retry_max_delay_ms: 0
      }
    }

    tmp_path = write_temp_file("duck")

    assert {:ok, %{etag: "etag-1", version_id: "42", size: 4}} =
             GCS.put_file(config, tmp_path, %{bucket: "bucket", key: "path/file.parquet"}, [])

    assert Agent.get(attempts, & &1) == 3

    assert_receive {:gcs_request_body, %File.Stream{}}, 200

    Agent.stop(attempts)
    File.rm(tmp_path)
  end

  test "put_file returns error when retries are exhausted" do
    {:ok, attempts} = Agent.start_link(fn -> 0 end)

    request_fun = fn _req_opts ->
      _ = Agent.get_and_update(attempts, fn current -> {current + 1, current + 1} end)
      {:ok, Req.Response.new(status: 500, body: "nope")}
    end

    config = %{
      provider: :gcs,
      bucket: "bucket",
      token: "token",
      adapter_opts: %{
        request_fun: request_fun,
        retry_max_attempts: 2,
        retry_base_delay_ms: 0,
        retry_jitter_ms: 0,
        retry_max_delay_ms: 0
      }
    }

    tmp_path = write_temp_file("duck")

    assert {:error, {:gcs_request_failed, 500, "nope"}} =
             GCS.put_file(config, tmp_path, %{bucket: "bucket", key: "path/file.parquet"}, [])

    assert Agent.get(attempts, & &1) == 2

    Agent.stop(attempts)
    File.rm(tmp_path)
  end

  defp write_temp_file(contents) when is_binary(contents) do
    path =
      Path.join(
        System.tmp_dir!(),
        "duck_feeder_gcs_test_#{System.unique_integer([:positive])}.bin"
      )

    File.write!(path, contents)
    path
  end
end
