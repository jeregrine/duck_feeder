defmodule DuckFeeder.Storage.S3Test do
  use ExUnit.Case, async: true

  alias DuckFeeder.Storage.S3

  test "put_file returns missing credentials errors before request" do
    config = %{provider: :s3, bucket: "b"}

    assert {:error, :missing_s3_access_key_id} =
             S3.put_file(config, "/tmp/does-not-matter", %{bucket: "b", key: "k"}, [])
  end

  test "head_object returns missing credentials errors before request" do
    config = %{provider: :s3, bucket: "b", access_key_id: "x"}

    assert {:error, :missing_s3_secret_access_key} =
             S3.head_object(config, %{bucket: "b", key: "k"})
  end

  test "delete_object returns missing credentials errors before request" do
    config = %{provider: :s3, bucket: "b", secret_access_key: "y"}

    assert {:error, :missing_s3_access_key_id} =
             S3.delete_object(config, %{bucket: "b", key: "k"})
  end

  test "put_file uses multipart flow when threshold is met" do
    {:ok, calls_agent} = Agent.start_link(fn -> [] end)
    tmp_path = write_temp_file(10 * 1_024 * 1_024 + 123)

    request_fun = fn _req, opts ->
      Agent.update(calls_agent, &[{opts[:method], opts[:params]} | &1])

      case {opts[:method], opts[:params]} do
        {:post, [uploads: ""]} ->
          {:ok,
           Req.Response.new(
             status: 200,
             body:
               "<InitiateMultipartUploadResult><UploadId>upload-1</UploadId></InitiateMultipartUploadResult>"
           )}

        {:put, [partNumber: part_number, uploadId: "upload-1"]} ->
          {:ok,
           Req.Response.new(
             status: 200,
             headers: [{"etag", "\"etag-#{part_number}\""}]
           )}

        {:post, [uploadId: "upload-1"]} ->
          {:ok,
           Req.Response.new(
             status: 200,
             body:
               "<CompleteMultipartUploadResult><ETag>\"final-etag\"</ETag></CompleteMultipartUploadResult>"
           )}

        other ->
          {:error, {:unexpected_request, other}}
      end
    end

    config = %{
      provider: :s3,
      bucket: "bucket",
      access_key_id: "key",
      secret_access_key: "secret",
      endpoint: "https://s3.example.test",
      force_path_style: true,
      adapter_opts: %{
        request_fun: request_fun,
        multipart_threshold: 1024,
        part_size: 5 * 1_024 * 1_024
      }
    }

    assert {:ok, %{etag: "\"final-etag\"", size: size}} =
             S3.put_file(config, tmp_path, %{bucket: "bucket", key: "path/file.parquet"}, [])

    assert size == 10 * 1_024 * 1_024 + 123

    calls = Agent.get(calls_agent, &Enum.reverse/1)

    assert hd(calls) == {:post, [uploads: ""]}
    assert Enum.member?(calls, {:put, [partNumber: 1, uploadId: "upload-1"]})
    assert Enum.member?(calls, {:put, [partNumber: 2, uploadId: "upload-1"]})
    assert Enum.member?(calls, {:put, [partNumber: 3, uploadId: "upload-1"]})
    assert List.last(calls) == {:post, [uploadId: "upload-1"]}

    Agent.stop(calls_agent)
    File.rm(tmp_path)
  end

  test "multipart upload aborts when a part fails" do
    {:ok, calls_agent} = Agent.start_link(fn -> [] end)
    tmp_path = write_temp_file(6 * 1_024 * 1_024)

    request_fun = fn _req, opts ->
      Agent.update(calls_agent, &[{opts[:method], opts[:params]} | &1])

      case {opts[:method], opts[:params]} do
        {:post, [uploads: ""]} ->
          {:ok,
           Req.Response.new(
             status: 200,
             body:
               "<InitiateMultipartUploadResult><UploadId>upload-2</UploadId></InitiateMultipartUploadResult>"
           )}

        {:put, [partNumber: 1, uploadId: "upload-2"]} ->
          {:ok, Req.Response.new(status: 500, body: "part failure")}

        {:delete, [uploadId: "upload-2"]} ->
          {:ok, Req.Response.new(status: 204)}

        other ->
          {:error, {:unexpected_request, other}}
      end
    end

    config = %{
      provider: :s3,
      bucket: "bucket",
      access_key_id: "key",
      secret_access_key: "secret",
      endpoint: "https://s3.example.test",
      force_path_style: true,
      adapter_opts: %{
        request_fun: request_fun,
        multipart_threshold: 1024,
        part_size: 5 * 1_024 * 1_024
      }
    }

    assert {:error, {:s3_upload_part_failed, 500, "part failure"}} =
             S3.put_file(config, tmp_path, %{bucket: "bucket", key: "path/file.parquet"}, [])

    calls = Agent.get(calls_agent, &Enum.reverse/1)
    assert Enum.member?(calls, {:delete, [uploadId: "upload-2"]})

    Agent.stop(calls_agent)
    File.rm(tmp_path)
  end

  defp write_temp_file(size) when is_integer(size) and size > 0 do
    path =
      Path.join(
        System.tmp_dir!(),
        "duck_feeder_s3_test_#{System.unique_integer([:positive])}.bin"
      )

    File.write!(path, :binary.copy(<<97>>, size))
    path
  end
end
