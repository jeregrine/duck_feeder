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
end
