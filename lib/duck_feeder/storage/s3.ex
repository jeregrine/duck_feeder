defmodule DuckFeeder.Storage.S3 do
  @moduledoc """
  S3/S3-compatible adapter.

  Talks to S3 directly over HTTP using Req + AWS SigV4 (no ExAws).
  """

  @behaviour DuckFeeder.Storage.Adapter

  alias DuckFeeder.Storage.Adapter

  @default_region "us-east-1"
  @default_chunk_size 8 * 1_024 * 1_024

  @impl Adapter
  def put_file(config, local_path, %{bucket: bucket, key: key}, opts) do
    with {:ok, credentials} <- credentials(config),
         {:ok, size} <- file_size(local_path),
         {:ok, response} <-
           request(config, credentials,
             method: :put,
             bucket: bucket,
             key: key,
             headers: put_headers(size, opts),
             body: File.stream!(local_path, [], chunk_size(config, opts)),
             opts: request_opts(config, opts)
           ),
         :ok <- ensure_success(response, :s3_put_failed) do
      {:ok,
       %{
         etag: header(response, "etag"),
         version_id: header(response, "x-amz-version-id"),
         size: size
       }}
    end
  rescue
    exception ->
      {:error, {:s3_put_failed, Exception.message(exception)}}
  end

  @impl Adapter
  def head_object(config, %{bucket: bucket, key: key}) do
    with {:ok, credentials} <- credentials(config),
         {:ok, response} <-
           request(config, credentials,
             method: :head,
             bucket: bucket,
             key: key,
             opts: request_opts(config, [])
           ) do
      case response.status do
        200 ->
          {:ok,
           %{
             etag: header(response, "etag"),
             content_length: header(response, "content-length"),
             version_id: header(response, "x-amz-version-id")
           }}

        404 ->
          {:error, :not_found}

        status ->
          {:error, {:s3_head_failed, status, response.body}}
      end
    end
  rescue
    exception ->
      {:error, {:s3_head_failed, Exception.message(exception)}}
  end

  @impl Adapter
  def delete_object(config, %{bucket: bucket, key: key}) do
    with {:ok, credentials} <- credentials(config),
         {:ok, response} <-
           request(config, credentials,
             method: :delete,
             bucket: bucket,
             key: key,
             opts: request_opts(config, [])
           ) do
      case response.status do
        status when status in 200..299 -> :ok
        404 -> :ok
        status -> {:error, {:s3_delete_failed, status, response.body}}
      end
    end
  rescue
    exception ->
      {:error, {:s3_delete_failed, Exception.message(exception)}}
  end

  defp request(config, credentials, opts) do
    method = Keyword.fetch!(opts, :method)
    bucket = Keyword.fetch!(opts, :bucket)
    key = Keyword.fetch!(opts, :key)
    headers = Keyword.get(opts, :headers, [])
    body = Keyword.get(opts, :body, "")
    req_opts = Keyword.get(opts, :opts, [])

    req =
      Req.new(
        base_url: base_url(config, bucket),
        aws_sigv4: aws_sigv4_opts(config, credentials)
      )
      |> Req.merge(req_opts)

    case Req.request(req,
           method: method,
           url: "/" <> encode_key(key),
           headers: headers,
           body: body,
           decode_body: false,
           retry: false
         ) do
      {:ok, response} -> {:ok, response}
      {:error, reason} -> {:error, {:s3_http_failed, reason}}
    end
  end

  defp credentials(config) do
    access_key_id = config[:access_key_id]
    secret_access_key = config[:secret_access_key]

    cond do
      is_nil(access_key_id) or access_key_id == "" ->
        {:error, :missing_s3_access_key_id}

      is_nil(secret_access_key) or secret_access_key == "" ->
        {:error, :missing_s3_secret_access_key}

      true ->
        {:ok,
         %{
           access_key_id: access_key_id,
           secret_access_key: secret_access_key,
           token: config[:session_token] || config[:token]
         }}
    end
  end

  defp aws_sigv4_opts(config, credentials) do
    [
      access_key_id: credentials.access_key_id,
      secret_access_key: credentials.secret_access_key,
      token: credentials.token,
      region: region(config),
      service: :s3
    ]
  end

  defp request_opts(config, call_opts) do
    config_opts =
      config
      |> Map.get(:adapter_opts, %{})
      |> Map.new()

    call_adapter_opts =
      call_opts
      |> Keyword.get(:adapter_opts, %{})
      |> Map.new()

    config_opts
    |> Map.merge(call_adapter_opts)
    |> Map.get(:request_opts, [])
  end

  defp put_headers(size, opts) do
    content_type = Keyword.get(opts, :content_type, "application/octet-stream")

    [
      {"content-type", content_type},
      {"content-length", Integer.to_string(size)}
    ]
  end

  defp chunk_size(config, opts) do
    config_size =
      config
      |> Map.get(:adapter_opts, %{})
      |> Map.new()
      |> Map.get(:chunk_size)

    call_size =
      opts
      |> Keyword.get(:adapter_opts, %{})
      |> Map.new()
      |> Map.get(:chunk_size)

    call_size || config_size || @default_chunk_size
  end

  defp region(config), do: config[:region] || @default_region

  defp base_url(config, bucket) do
    {scheme, host, port, endpoint_path} = endpoint_parts(config)
    authority = host <> port_suffix(port)

    if config[:force_path_style] do
      build_url(scheme, authority, [endpoint_path, bucket])
    else
      build_url(scheme, bucket <> "." <> authority, [endpoint_path])
    end
  end

  defp endpoint_parts(config) do
    endpoint = config[:endpoint] || "https://s3.#{region(config)}.amazonaws.com"
    uri = URI.parse(endpoint)

    scheme = uri.scheme || "https"
    host = uri.host || "s3.#{region(config)}.amazonaws.com"
    port = uri.port
    path = normalize_path(uri.path)

    {scheme, host, port, path}
  end

  defp normalize_path(nil), do: ""

  defp normalize_path(path) do
    path
    |> String.trim()
    |> String.trim("/")
  end

  defp build_url(scheme, authority, path_segments) do
    path =
      path_segments
      |> Enum.reject(&(&1 in [nil, ""]))
      |> Enum.join("/")

    if path == "" do
      "#{scheme}://#{authority}"
    else
      "#{scheme}://#{authority}/#{path}"
    end
  end

  defp port_suffix(nil), do: ""
  defp port_suffix(port), do: ":#{port}"

  defp ensure_success(%Req.Response{status: status}, _tag) when status in 200..299, do: :ok

  defp ensure_success(%Req.Response{status: status, body: body}, tag) do
    {:error, {tag, status, body}}
  end

  defp header(response, name) do
    downcased = String.downcase(name)

    response
    |> Req.get_headers_list()
    |> Enum.find_value(fn
      {key, value} when is_binary(key) and is_binary(value) ->
        if String.downcase(key) == downcased, do: value

      _ ->
        nil
    end)
  end

  defp encode_key(key) do
    key
    |> String.split("/")
    |> Enum.map_join("/", fn segment -> URI.encode(segment, &URI.char_unreserved?/1) end)
  end

  defp file_size(path) do
    case File.stat(path) do
      {:ok, %{size: size}} -> {:ok, size}
      {:error, reason} -> {:error, reason}
    end
  end
end
