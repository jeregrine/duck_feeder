defmodule DuckFeeder.Storage.S3 do
  @moduledoc """
  S3/S3-compatible adapter.

  Talks to S3 directly over HTTP using Req + AWS SigV4.

  Supports both:
  - single PUT uploads
  - multipart uploads for larger files

  Multipart adapter options:
  - `:multipart` (`true | false`) - force on/off
  - `:multipart_threshold` - auto-multipart threshold in bytes (default: 64 MiB)
  - `:part_size` - multipart chunk size in bytes (default: 8 MiB, minimum 5 MiB)
  - `:chunk_size` - stream chunk size for single PUT (default: 8 MiB)
  - `:request_opts` - extra Req request options
  - `:request_fun` - request function override for tests (`fn req, opts -> ... end`)
  """

  @behaviour DuckFeeder.Storage.Adapter

  alias DuckFeeder.Storage.Adapter

  @default_region "us-east-1"
  @default_chunk_size 8 * 1_024 * 1_024
  @default_multipart_threshold 64 * 1_024 * 1_024
  @min_part_size 5 * 1_024 * 1_024

  @impl Adapter
  def put_file(config, local_path, %{bucket: bucket, key: key}, opts) do
    with {:ok, credentials} <- credentials(config),
         {:ok, size} <- file_size(local_path) do
      if multipart_upload?(size, config, opts) do
        put_file_multipart(config, credentials, local_path, bucket, key, size, opts)
      else
        put_file_single(config, credentials, local_path, bucket, key, size, opts)
      end
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
             call_opts: []
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
             call_opts: []
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

  defp put_file_single(config, credentials, local_path, bucket, key, size, opts) do
    with {:ok, response} <-
           request(config, credentials,
             method: :put,
             bucket: bucket,
             key: key,
             headers: put_headers(size, opts),
             body: File.stream!(local_path, [], chunk_size(config, opts)),
             call_opts: opts
           ),
         :ok <- ensure_success(response, :s3_put_failed) do
      {:ok,
       %{
         etag: header(response, "etag"),
         version_id: header(response, "x-amz-version-id"),
         size: size
       }}
    end
  end

  defp put_file_multipart(config, credentials, local_path, bucket, key, size, opts) do
    with {:ok, upload_id} <- initiate_multipart_upload(config, credentials, bucket, key, opts) do
      case upload_and_complete_multipart(
             config,
             credentials,
             local_path,
             bucket,
             key,
             upload_id,
             size,
             opts
           ) do
        {:ok, result} ->
          {:ok, result}

        {:error, reason} ->
          _ = abort_multipart_upload(config, credentials, bucket, key, upload_id, opts)
          {:error, reason}

        other ->
          _ = abort_multipart_upload(config, credentials, bucket, key, upload_id, opts)
          {:error, {:s3_multipart_failed, other}}
      end
    end
  end

  defp upload_and_complete_multipart(
         config,
         credentials,
         local_path,
         bucket,
         key,
         upload_id,
         size,
         opts
       ) do
    with {:ok, etags} <-
           upload_parts(config, credentials, local_path, bucket, key, upload_id, opts),
         {:ok, response} <-
           complete_multipart_upload(config, credentials, bucket, key, upload_id, etags, opts),
         :ok <- ensure_success(response, :s3_complete_multipart_failed) do
      {:ok,
       %{
         etag: multipart_etag(response, etags),
         version_id: header(response, "x-amz-version-id"),
         size: size
       }}
    end
  end

  defp initiate_multipart_upload(config, credentials, bucket, key, opts) do
    with {:ok, response} <-
           request(config, credentials,
             method: :post,
             bucket: bucket,
             key: key,
             params: [uploads: ""],
             headers: [{"content-type", "application/octet-stream"}],
             body: "",
             call_opts: opts
           ),
         :ok <- ensure_success(response, :s3_multipart_init_failed),
         {:ok, upload_id} <- parse_upload_id(response.body) do
      {:ok, upload_id}
    end
  end

  defp upload_parts(config, credentials, local_path, bucket, key, upload_id, opts) do
    local_path
    |> File.stream!([], part_size(config, opts))
    |> Enum.reduce_while({:ok, {1, []}}, fn chunk, {:ok, {part_number, rev_etags}} ->
      with {:ok, response} <-
             request(config, credentials,
               method: :put,
               bucket: bucket,
               key: key,
               params: [partNumber: part_number, uploadId: upload_id],
               headers: put_part_headers(chunk),
               body: chunk,
               call_opts: opts
             ),
           :ok <- ensure_success(response, :s3_upload_part_failed),
           etag when is_binary(etag) <- header(response, "etag") do
        {:cont, {:ok, {part_number + 1, [etag | rev_etags]}}}
      else
        nil ->
          {:halt, {:error, {:s3_upload_part_failed, :missing_etag, part_number}}}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
    |> normalize_uploaded_etags()
  end

  defp complete_multipart_upload(config, credentials, bucket, key, upload_id, etags, opts) do
    body = complete_multipart_upload_xml(etags)

    request(config, credentials,
      method: :post,
      bucket: bucket,
      key: key,
      params: [uploadId: upload_id],
      headers: [
        {"content-type", "application/xml"},
        {"content-length", Integer.to_string(byte_size(body))}
      ],
      body: body,
      call_opts: opts
    )
  end

  defp abort_multipart_upload(config, credentials, bucket, key, upload_id, opts) do
    case request(config, credentials,
           method: :delete,
           bucket: bucket,
           key: key,
           params: [uploadId: upload_id],
           body: "",
           call_opts: opts
         ) do
      {:ok, %Req.Response{status: status}} when status in [200, 204, 404] -> :ok
      {:ok, _response} -> :ok
      {:error, _reason} -> :ok
    end
  end

  defp request(config, credentials, opts) do
    method = Keyword.fetch!(opts, :method)
    bucket = Keyword.fetch!(opts, :bucket)
    key = Keyword.fetch!(opts, :key)

    headers = Keyword.get(opts, :headers, [])
    body = Keyword.get(opts, :body, "")
    params = Keyword.get(opts, :params, [])
    call_opts = Keyword.get(opts, :call_opts, [])

    req =
      Req.new(
        base_url: base_url(config, bucket),
        aws_sigv4: aws_sigv4_opts(config, credentials)
      )
      |> Req.merge(request_opts(config, call_opts))

    request_fun = request_fun(config, call_opts)
    retry_config = retry_config(config, call_opts)

    request_with_retry(
      request_fun,
      req,
      [
        method: method,
        url: "/" <> encode_key(key),
        headers: headers,
        body: body,
        params: params,
        decode_body: false,
        retry: false
      ],
      retry_config,
      1
    )
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
    adapter_opts = merged_adapter_opts(config, call_opts)
    timeout_ms = timeout_ms(adapter_opts)

    adapter_opts
    |> Map.get(:request_opts, [])
    |> Keyword.put_new(:receive_timeout, timeout_ms)
    |> put_connect_timeout(timeout_ms)
  end

  defp request_fun(config, call_opts) do
    case Map.get(merged_adapter_opts(config, call_opts), :request_fun) do
      fun when is_function(fun, 2) -> fun
      _ -> &Req.request/2
    end
  end

  defp request_with_retry(request_fun, req, request_args, retry_config, attempt)
       when is_function(request_fun, 2) and is_list(request_args) and is_map(retry_config) do
    case request_fun.(req, request_args) do
      {:ok, %Req.Response{} = response} ->
        if retryable_status?(response.status, retry_config) and
             attempt < retry_config.max_attempts do
          sleep_before_retry(retry_config, attempt)
          request_with_retry(request_fun, req, request_args, retry_config, attempt + 1)
        else
          {:ok, response}
        end

      {:error, reason} ->
        if attempt < retry_config.max_attempts do
          sleep_before_retry(retry_config, attempt)
          request_with_retry(request_fun, req, request_args, retry_config, attempt + 1)
        else
          {:error, {:s3_http_failed, reason}}
        end

      other ->
        {:error, {:s3_http_failed, {:unexpected_request_result, other}}}
    end
  end

  defp retry_config(config, call_opts) do
    adapter_opts = merged_adapter_opts(config, call_opts)

    %{
      max_attempts: normalize_max_attempts(Map.get(adapter_opts, :retry_max_attempts, 3)),
      base_delay_ms:
        normalize_non_neg_integer(Map.get(adapter_opts, :retry_base_delay_ms, 100), 100),
      max_delay_ms:
        normalize_non_neg_integer(Map.get(adapter_opts, :retry_max_delay_ms, 2_000), 2_000),
      jitter_ms: normalize_non_neg_integer(Map.get(adapter_opts, :retry_jitter_ms, 50), 50),
      retryable_statuses:
        normalize_retryable_statuses(
          Map.get(adapter_opts, :retryable_statuses, [408, 425, 429, 500, 502, 503, 504])
        )
    }
  end

  defp retryable_status?(status, %{retryable_statuses: statuses}) when is_integer(status) do
    status in statuses
  end

  defp retryable_status?(_status, _retry_config), do: false

  defp sleep_before_retry(retry_config, attempt)
       when is_map(retry_config) and is_integer(attempt) and attempt >= 1 do
    exponential =
      retry_config.base_delay_ms
      |> Kernel.*(trunc(:math.pow(2, attempt - 1)))
      |> min(retry_config.max_delay_ms)

    jitter =
      if retry_config.jitter_ms > 0 do
        :rand.uniform(retry_config.jitter_ms * 2 + 1) - (retry_config.jitter_ms + 1)
      else
        0
      end

    Process.sleep(max(exponential + jitter, 0))
  end

  defp timeout_ms(adapter_opts) do
    normalize_non_neg_integer(Map.get(adapter_opts, :timeout_ms, 30_000), 30_000)
  end

  defp put_connect_timeout(request_opts, timeout_ms) when is_list(request_opts) do
    connect_options =
      request_opts
      |> Keyword.get(:connect_options, [])
      |> List.wrap()
      |> Keyword.put_new(:timeout, timeout_ms)

    Keyword.put(request_opts, :connect_options, connect_options)
  end

  defp normalize_max_attempts(value) when is_integer(value) and value > 0, do: value
  defp normalize_max_attempts(_value), do: 3

  defp normalize_non_neg_integer(value, _default) when is_integer(value) and value >= 0, do: value
  defp normalize_non_neg_integer(_value, default), do: default

  defp normalize_retryable_statuses(statuses) when is_list(statuses) do
    statuses
    |> Enum.filter(&(is_integer(&1) and &1 >= 100 and &1 <= 599))
    |> Enum.uniq()
    |> case do
      [] -> [408, 425, 429, 500, 502, 503, 504]
      values -> values
    end
  end

  defp normalize_retryable_statuses(_other), do: [408, 425, 429, 500, 502, 503, 504]

  defp multipart_upload?(size, _config, _opts) when size <= 0, do: false

  defp multipart_upload?(size, config, opts) do
    adapter_opts = merged_adapter_opts(config, opts)

    case Map.get(adapter_opts, :multipart) do
      true -> true
      false -> false
      _ -> size >= multipart_threshold(adapter_opts)
    end
  end

  defp multipart_threshold(adapter_opts),
    do: Map.get(adapter_opts, :multipart_threshold, @default_multipart_threshold)

  defp part_size(config, opts) do
    config
    |> merged_adapter_opts(opts)
    |> Map.get(:part_size, @default_chunk_size)
    |> max(@min_part_size)
  end

  defp chunk_size(config, opts) do
    config
    |> merged_adapter_opts(opts)
    |> Map.get(:chunk_size, @default_chunk_size)
  end

  defp merged_adapter_opts(config, call_opts) do
    config_opts =
      config
      |> Map.get(:adapter_opts, %{})
      |> Map.new()

    call_opts_map =
      call_opts
      |> Keyword.get(:adapter_opts, %{})
      |> Map.new()

    Map.merge(config_opts, call_opts_map)
  end

  defp put_headers(size, opts) do
    content_type = Keyword.get(opts, :content_type, "application/octet-stream")

    [
      {"content-type", content_type},
      {"content-length", Integer.to_string(size)}
    ]
  end

  defp put_part_headers(chunk) do
    [
      {"content-type", "application/octet-stream"},
      {"content-length", Integer.to_string(byte_size(chunk))}
    ]
  end

  defp complete_multipart_upload_xml(etags) do
    parts_xml =
      etags
      |> Enum.with_index(1)
      |> Enum.map_join(fn {etag, part_number} ->
        "<Part><PartNumber>#{part_number}</PartNumber><ETag>#{xml_escape(etag)}</ETag></Part>"
      end)

    "<CompleteMultipartUpload>#{parts_xml}</CompleteMultipartUpload>"
  end

  defp normalize_uploaded_etags({:ok, {_next_part_number, rev_etags}}) when rev_etags != [],
    do: {:ok, Enum.reverse(rev_etags)}

  defp normalize_uploaded_etags({:ok, {_next_part_number, []}}),
    do: {:error, {:s3_upload_part_failed, :no_parts_uploaded}}

  defp normalize_uploaded_etags({:error, _reason} = error), do: error

  defp parse_upload_id(body) when is_binary(body) do
    case parse_xml_tag(body, "UploadId") do
      nil -> {:error, {:invalid_multipart_init_response, body}}
      upload_id -> {:ok, upload_id}
    end
  end

  defp parse_upload_id(other), do: {:error, {:invalid_multipart_init_response, other}}

  defp multipart_etag(response, etags) do
    header(response, "etag") || parse_xml_tag(response.body, "ETag") || List.last(etags)
  end

  defp parse_xml_tag(body, tag) when is_binary(body) and is_binary(tag) do
    regex = ~r/<#{tag}>([^<]+)<\/#{tag}>/

    case Regex.run(regex, body, capture: :all_but_first) do
      [value] -> xml_unescape(String.trim(value))
      _ -> nil
    end
  end

  defp parse_xml_tag(_body, _tag), do: nil

  defp xml_escape(value) do
    value
    |> String.replace("&", "&amp;")
    |> String.replace("<", "&lt;")
    |> String.replace(">", "&gt;")
    |> String.replace("\"", "&quot;")
    |> String.replace("'", "&apos;")
  end

  defp xml_unescape(value) do
    value
    |> String.replace("&quot;", "\"")
    |> String.replace("&apos;", "'")
    |> String.replace("&lt;", "<")
    |> String.replace("&gt;", ">")
    |> String.replace("&amp;", "&")
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
