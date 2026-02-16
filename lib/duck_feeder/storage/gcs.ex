defmodule DuckFeeder.Storage.GCS do
  @moduledoc """
  Google Cloud Storage adapter.

  This adapter uses GCS JSON API and expects an OAuth Bearer token supplied via config.

  Required config keys:
  - `:token` (string) or `:token_fun` (0-arity function returning token)
  - `:bucket` is supplied by object_ref

  Optional config keys:
  - `:base_url` (default: `https://storage.googleapis.com`)
  - `:adapter_opts` map forwarded into Req (`request_opts` key)
  """

  @behaviour DuckFeeder.Storage.Adapter

  alias DuckFeeder.Storage.Adapter

  @default_base_url "https://storage.googleapis.com"

  @impl Adapter
  def put_file(config, local_path, %{bucket: bucket, key: key}, opts) do
    with {:ok, token} <- fetch_token(config),
         {:ok, size} <- file_size(local_path),
         {:ok, response} <-
           req(
             :post,
             gcs_url(config, "/upload/storage/v1/b/#{encode(bucket)}/o"),
             config,
             headers:
               auth_headers(token, [
                 {"content-type", content_type(opts)},
                 {"content-length", Integer.to_string(size)}
               ]),
             params: [uploadType: "media", name: key],
             body: File.stream!(local_path, [], chunk_size(config))
           ),
         :ok <- ensure_success(response),
         {:ok, parsed} <- parse_json_body(response.body) do
      {:ok,
       %{
         etag: Map.get(parsed, "etag"),
         version_id: Map.get(parsed, "generation"),
         size: size
       }}
    end
  end

  @impl Adapter
  def head_object(config, %{bucket: bucket, key: key}) do
    with {:ok, token} <- fetch_token(config),
         {:ok, response} <-
           req(
             :get,
             gcs_url(config, "/storage/v1/b/#{encode(bucket)}/o/#{encode(key)}"),
             config,
             headers: auth_headers(token)
           ),
         :ok <- ensure_success(response),
         {:ok, parsed} <- parse_json_body(response.body) do
      {:ok, parsed}
    end
  end

  @impl Adapter
  def delete_object(config, %{bucket: bucket, key: key}) do
    with {:ok, token} <- fetch_token(config),
         {:ok, response} <-
           req(
             :delete,
             gcs_url(config, "/storage/v1/b/#{encode(bucket)}/o/#{encode(key)}"),
             config,
             headers: auth_headers(token)
           ) do
      case response.status do
        status when status in 200..299 -> :ok
        404 -> :ok
        _ -> {:error, {:gcs_delete_failed, response.status, response.body}}
      end
    end
  end

  defp fetch_token(%{token: token}) when is_binary(token) and token != "", do: {:ok, token}

  defp fetch_token(%{token_fun: token_fun}) when is_function(token_fun, 0) do
    case token_fun.() do
      token when is_binary(token) and token != "" -> {:ok, token}
      other -> {:error, {:invalid_gcs_token, other}}
    end
  rescue
    exception -> {:error, {:gcs_token_error, Exception.message(exception)}}
  end

  defp fetch_token(_), do: {:error, :missing_gcs_token}

  defp req(method, url, config, opts) do
    req_opts =
      config
      |> request_opts()
      |> Keyword.merge(method: method, url: url, decode_body: false, retry: false)
      |> Keyword.merge(opts)

    request_with_retry(req_opts, retry_config(config), request_fun(config), 1)
  end

  defp request_with_retry(req_opts, retry_config, request_fun, attempt)
       when is_list(req_opts) and is_map(retry_config) and is_function(request_fun, 1) and
              is_integer(attempt) do
    case request_fun.(req_opts) do
      {:ok, %Req.Response{} = response} ->
        if retryable_status?(response.status, retry_config) and
             attempt < retry_config.max_attempts do
          sleep_before_retry(retry_config, attempt)
          request_with_retry(req_opts, retry_config, request_fun, attempt + 1)
        else
          {:ok, response}
        end

      {:error, reason} ->
        if attempt < retry_config.max_attempts do
          sleep_before_retry(retry_config, attempt)
          request_with_retry(req_opts, retry_config, request_fun, attempt + 1)
        else
          {:error, {:gcs_http_failed, reason}}
        end

      other ->
        {:error, {:gcs_http_failed, {:unexpected_request_result, other}}}
    end
  end

  defp request_opts(config) do
    adapter_opts =
      config
      |> Map.get(:adapter_opts, %{})
      |> Map.new()

    timeout_ms = timeout_ms(adapter_opts)

    adapter_opts
    |> Map.get(:request_opts, [])
    |> Keyword.put_new(:receive_timeout, timeout_ms)
    |> put_connect_timeout(timeout_ms)
  end

  defp request_fun(config) do
    adapter_opts =
      config
      |> Map.get(:adapter_opts, %{})
      |> Map.new()

    case Map.get(adapter_opts, :request_fun) do
      fun when is_function(fun, 1) -> fun
      _ -> &Req.request/1
    end
  end

  defp retry_config(config) do
    adapter_opts =
      config
      |> Map.get(:adapter_opts, %{})
      |> Map.new()

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

  defp retryable_status?(status, %{retryable_statuses: statuses}) when is_integer(status),
    do: status in statuses

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

  defp timeout_ms(adapter_opts),
    do: normalize_non_neg_integer(Map.get(adapter_opts, :timeout_ms, 30_000), 30_000)

  defp put_connect_timeout(request_opts, timeout_ms) when is_list(request_opts) do
    connect_options =
      request_opts
      |> Keyword.get(:connect_options, [])
      |> List.wrap()
      |> Keyword.put_new(:timeout, timeout_ms)

    Keyword.put(request_opts, :connect_options, connect_options)
  end

  defp chunk_size(config) do
    config
    |> Map.get(:adapter_opts, %{})
    |> Map.new()
    |> Map.get(:chunk_size, 1_024 * 1_024)
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

  defp auth_headers(token, extra \\ []), do: [{"authorization", "Bearer #{token}"} | extra]

  defp gcs_url(config, path) do
    base = Map.get(config, :base_url, @default_base_url)
    String.trim_trailing(base, "/") <> path
  end

  defp encode(value), do: URI.encode_www_form(value)
  defp content_type(opts), do: Keyword.get(opts, :content_type, "application/octet-stream")

  defp ensure_success(%Req.Response{status: status}) when status in 200..299, do: :ok

  defp ensure_success(%Req.Response{status: status, body: body}) do
    {:error, {:gcs_request_failed, status, body}}
  end

  defp parse_json_body(nil), do: {:ok, %{}}
  defp parse_json_body(""), do: {:ok, %{}}

  defp parse_json_body(body) when is_binary(body) do
    case JSON.decode(body) do
      {:ok, parsed} when is_map(parsed) -> {:ok, parsed}
      {:ok, other} -> {:error, {:unexpected_gcs_body, other}}
      {:error, reason} -> {:error, {:invalid_gcs_json, reason}}
    end
  end

  defp parse_json_body(other), do: {:error, {:unexpected_gcs_body, other}}

  defp file_size(path) do
    case File.stat(path) do
      {:ok, %{size: size}} -> {:ok, size}
      {:error, reason} -> {:error, reason}
    end
  end
end
