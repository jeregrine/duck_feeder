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
         {:ok, body} <- File.read(local_path),
         {:ok, response} <-
           req(
             :post,
             gcs_url(config, "/upload/storage/v1/b/#{encode(bucket)}/o"),
             config,
             headers: auth_headers(token, [{"content-type", content_type(opts)}]),
             params: [uploadType: "media", name: key],
             body: body
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

    case Req.request(req_opts) do
      {:ok, response} -> {:ok, response}
      {:error, reason} -> {:error, {:gcs_http_failed, reason}}
    end
  end

  defp request_opts(config) do
    config
    |> Map.get(:adapter_opts, %{})
    |> Map.new()
    |> Map.get(:request_opts, [])
  end

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
