defmodule DuckFeeder.CDC.ConnectionOptions do
  @moduledoc """
  Resolves Postgrex connection options for CDC replication connections.
  """

  @type source :: %{optional(:connection_info) => map()}

  @spec resolve(source(), keyword()) :: {:ok, keyword()} | {:error, term()}
  def resolve(source, opts \\ []) when is_map(source) do
    case Keyword.get(opts, :connection_opts) do
      connection_opts when is_list(connection_opts) ->
        {:ok, connection_opts}

      nil ->
        source
        |> Map.get(:connection_info, %{})
        |> from_connection_info()
        |> merge_overrides(Keyword.get(opts, :connection_overrides, []))
    end
  end

  @spec parse_url(String.t()) :: {:ok, keyword()} | {:error, term()}
  def parse_url(url) when is_binary(url) do
    uri = URI.parse(url)

    with :ok <- validate_scheme(uri.scheme),
         :ok <- validate_host(uri.host),
         {:ok, database} <- parse_database(uri.path) do
      query_map = URI.decode_query(uri.query || "")
      {username, password} = parse_userinfo(uri.userinfo)

      opts =
        []
        |> put_opt(:hostname, uri.host)
        |> put_opt(:port, uri.port || 5432)
        |> put_opt(:database, database)
        |> put_opt(:username, username)
        |> put_opt(:password, password)
        |> put_opt(:ssl, parse_ssl(query_map))

      {:ok, opts}
    end
  end

  defp from_connection_info(info) when is_map(info) do
    with {:ok, url} <- fetch_url(info) do
      parse_url(url)
    else
      {:error, :missing_url} -> from_fields(info)
      {:error, _} = error -> error
    end
  end

  defp from_connection_info(_), do: {:error, :invalid_connection_info}

  defp from_fields(info) do
    host = fetch(info, :host) || fetch(info, :hostname)
    database = fetch(info, :database)

    if blank?(host) or blank?(database) do
      {:error, :missing_connection_info}
    else
      port = fetch(info, :port) |> parse_optional_integer(5432)

      opts =
        []
        |> put_opt(:hostname, host)
        |> put_opt(:port, port)
        |> put_opt(:database, database)
        |> put_opt(:username, fetch(info, :username) || fetch(info, :user))
        |> put_opt(:password, fetch(info, :password))
        |> put_opt(:ssl, parse_ssl(info))

      {:ok, opts}
    end
  end

  defp fetch_url(info) do
    url =
      fetch(info, :postgres_url) || fetch(info, :dsn) || fetch(info, :database_url) ||
        fetch(info, :url)

    if blank?(url), do: {:error, :missing_url}, else: {:ok, url}
  end

  defp parse_userinfo(nil), do: {nil, nil}

  defp parse_userinfo(userinfo) when is_binary(userinfo) do
    case String.split(userinfo, ":", parts: 2) do
      [username, password] -> {URI.decode_www_form(username), URI.decode_www_form(password)}
      [username] -> {URI.decode_www_form(username), nil}
    end
  end

  defp parse_database(path) when is_binary(path) do
    db = String.trim_leading(path, "/")

    if db == "" do
      {:error, :missing_database}
    else
      {:ok, db}
    end
  end

  defp parse_database(_), do: {:error, :missing_database}

  defp validate_scheme(scheme) when scheme in ["postgres", "postgresql"], do: :ok
  defp validate_scheme(_), do: {:error, :invalid_scheme}

  defp validate_host(host) when is_binary(host) and host != "", do: :ok
  defp validate_host(_), do: {:error, :missing_host}

  defp parse_ssl(map) when is_map(map) do
    mode = fetch(map, :sslmode)
    ssl = fetch(map, :ssl)

    cond do
      mode in ["disable"] -> false
      mode in ["require", "verify-ca", "verify-full"] -> true
      ssl in [true, "true", "1"] -> true
      ssl in [false, "false", "0"] -> false
      true -> nil
    end
  end

  defp merge_overrides({:error, _} = error, _overrides), do: error

  defp merge_overrides({:ok, opts}, overrides) do
    overrides = normalize_overrides(overrides)
    {:ok, Keyword.merge(opts, overrides)}
  end

  defp normalize_overrides(overrides) when is_list(overrides), do: overrides

  defp normalize_overrides(overrides) when is_map(overrides) do
    Enum.map(overrides, fn {k, v} -> {normalize_key(k), v} end)
  end

  defp normalize_overrides(_), do: []

  defp normalize_key(k) when is_atom(k), do: k
  defp normalize_key(k) when is_binary(k), do: String.to_atom(k)
  defp normalize_key(k), do: k

  defp parse_optional_integer(nil, default), do: default
  defp parse_optional_integer("", default), do: default
  defp parse_optional_integer(value, _default) when is_integer(value), do: value

  defp parse_optional_integer(value, default) when is_binary(value) do
    case Integer.parse(value) do
      {int, ""} -> int
      _ -> default
    end
  end

  defp parse_optional_integer(_value, default), do: default

  defp put_opt(opts, _key, nil), do: opts
  defp put_opt(opts, _key, ""), do: opts
  defp put_opt(opts, key, value), do: Keyword.put(opts, key, value)

  defp fetch(map, key) when is_map(map) do
    Map.get(map, key) || Map.get(map, Atom.to_string(key))
  end

  defp blank?(value), do: is_nil(value) or value == ""
end
