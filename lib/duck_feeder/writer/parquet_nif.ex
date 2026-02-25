defmodule DuckFeeder.Writer.ParquetNif do
  @moduledoc """
  Parquet writer backed by a Rustler NIF.
  """

  version = Mix.Project.config()[:version]

  use RustlerPrecompiled,
    otp_app: :duck_feeder,
    crate: "duck_feeder_parquet",
    base_url: "https://github.com/jeregrine/duck_feeder/releases/download/v#{version}",
    targets: [
      "aarch64-apple-darwin",
      "aarch64-unknown-linux-gnu",
      "arm-unknown-linux-gnueabihf",
      "riscv64gc-unknown-linux-gnu",
      "x86_64-apple-darwin",
      "x86_64-pc-windows-gnu",
      "x86_64-pc-windows-msvc",
      "x86_64-unknown-freebsd",
      "x86_64-unknown-linux-gnu"
    ],
    nif_versions: ["2.17"],
    version: version,
    force_build:
      System.get_env("DUCK_FEEDER_BUILD_NIF") in ["1", "true"] or
        Application.compile_env(:rustler_precompiled, [:force_build, :duck_feeder], false)

  @behaviour DuckFeeder.Writer.Adapter

  @impl true
  def write_batch(_config, %{rows: []}, _opts), do: {:error, :empty_rows}

  def write_batch(config, %{rows: rows}, _opts) when is_list(rows) do
    datetime_encoding = Map.get(config, :datetime_encoding, :iso8601)

    with {:ok, path} <- temp_parquet_path(config),
         normalized_rows <- Enum.map(rows, &normalize_term(&1, datetime_encoding)),
         :ok <- run_nif_write(path, normalized_rows),
         {:ok, %{size: size}} <- File.stat(path) do
      {:ok,
       %{
         local_path: path,
         row_count: length(rows),
         file_size_bytes: size,
         format: :parquet
       }}
    end
  end

  @impl true
  def cleanup(_config, %{local_path: local_path}) do
    _ = File.rm(local_path)
    :ok
  end

  defp temp_parquet_path(config) when is_map(config) do
    tmp_dir =
      config
      |> Map.get(:adapter_opts, %{})
      |> Map.new()
      |> Map.get(:tmp_dir, System.tmp_dir!())

    _ = DuckFeeder.TempFileReaper.maybe_reap(config, suffixes: [".parquet"])

    {:ok,
     Path.join(
       tmp_dir,
       "duck_feeder_#{System.unique_integer([:positive])}.parquet"
     )}
  end

  defp run_nif_write(path, rows) when is_list(rows) do
    try do
      case nif_write_parquet(path, rows) do
        :ok -> :ok
        {:ok, :ok} -> :ok
        {:error, reason} -> {:error, reason}
        {:error, atom, reason} -> {:error, {atom, reason}}
        other -> {:error, {:unexpected_parquet_nif_result, other}}
      end
    rescue
      exception in ErlangError ->
        case exception.original do
          :nif_not_loaded -> {:error, :parquet_nif_not_implemented}
          :undef -> {:error, :parquet_nif_not_implemented}
          {:undef, _} -> {:error, :parquet_nif_not_implemented}
          other -> {:error, {:parquet_nif_exception, other}}
        end
    catch
      :error, :nif_not_loaded ->
        {:error, :parquet_nif_not_implemented}

      :error, :undef ->
        {:error, :parquet_nif_not_implemented}

      kind, reason ->
        {:error, {:parquet_nif_throw, kind, reason}}
    end
  end

  defp normalize_term(%DateTime{} = datetime, :unix_microseconds),
    do: DateTime.to_unix(datetime, :microsecond)

  defp normalize_term(%DateTime{} = datetime, _encoding), do: DateTime.to_iso8601(datetime)

  defp normalize_term(%NaiveDateTime{} = datetime, :unix_microseconds) do
    datetime
    |> DateTime.from_naive!("Etc/UTC")
    |> DateTime.to_unix(:microsecond)
  end

  defp normalize_term(%NaiveDateTime{} = datetime, _encoding),
    do: NaiveDateTime.to_iso8601(datetime)

  defp normalize_term(%Date{} = date, _encoding), do: Date.to_iso8601(date)
  defp normalize_term(%Time{} = time, _encoding), do: Time.to_iso8601(time)

  defp normalize_term(nil, _encoding), do: nil
  defp normalize_term(true, _encoding), do: true
  defp normalize_term(false, _encoding), do: false
  defp normalize_term(term, _encoding) when is_atom(term), do: Atom.to_string(term)

  defp normalize_term(term, _encoding) when is_struct(term), do: inspect(term)

  defp normalize_term(term, encoding) when is_map(term) do
    term
    |> Enum.map(fn {k, v} -> {normalize_term(k, encoding), normalize_term(v, encoding)} end)
    |> Map.new()
  end

  defp normalize_term(term, encoding) when is_list(term),
    do: Enum.map(term, &normalize_term(&1, encoding))

  defp normalize_term(term, encoding) when is_tuple(term),
    do: term |> Tuple.to_list() |> normalize_term(encoding)

  defp normalize_term(term, _encoding), do: term

  defp nif_write_parquet(_path, _rows), do: :erlang.nif_error(:nif_not_loaded)
end
