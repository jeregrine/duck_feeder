defmodule DuckFeeder.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/jeregrine/duck_feeder"

  def project do
    [
      app: :duck_feeder,
      version: @version,
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      description: description(),
      deps: deps(),
      package: package(),
      docs: docs(),
      source_url: @source_url
    ]
  end

  defp package do
    [
      files: [
        "lib",
        "native/duck_feeder_parquet/src",
        "native/duck_feeder_parquet/Cargo*",
        "checksum-*.exs",
        "mix.exs",
        "README*",
        "LICENSE"
      ],
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => @source_url,
        "Changelog" => "#{@source_url}/releases"
      }
    ]
  end

  defp description do
    "Postgres CDC/WAL to Parquet on object storage with DuckLake metadata for DuckDB analytics."
  end

  defp docs do
    [
      main: "readme",
      source_ref: "v#{@version}",
      source_url: @source_url,
      extras: [
        "README.md",
        "docs/plan.md"
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :ssl]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:req, "~> 0.5"},
      {:postgrex, "~> 0.20"},
      {:nimble_options, "~> 1.1"},
      {:rustler_precompiled, "~> 0.8"},
      {:rustler, ">= 0.0.0", optional: true},
      {:adbc, "~> 0.8"},
      {:jason, "~> 1.4"},
      {:ecto_sql, "~> 3.12", only: :test},
      {:benchee, "~> 1.3", only: :dev, runtime: false},
      {:ex_doc, "~> 0.38", only: :dev, runtime: false}
    ]
  end
end
