defmodule DuckFeeder.MixProject do
  use Mix.Project

  @source_url "https://github.com/jeregrine/duck_feeder"

  def project do
    [
      app: :duck_feeder,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package(),
      source_url: @source_url
    ]
  end

  defp package do
    [
      files: [
        "lib",
        "native/duck_feeder_parquet/.cargo",
        "native/duck_feeder_parquet/src",
        "native/duck_feeder_parquet/Cargo*",
        "checksum-*.exs",
        "mix.exs",
        "README*",
        "LICENSE*"
      ],
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => @source_url}
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
      {:ecto_sql, "~> 3.12", only: :test},
      {:adbc, "~> 0.8", only: :test},
      {:benchee, "~> 1.3", only: :dev, runtime: false}
    ]
  end
end
