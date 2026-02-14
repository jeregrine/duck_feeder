defmodule DuckFeeder.MixProject do
  use Mix.Project

  def project do
    [
      app: :duck_feeder,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps()
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
      {:rustler, "~> 0.36", runtime: false},
      {:benchee, "~> 1.3", only: :dev, runtime: false}
    ]
  end
end
