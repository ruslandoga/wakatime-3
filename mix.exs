defmodule W3.MixProject do
  use Mix.Project

  def project do
    [
      app: :w3,
      version: "0.1.0",
      elixir: "~> 1.20",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {W3.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:bandit, "~> 1.12"},
      {:adbc, "~> 0.12.1"},
      {:req, "~> 0.6.3"},
      {:req_s3, "~> 0.2.4"},
      {:nimble_options, "~> 1.1"}
    ]
  end
end
