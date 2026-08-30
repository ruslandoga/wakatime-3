defmodule W3.MixProject do
  use Mix.Project

  def project do
    [
      app: :w3,
      version: "0.1.0",
      elixir: "~> 1.20",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      test_coverage: [ignore_modules: [Help]],
      dialyzer: [
        plt_local_path: "plts",
        plt_core_path: "plts",
        plt_add_apps: [:ex_unit]
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {W3.Application, []}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_env), do: ["lib"]

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:bandit, "~> 1.12"},
      {:duck_nif, github: "ruslandoga/duxdb"},
      {:req, "~> 0.7.4"},
      {:req_s3, "~> 0.2.4"},
      {:telemetry, "~> 1.4"}
    ]
  end
end
