defmodule EMQXPrometheus.MixProject do
  use Mix.Project

  def project do
    [
      app: :emqx_prometheus,
      version: "4.3.0",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "Prometheus for EMQ X"
    ]
  end

  def application do
    [
      registered: [:emqx_prometheus_sup],
      mod: {:emqx_prometheus_app, []},
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:prometheus, github: "emqx/prometheus.erl", tag: "v3.1.1"}
    ]
  end
end
