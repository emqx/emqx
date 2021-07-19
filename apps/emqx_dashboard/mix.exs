defmodule EMQXDashboard.MixProject do
  use Mix.Project

  def project do
    [
      app: :emqx_dashboard,
      version: "4.4.0",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "EMQ X Web Dashboard"
    ]
  end

  def application do
    [
      registered: [:emqx_dashboard_sup],
      mod: {:emqx_dashboard_app, []},
      extra_applications: [:logger, :mnesia]
    ]
  end

  defp deps do
    [
      {:emqx, in_umbrella: true, runtime: false},
      {:minirest, github: "emqx/minirest", tag: "0.3.5"}
    ]
  end
end
