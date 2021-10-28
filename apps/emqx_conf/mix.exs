defmodule EMQXConf.MixProject do
  use Mix.Project

  def project do
    [
      app: :emqx_conf,
      version: "0.1.0",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "EMQ X Configuration Management"
    ]
  end

  def application do
    [
      mod: {:emqx_conf_app, []},
      extra_applications: [:logger, :os_mon, :syntax_tools]
    ]
  end

  defp deps do
    [
      {:emqx, in_umbrella: true, runtime: false},
      {:hocon, github: "emqx/hocon"}
    ]
  end
end
