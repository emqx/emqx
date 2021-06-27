defmodule EmqxRuleEngine.MixProject do
  use Mix.Project

  def project do
    [
      app: :emqx_rule_engine,
      version: "4.3.2",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "EMQ X Rule Engine"
    ]
  end

  def application do
    [
      registered: [:emqx_rule_engine_sup, :emqx_rule_registry],
      mod: {:emqx_rule_engine_app, []},
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:emqx, in_umbrella: true},
      {:ekka, github: "emqx/ekka", tag: "0.9.0"}
    ]
  end
end
