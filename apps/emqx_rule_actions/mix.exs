defmodule EmqxRuleActions.MixProject do
  use Mix.Project

  def project do
    [
      app: :emqx_rule_actions,
      version: "5.0.0",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "Rule Actions"
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:emqx, in_umbrella: true, runtime: false},
      {:emqx_rule_engine, in_umbrella: true}
    ]
  end
end
