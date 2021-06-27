defmodule EmqxManagement.MixProject do
  use Mix.Project

  def project do
    [
      app: :emqx_management,
      version: "4.4.0",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "EMQ X Management API and CLI"
    ]
  end

  def application do
    [
      registered: [:emqx_management_sup],
      mod: {:emqx_mgmt_app, []},
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:emqx, in_umbrella: true},
      {:emqx_rule_engine, in_umbrella: true},
      {:minirest, github: "emqx/minirest", tag: "0.3.5"}
    ]
  end
end
