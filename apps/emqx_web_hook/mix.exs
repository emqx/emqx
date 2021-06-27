defmodule EmqxWebHook.MixProject do
  use Mix.Project

  def project do
    [
      app: :emqx_web_hook,
      version: "4.3.1",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "EMQ X WebHook Plugin"
    ]
  end

  def application do
    [
      registered: [:emqx_web_hook_sup],
      mod: {:emqx_web_hook_app, []},
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:emqx_rule_engine, in_umbrella: true},
      {:ehttpc, github: "emqx/ehttpc", tag: "0.1.6"}
    ]
  end
end
