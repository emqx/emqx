defmodule EMQXBridgeRocketmq.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_bridge_rocketmq,
      version: "6.0.0",
      build_path: "../../_build",
      erlc_options: UMP.erlc_options(),
      erlc_paths: UMP.erlc_paths(),
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: UMP.extra_applications(),
      env: [
        emqx_action_info_modules: [:emqx_bridge_rocketmq_action_info],
        emqx_connector_info_modules: [:emqx_bridge_rocketmq_connector_info]
      ]
    ]
  end

  def deps() do
    [
      {:rocketmq, github: "emqx/rocketmq-client-erl", tag: "v0.6.1"},
      {:emqx_connector, in_umbrella: true, runtime: false},
      {:emqx_resource, in_umbrella: true},
      {:emqx_bridge, in_umbrella: true, runtime: false}
    ]
  end
end
