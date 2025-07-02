defmodule EMQXBridgeGcpPubsub.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_bridge_gcp_pubsub,
      version: "0.3.8",
      build_path: "../../_build",
      erlc_options: UMP.strict_erlc_options(),
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
        emqx_action_info_modules: [
          :emqx_bridge_gcp_pubsub_producer_action_info,
          :emqx_bridge_gcp_pubsub_consumer_action_info
        ],
        emqx_connector_info_modules: [
          :emqx_bridge_gcp_pubsub_producer_connector_info,
          :emqx_bridge_gcp_pubsub_consumer_connector_info
        ]
      ]
    ]
  end

  def deps() do
    [
      {:emqx_connector_jwt, in_umbrella: true},
      {:emqx_connector, in_umbrella: true, runtime: false},
      {:emqx_resource, in_umbrella: true},
      {:emqx_bridge, in_umbrella: true, runtime: false},
      {:emqx_bridge_http, in_umbrella: true},
      UMP.common_dep(:ehttpc)
    ]
  end
end
