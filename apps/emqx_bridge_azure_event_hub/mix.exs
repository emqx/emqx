defmodule EMQXBridgeAzureEventHub.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_bridge_azure_event_hub,
      version: "0.2.4",
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
    [extra_applications: UMP.extra_applications()]
  end

  def deps() do
    [
      UMP.common_dep(:wolff),
      UMP.common_dep(:kafka_protocol),
      UMP.common_dep(:brod_gssapi),
      UMP.common_dep(:brod),
      UMP.common_dep(:snappyer),
      UMP.common_dep(:telemetry),
      {:emqx_connector, in_umbrella: true, runtime: false},
      {:emqx_resource, in_umbrella: true},
      {:emqx_bridge, in_umbrella: true, runtime: false}
    ]
  end
end
