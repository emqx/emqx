defmodule EMQXBridgeAzureEventHub.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_bridge_azure_event_hub,
      version: "0.1.0",
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
      {:wolff, github: "kafka4beam/wolff", tag: "3.0.2"},
      {:kafka_protocol, github: "kafka4beam/kafka_protocol", tag: "4.1.5", override: true},
      {:brod_gssapi, github: "kafka4beam/brod_gssapi", tag: "v0.1.1"},
      {:brod, github: "kafka4beam/brod", tag: "3.18.0"},
      ## TODO: remove `mix.exs` from `wolff` and remove this override
      ## TODO: remove `mix.exs` from `pulsar` and remove this override
      {:snappyer, "1.2.9", override: true},
      {:emqx_connector, in_umbrella: true, runtime: false},
      {:emqx_resource, in_umbrella: true},
      {:emqx_bridge, in_umbrella: true, runtime: false}
    ]
  end
end
