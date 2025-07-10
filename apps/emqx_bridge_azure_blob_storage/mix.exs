defmodule EMQXBridgeAzureBlobStorage.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_bridge_azure_blob_storage,
      version: "0.1.5",
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
      mod: {:emqx_bridge_azure_blob_storage_app, []},
      env: [
        emqx_action_info_modules: [:emqx_bridge_azure_blob_storage_action_info],
        emqx_connector_info_modules: [:emqx_bridge_azure_blob_storage_connector_info]
      ]
    ]
  end

  def deps() do
    [
      {:emqx_resource, in_umbrella: true},
      {:emqx_gen_bridge, in_umbrella: true},
      {:emqx_connector_aggregator, in_umbrella: true},
      {:erlazure, github: "emqx/erlazure", tag: "0.4.0.1"}
    ]
  end
end
