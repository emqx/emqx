defmodule EMQXBridgeCouchbase.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_bridge_couchbase,
      version: "6.0.0",
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
        emqx_action_info_modules: [:emqx_bridge_couchbase_action_info],
        emqx_connector_info_modules: [:emqx_bridge_couchbase_connector_info]
      ]
    ]
  end

  def deps() do
    [
      {:emqx_resource, in_umbrella: true},
      {:emqx_gen_bridge, in_umbrella: true},
      UMP.common_dep(:ehttpc)
    ]
  end
end
