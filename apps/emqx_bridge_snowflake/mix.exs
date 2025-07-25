defmodule EMQXBridgeSnowflake.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_bridge_snowflake,
      version: "0.1.3",
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
      extra_applications: [:ssl, :odbc] ++ UMP.extra_applications(),
      mod: {:emqx_bridge_snowflake_app, []},
      env: [
        emqx_action_info_modules: [:emqx_bridge_snowflake_aggregated_action_info],
        emqx_connector_info_modules: [:emqx_bridge_snowflake_aggregated_connector_info]
      ]
    ]
  end

  def deps() do
    [
      {:emqx_resource, in_umbrella: true},
      {:emqx_gen_bridge, in_umbrella: true},
      {:emqx_connector_jwt, in_umbrella: true},
      {:emqx_connector_aggregator, in_umbrella: true},
      UMP.common_dep(:ehttpc),
      UMP.common_dep(:ecpool),
      UMP.common_dep(:gproc)
    ]
  end
end
