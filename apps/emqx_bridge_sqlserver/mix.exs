defmodule EMQXBridgeSqlserver.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_bridge_sqlserver,
      version: "0.2.9",
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
      extra_applications: [:odbc] ++ UMP.extra_applications(),
      env: [
        emqx_action_info_modules: [:emqx_bridge_sqlserver_action_info],
        emqx_connector_info_modules: [:emqx_bridge_sqlserver_connector_info]
      ]
    ]
  end

  def deps() do
    [
      {:emqx_connector, in_umbrella: true, runtime: false},
      {:emqx_resource, in_umbrella: true},
      {:emqx_bridge, in_umbrella: true, runtime: false}
    ]
  end
end
