defmodule EMQXBridgeDatalayers.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_bridge_datalayers,
      version: "6.0.0",
      build_path: "../../_build",
      compilers: Mix.compilers() ++ [:copy_srcs],
      erlc_options: UMP.strict_erlc_options(),
      erlc_paths: UMP.erlc_paths(),
      extra_dirs: extra_dirs(),
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
        emqx_action_info_modules: [:emqx_bridge_datalayers_action_info],
        emqx_connector_info_modules: [:emqx_bridge_datalayers_connector_info]
      ]
    ]
  end

  def deps() do
    UMP.deps([
      :influxdb,
      {:datalayers, github: "datalayers-io/datalayers-adapter-erl", tag: "0.1.0", override: true},
      {:emqx_connector, in_umbrella: true, runtime: false},
      {:emqx_resource, in_umbrella: true},
      {:emqx_bridge, in_umbrella: true, runtime: false}
    ])
  end

  defp extra_dirs() do
    dirs = []

    if UMP.test_env?() do
      ["test" | dirs]
    else
      dirs
    end
  end
end
