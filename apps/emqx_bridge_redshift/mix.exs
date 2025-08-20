defmodule EMQXBridgeRedshift.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_bridge_redshift,
      version: "6.0.0",
      build_path: "../../_build",
      compilers: Mix.compilers() ++ [:copy_srcs],
      # used by our `Mix.Tasks.Compile.CopySrcs` compiler
      extra_dirs: extra_dirs(),
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
        emqx_action_info_modules: [:emqx_bridge_redshift_action_info],
        emqx_connector_info_modules: [:emqx_bridge_redshift_connector_info]
      ]
    ]
  end

  def deps() do
    UMP.deps([
      {:emqx_gen_bridge, in_umbrella: true},
      {:emqx_resource, in_umbrella: true}
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
