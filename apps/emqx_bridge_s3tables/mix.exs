defmodule EMQXBridgeS3Tables.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_bridge_s3tables,
      version: "0.1.0",
      build_path: "../../_build",
      compilers: Mix.compilers() ++ [:copy_srcs],
      # used by our `Mix.Tasks.Compile.CopySrcs` compiler
      extra_dirs: extra_dirs(),
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
      mod: {:emqx_bridge_s3tables_app, []}
    ]
  end

  def deps() do
    [
      {:emqx_resource, in_umbrella: true},
      {:emqx_connector_aggregator, in_umbrella: true},
      {:emqx_s3, in_umbrella: true},
      {:parquer, github: "emqx/parquer", tag: "0.1.2"},
      UMP.common_dep(:erlcloud),
      UMP.common_dep(:murmerl3),
    ]
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
