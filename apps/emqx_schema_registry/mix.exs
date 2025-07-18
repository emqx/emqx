defmodule EMQXSchemaRegistry.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_schema_registry,
      version: "0.3.10",
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
      included_applications: [:emqx_rule_engine],
      mod: {:emqx_schema_registry_app, []}
    ]
  end

  def deps() do
    [
      {:emqx, in_umbrella: true},
      {:emqx_utils, in_umbrella: true},
      {:emqx_bridge_http, in_umbrella: true},
      {:emqx_rule_engine, in_umbrella: true},
      UMP.common_dep(:erlavro),
      UMP.common_dep(:jesse),
      UMP.common_dep(:gpb, runtime: true),
      {:avlizer, github: "emqx/avlizer", tag: "0.5.1.1"}
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
