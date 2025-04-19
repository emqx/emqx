defmodule EMQXS3.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_s3,
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
    [extra_applications: UMP.extra_applications(), mod: {:emqx_s3_app, []}]
  end

  def deps() do
    [
      {:emqx_mix_utils, in_umbrella: true, runtime: false},
      {:emqx, in_umbrella: true},
      UMP.common_dep(:gproc),
      UMP.common_dep(:hackney),
      UMP.common_dep(:erlcloud),
      {:emqx_bridge_http, in_umbrella: true, runtime: false},
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
