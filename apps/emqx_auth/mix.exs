defmodule EMQXAuth.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_auth,
      version: "0.4.7",
      build_path: "../../_build",
      compilers: Mix.compilers() ++ [:copy_srcs],
      # used by our `Mix.Tasks.Compile.CopySrcs` compiler
      extra_dirs: extra_dirs(),
      # config_path: "../../config/config.exs",
      erlc_options: [{:parse_transform} | UMP.strict_erlc_options()],
      erlc_paths: UMP.erlc_paths(),
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications
  def application do
    [extra_applications: UMP.extra_applications(), mod: {:emqx_auth_app, []}]
  end

  def deps() do
    [
      {:emqx_mix_utils, in_umbrella: true, runtime: false},
      {:emqx, in_umbrella: true},
      {:emqx_utils, in_umbrella: true}
    ]
  end

  defp extra_dirs() do
    dirs = ["etc"]

    if UMP.test_env?() do
      ["test" | dirs]
    else
      dirs
    end
  end
end
