defmodule EMQXPrometheus.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_prometheus,
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
    [extra_applications: UMP.extra_applications(), mod: {:emqx_prometheus_app, []}]
  end

  def deps() do
    [
      {:emqx, in_umbrella: true},
      {:emqx_utils, in_umbrella: true},
      {:emqx_auth, in_umbrella: true},
      {:emqx_management, in_umbrella: true},
      {:emqx_resource, in_umbrella: true},
      {:emqx_durable_storage, in_umbrella: true},
      {:prometheus, git: "https://github.com/emqx/prometheus.erl", tag: "v4.10.0.2"}
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
