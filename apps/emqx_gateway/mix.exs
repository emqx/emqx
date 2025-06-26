defmodule EMQXGateway.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_gateway,
      version: "0.2.6",
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
      extra_applications: [
        :emqx_auth_http,
        :emqx_auth_jwt,
        :emqx_auth_ldap,
        :emqx_auth_mnesia,
        :emqx_auth_mongodb,
        :emqx_auth_mysql,
        :emqx_auth_postgresql,
        :emqx_auth_redis
        | UMP.extra_applications()
      ],
      mod: {:emqx_gateway_app, []}
    ]
  end

  def deps() do
    [
      {:emqx_mix_utils, in_umbrella: true, runtime: false},
      {:emqx, in_umbrella: true},
      {:emqx_utils, in_umbrella: true},
      {:emqx_ctl, in_umbrella: true},
      {:emqx_auth, in_umbrella: true}
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
