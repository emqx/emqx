defmodule EMQX.MixProject do
  use Mix.Project

  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx,
      version: "0.1.0",
      build_path: "../../_build",
      erlc_paths: UMP.erlc_paths(),
      erlc_options: [
        {:i, "src"}
        | UMP.erlc_options()
      ],
      compilers: Mix.compilers() ++ [:copy_srcs],
      # used by our `Mix.Tasks.Compile.CopySrcs` compiler
      extra_dirs: extra_dirs(),
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications
  def application do
    [
      ## FIXME!!! go though emqx.app.src and add missing stuff...
      extra_applications: [:public_key, :ssl, :os_mon, :logger, :mnesia] ++ UMP.extra_applications(),
      mod: {:emqx_app, []}
    ]
  end

  def deps() do
    ## FIXME!!! go though emqx.app.src and add missing stuff...
    [
      {:emqx_utils, in_umbrella: true},
      {:emqx_ds_backends, in_umbrella: true},

      {:ekka, github: "emqx/ekka", tag: "0.19.3", override: true},
      {:esockd, github: "emqx/esockd", tag: "5.11.2"},
      {:gproc, github: "emqx/gproc", tag: "0.9.0.1", override: true},
      {:hocon, github: "emqx/hocon", tag: "0.42.2", override: true},
      {:lc, github: "emqx/lc", tag: "0.3.2", override: true},
      {:ranch, github: "emqx/ranch", tag: "1.8.1-emqx", override: true},
    ] ++ UMP.quicer_dep()
  end

  defp extra_dirs() do
    dirs = ["src", "etc"]
    if UMP.test_env?() do
      ["test", "integration_test" | dirs]
    else
      dirs
    end
  end
end
