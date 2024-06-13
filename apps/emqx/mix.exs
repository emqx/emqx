defmodule EMQX.MixProject do
  use Mix.Project

  def project do
    [
      app: :emqx,
      version: "0.1.0",
      build_path: "../../_build",
      erlc_options: [
        {:i, "src"}
        | EMQXUmbrella.MixProject.erlc_options()
      ],
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
      extra_applications: [:public_key, :ssl],
      mod: {:emqx_app, []}
    ]
  end

  def deps() do
    [
      {:emqx_utils, in_umbrella: true},

      {:ekka, github: "emqx/ekka", tag: "0.19.3", override: true},
      {:esockd, github: "emqx/esockd", tag: "5.11.2"},
      {:gproc, github: "emqx/gproc", tag: "0.9.0.1", override: true},
      {:hocon, github: "emqx/hocon", tag: "0.42.2", override: true},
      {:lc, github: "emqx/lc", tag: "0.3.2", override: true},
      {:ranch, github: "emqx/ranch", tag: "1.8.1-emqx", override: true},
      {:snabbkaffe, github: "kafka4beam/snabbkaffe", tag: "1.0.10", override: true},
    ] ++ quicer_dep()
  end

  defp quicer_dep(), do: EMQXUmbrella.MixProject.quicer_dep()
end
