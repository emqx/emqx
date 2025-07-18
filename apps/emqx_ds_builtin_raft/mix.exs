defmodule EMQXDsBuiltinRaft.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_ds_builtin_raft,
      version: "0.2.8",
      build_path: "../../_build",
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
      extra_applications: [:mria | UMP.extra_applications()],
      mod: {:emqx_ds_builtin_raft_app, []}
    ]
  end

  def deps() do
    [
      {:emqx_durable_storage, in_umbrella: true},
      {:emqx_bpapi, in_umbrella: true},
      UMP.common_dep(:gproc),
      UMP.common_dep(:ra)
    ]
  end
end
