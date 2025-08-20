defmodule EMQXDsBuiltinLocal.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_ds_builtin_local,
      version: "6.0.0",
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
      extra_applications: [:gproc, :mria, :rocksdb | UMP.extra_applications()],
      mod: {:emqx_ds_builtin_local_app, []}
    ]
  end

  def deps() do
    UMP.deps([
      {:emqx_utils, in_umbrella: true},
      {:emqx_durable_storage, in_umbrella: true}
    ])
  end
end
