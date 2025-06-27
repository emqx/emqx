defmodule EMQXDsBackends.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_ds_backends,
      version: "0.1.0",
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
      extra_applications: UMP.extra_applications(),
      env: [
        available_backends: [
          :emqx_ds_builtin_local,
          :emqx_ds_builtin_raft
        ]
      ]
    ]
  end

  def deps() do
    %{edition_type: edition_type} = UMP.profile_info()

    ee_deps =
      if edition_type == :enterprise,
        do: [{:emqx_ds_builtin_raft, in_umbrella: true}],
        else: []

    ee_deps ++
      [
        {:emqx_utils, in_umbrella: true},
        {:emqx_durable_storage, in_umbrella: true},
        {:emqx_ds_builtin_local, in_umbrella: true}
      ]
  end
end
