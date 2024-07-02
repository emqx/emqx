defmodule EMQXDurableStorage.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_durable_storage,
      version: "0.1.0",
      build_path: "../../_build",
      # config_path: "../../config/config.exs",
      erlc_options: UMP.erlc_options(),
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
    [
      extra_applications: [:mria] ++ UMP.extra_applications(),
      mod: {:emqx_ds_app, []}
    ]
  end

  def deps() do
    [
      {:emqx_utils, in_umbrella: true},
      {:gproc, github: "emqx/gproc", tag: "0.9.0.1"},
      {:rocksdb, github: "emqx/erlang-rocksdb", tag: "1.8.0-emqx-5"},
      {:ra, "2.7.3"},
    ]
  end
end
