defmodule EMQXNodeRebalance.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_node_rebalance,
      version: "5.0.12",
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
    [extra_applications: UMP.extra_applications(), mod: {:emqx_node_rebalance_app, []}]
  end

  def deps() do
    [
      {:emqx, in_umbrella: true},
      {:emqx_ctl, in_umbrella: true},
      {:emqx_eviction_agent, in_umbrella: true},
    ]
  end
end
