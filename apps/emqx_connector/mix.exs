defmodule EMQXConnector.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_connector,
      version: "0.1.0",
      build_path: "../../_build",
      # erlc_options: [
      #   # config_path: "../../config/config.exs",
      #   # We need this because we can't make `:emqx_connector` application depend on
      #   # `:emqx_bridge`, otherwise a dependency cycle would be created, but at the same
      #   # time `:emqx_connector` need some includes from `:emqx_bridge` to compile...
      #   {:i, "../emqx_bridge/include"} | UMP.erlc_options()
      # ],
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
    [extra_applications: [:eredis], mod: {:emqx_connector_app, []}]
  end

  def deps() do
    [
      {:emqx, in_umbrella: true},
      {:emqx_resource, in_umbrella: true},
      {:jose, github: "potatosalad/erlang-jose", tag: "1.11.2"},
      {:ecpool, github: "emqx/ecpool", tag: "0.5.7"},
      {:eredis_cluster, github: "emqx/eredis_cluster", tag: "0.8.4"},
      {:ehttpc, github: "emqx/ehttpc", tag: "0.4.13"},
      {:emqtt, github: "emqx/emqtt", tag: "1.10.1", system_env: UMP.maybe_no_quic_env()}
    ]
  end
end
