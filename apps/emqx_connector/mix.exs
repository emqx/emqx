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
    [mod: {:emqx_connector_app, []}]
  end

  def deps() do
    [
      {:emqx, in_umbrella: true},
      {:emqx_resource, in_umbrella: true},
      {:emqx_connector_jwt, in_umbrella: true},
      UMP.common_dep(:jose),
      UMP.common_dep(:ecpool),
      UMP.common_dep(:ehttpc),
      UMP.common_dep(:emqtt),
    ]
  end
end
