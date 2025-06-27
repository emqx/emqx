defmodule EMQXManagement.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_management,
      version: "5.3.9",
      build_path: "../../_build",
      erlc_options: UMP.strict_erlc_options(),
      erlc_paths: UMP.erlc_paths(),
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [extra_applications: UMP.extra_applications(), mod: {:emqx_mgmt_app, []}]
  end

  def deps() do
    [
      {:emqx, in_umbrella: true},
      {:emqx_utils, in_umbrella: true},
      {:emqx_bridge_http, in_umbrella: true},
      {:emqx_dashboard, in_umbrella: true, runtime: false},
      {:emqx_plugins, in_umbrella: true},
      {:emqx_ctl, in_umbrella: true},
      UMP.common_dep(:minirest),
      UMP.common_dep(:emqx_http_lib)
    ]
  end
end
