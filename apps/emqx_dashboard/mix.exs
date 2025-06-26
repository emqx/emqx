defmodule EMQXDashboard.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_dashboard,
      version: "5.2.4",
      build_path: "../../_build",
      # config_path: "../../config/config.exs",
      erlc_options: [{:d, :APPLICATION, :emqx} | UMP.strict_erlc_options()],
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
    [extra_applications: UMP.extra_applications(), mod: {:emqx_dashboard_app, []}]
  end

  def deps() do
    [
      {:pot, "1.0.2"},
      {:emqx_ctl, in_umbrella: true},
      {:emqx_utils, in_umbrella: true},
      {:emqx, in_umbrella: true},
      ## Probably not actually needed?
      {:emqx_bridge_http, in_umbrella: true},
      UMP.common_dep(:minirest),
    ]
  end
end
