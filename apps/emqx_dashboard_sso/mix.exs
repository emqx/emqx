defmodule EMQXDashboardSso.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_dashboard_sso,
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
    [extra_applications: UMP.extra_applications(), mod: {:emqx_dashboard_sso_app, []}]
  end

  def deps() do
    [
      {:emqx_ctl, in_umbrella: true},
      {:emqx_ldap, in_umbrella: true},
      {:emqx_dashboard, in_umbrella: true},
      {:esaml, github: "emqx/esaml", tag: "v1.1.3"},
      {:oidcc, github: "emqx/oidcc", tag: "v3.2.0-1", manager: :rebar3}
    ]
  end
end
