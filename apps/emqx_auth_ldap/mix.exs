defmodule EMQXAuthLDAP.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_auth_ldap,
      version: "6.0.0",
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
      extra_applications: [:eldap | UMP.extra_applications()],
      mod: {:emqx_auth_ldap_app, []}
    ]
  end

  def deps() do
    UMP.deps([
      {:emqx, in_umbrella: true},
      {:emqx_utils, in_umbrella: true},
      {:emqx_auth, in_umbrella: true},
      {:emqx_connector, in_umbrella: true},
      {:emqx_resource, in_umbrella: true},
      {:emqx_ldap, in_umbrella: true}
    ])
  end
end
