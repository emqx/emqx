defmodule EMQXAuthKerberos.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_auth_kerberos,
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
      mod: {:emqx_auth_kerberos_app, []}
    ]
  end

  def deps() do
    [
      {:emqx_utils, in_umbrella: true},
      {:emqx, in_umbrella: true, runtime: false},
      {:emqx_auth, in_umbrella: true},
      UMP.common_dep(:sasl_auth)
    ]
  end
end
