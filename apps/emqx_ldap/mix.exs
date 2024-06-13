defmodule EMQXLdap.MixProject do
  use Mix.Project

  def project do
    [
      app: :emqx_ldap,
      version: "0.1.0",
      build_path: "../../_build",
      erlc_options: EMQXUmbrella.MixProject.erlc_options(),
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [extra_applications: [:eldap]]
  end

  def deps() do
    [{:emqx_connector, in_umbrella: true},
     {:emqx_resource, in_umbrella: true}]
  end
end
