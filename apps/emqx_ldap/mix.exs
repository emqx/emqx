defmodule EMQXLdap.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_ldap,
      version: "6.0.0",
      build_path: "../../_build",
      compilers: [:yecc, :leex] ++ Mix.compilers(),
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
    [extra_applications: [:eldap | UMP.extra_applications()]]
  end

  def deps() do
    [{:emqx_connector, in_umbrella: true}, {:emqx_resource, in_umbrella: true}]
  end
end
