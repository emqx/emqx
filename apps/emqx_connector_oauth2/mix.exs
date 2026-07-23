defmodule EMQXConnectorOAuth2.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_connector_oauth2,
      version: "6.1.0",
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

  # Run "mix help compile.app" to learn about applications
  def application do
    [
      extra_applications: UMP.extra_applications() ++ [:inets],
      mod: {:emqx_connector_oauth2_app, []}
    ]
  end

  def deps() do
    UMP.deps([
      {:emqx, in_umbrella: true},
      {:emqx_utils, in_umbrella: true},
      :hocon,
      :jose
    ])
  end
end
