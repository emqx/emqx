defmodule EMQXResource.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_resource,
      version: "0.1.0",
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
    [extra_applications: UMP.extra_applications(), mod: {:emqx_resource_app, []}]
  end

  def deps() do
    [
      {:emqx, in_umbrella: true},
      {:ecpool, github: "emqx/ecpool", tag: "0.5.7"},
      {:gproc, github: "emqx/gproc", tag: "0.9.0.1"},
      {:jsx, github: "talentdeficit/jsx", tag: "v3.1.0"},
      {:telemetry, "1.1.0"}
    ]
  end
end
