defmodule EMQXFt.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_ft,
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
    [extra_applications: UMP.extra_applications(), mod: {:emqx_ft_app, []}]
  end

  def deps() do
    [
      {:emqx, in_umbrella: true},
      {:emqx_s3, in_umbrella: true},
      UMP.common_dep(:gproc)
    ]
  end
end
