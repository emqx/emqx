defmodule EMQXRetainer.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_retainer,
      version: "5.1.2",
      build_path: "../../_build",
      # config_path: "../../config/config.exs",
      erlc_options: [{:parse_transform} | UMP.strict_erlc_options()],
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
    [extra_applications: UMP.extra_applications(), mod: {:emqx_retainer_app, []}]
  end

  def deps() do
    [
      {:emqx, in_umbrella: true},
      {:emqx_ctl, in_umbrella: true},
    ]
  end
end
