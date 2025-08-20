defmodule EMQXDurableTimer.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_durable_timer,
      version: "6.0.0",
      build_path: "../../_build",
      # config_path: "../../config/config.exs",
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
      extra_applications: [
        :kernel,
        :stdlib,
        :crypto,
        :gproc,
        :optvar,
        :emqx_ds_backends | UMP.extra_applications()
      ],
      mod: {:emqx_durable_timer_app, []}
    ]
  end

  def deps() do
    UMP.deps([
      {:emqx_ds_backends, in_umbrella: true},
      :gproc,
      :optvar
    ])
  end
end
