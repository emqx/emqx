defmodule EMQXGCPDevice.MixProject do
  use Mix.Project

  def project do
    [
      app: :emqx_gcp_device,
      version: "0.1.0",
      build_path: "../../_build",
      # config_path: "../../config/config.exs",
      erlc_options: EMQXUmbrella.MixProject.erlc_options(),
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
      extra_applications: [],
      mod: {:emqx_gcp_device_app, []}
    ]
  end

  def deps() do
    [
      {:emqx, in_umbrella: true},
      {:emqx_auth, in_umbrella: true},
      {:jose, github: "potatosalad/erlang-jose", tag: "1.11.2"},
    ]
  end
end
