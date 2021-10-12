defmodule EMQXAutoSubscribe.MixProject do
  use Mix.Project

  def project do
    [
      app: :emqx_auto_subscribe,
      version: "0.1.0",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "EMQ X Auto Subscribe"
    ]
  end

  def application do
    [
      registered: [],
      mod: {:emqx_auto_subscribe_app, []},
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:emqx, in_umbrella: true, runtime: false}
    ]
  end
end
