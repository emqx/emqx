defmodule EmqxStatsd.MixProject do
  use Mix.Project

  def project do
    [
      app: :emqx_statsd,
      version: "0.1.0",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "EMQ X Statsd"
    ]
  end

  def application do
    [
      mod: {:emqx_statsd_app, []},
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:estatsd, github: "emqx/estatsd", tag: "0.1.0"}
    ]
  end
end
