defmodule EMQXStomp.MixProject do
  use Mix.Project

  def project do
    [
      app: :emqx_stomp,
      version: "4.4.0",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "EMQ X Stomp Protocol Plugin"
    ]
  end

  def application do
    [
      registered: [:emqx_stomp_sup],
      mod: {:emqx_stomp, []},
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:emqx, in_umbrella: true}
    ]
  end
end
