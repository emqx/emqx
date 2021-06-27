defmodule EmqxRetainer.MixProject do
  use Mix.Project

  def project do
    [
      app: :emqx_retainer,
      version: "4.3.1",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "EMQ X Retainer"
    ]
  end

  def application do
    [
      registered: [:emqx_retainer_sup],
      mod: {:emqx_retainer_app, []},
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [{:emqx, in_umbrella: true}]
  end
end
