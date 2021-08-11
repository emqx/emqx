defmodule EMQXMachine.MixProject do
  use Mix.Project

  def project do
    [
      app: :emqx_machine,
      version: "0.1.0",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "The EMQ X Machine"
    ]
  end

  def application do
    [
      registered: [],
      mod: {:emqx_machine_app, []},
      extra_applications: [:logger]
    ]
  end

  defp deps do
    []
  end
end
