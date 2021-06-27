defmodule EmqxAuthentication.MixProject do
  use Mix.Project

  def project do
    [
      app: :emqx_authentication,
      version: "0.1.0",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "EMQ X Authentication"
    ]
  end

  def application do
    [
      mod: {:emqx_authentication_app, []},
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:emqx, in_umbrella: true},
      {:jose, "~> 1.11"}
    ]
  end
end
