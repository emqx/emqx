defmodule EMQXConnector.MixProject do
  use Mix.Project

  def project do
    [
      app: :emqx_connector,
      version: "0.1.0",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      mod: {:emqx_connector_app, []},
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:mysql, github: "emqx/mysql-otp", tag: "1.7.1"},
      {:ecpool, github: "emqx/ecpool", tag: "0.5.1"},
      {:emqx_resource, in_umbrella: true}
    ]
  end
end
