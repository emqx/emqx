defmodule EMQXSn.MixProject do
  use Mix.Project

  def project do
    [
      app: :emqx_sn,
      version: "4.4.0",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "EMQ X MQTT-SN Plugin"
    ]
  end

  def application do
    [
      mod: {:emqx_sn_app, []},
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:esockd, github: "emqx/esockd", tag: "5.7.4"}
    ]
  end
end
