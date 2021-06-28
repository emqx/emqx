defmodule EMQXBridgeMqtt.MixProject do
  use Mix.Project

  def project do
    [
      app: :emqx_bridge_mqtt,
      version: "4.3.1",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "EMQ X Bridge to MQTT Broker"
    ]
  end

  def application do
    [
      mod: {:emqx_bridge_mqtt_app, []},
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:emqx, in_umbrella: true, override: true},
      {:emqx_rule_engine, in_umbrella: true},
      {:emqtt, github: "emqx/emqtt", tag: "v1.2.3"}
    ]
  end
end
