defmodule EMQXStomp.MixProject do
  use Mix.Project

  def project do
    [
      app: :emqx_gateway,
      version: "4.4.0",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "The Gateway Management Application"
    ]
  end

  def application do
    [
      registered: [],
      mod: {:emqx_gateway_app, []},
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:emqx, in_umbrella: true, runtime: false},
      {:lwm2m_coap, github: "emqx/lwm2m-coap", tag: "v2.0.0"},
      {:grpc, github: "emqx/grpc-erl", tag: "0.6.2"},
      {:esockd, github: "emqx/esockd", tag: "5.7.4"}
    ]
  end
end
