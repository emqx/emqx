defmodule EmqxLwm2m.MixProject do
  use Mix.Project

  def project do
    [
      app: :emqx_lwm2m,
      version: "4.3.1",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "EMQ X LwM2M Gateway"
    ]
  end

  def application do
    [
      registered: [:emqx_lwm2m_sup],
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:emqx, in_umbrella: true},
      {:lwm2m_coap, github: "emqx/lwm2m-coap", tag: "v1.1.2"}
    ]
  end
end
