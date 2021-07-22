defmodule EMQXCoap.MixProject do
  use Mix.Project

  def project do
    [
      app: :emqx_coap,
      version: "4.3.0",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "EMQ X CoAP Gateway"
    ]
  end

  def application do
    [
      mod: {:emqx_coap_app, []},
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:emqx, in_umbrella: true, runtime: false},
      {:emqx_http_lib, github: "emqx/emqx_http_lib", tag: "0.2.1"},
      {:gen_coap, github: "emqx/gen_coap", tag: "v0.3.2"}
    ]
  end
end
