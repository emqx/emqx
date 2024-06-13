defmodule EMQXGatewayExproto.MixProject do
  use Mix.Project

  def project do
    [
      app: :emqx_gateway_exproto,
      version: "0.1.0",
      build_path: "../../_build",
      erlc_options: EMQXUmbrella.MixProject.erlc_options(),
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [extra_applications: []]
  end

  def deps() do
    [
      {:emqx, in_umbrella: true},
      {:emqx_utils, in_umbrella: true},
      {:emqx_gateway, in_umbrella: true},
      {:grpc, github: "emqx/grpc-erl", tag: "0.6.12", override: true},
    ]
  end
end
