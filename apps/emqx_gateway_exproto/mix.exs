defmodule EMQXGatewayExproto.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_gateway_exproto,
      version: "0.1.0",
      build_path: "../../_build",
      erlc_options: UMP.erlc_options(),
      erlc_paths: UMP.erlc_paths(),
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [extra_applications: UMP.extra_applications()]
  end

  def deps() do
    test_deps = if UMP.test_env?(), do: [{:emqx_exhook, in_umbrella: true}], else: []
    test_deps ++ [
      {:emqx, in_umbrella: true},
      {:emqx_utils, in_umbrella: true},
      {:emqx_gateway, in_umbrella: true},
      {:grpc, github: "emqx/grpc-erl", tag: "0.6.12", override: true}
    ]
  end
end
