defmodule EMQXGatewayLwm2m.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_gateway_lwm2m,
      version: "6.0.0",
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
    [extra_applications: [:xmerl | UMP.extra_applications()]]
  end

  def deps() do
    [
      {:emqx, in_umbrella: true},
      {:emqx_gateway, in_umbrella: true},
      {:emqx_gateway_coap, in_umbrella: true}
    ]
  end
end
