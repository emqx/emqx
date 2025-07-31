defmodule EMQXOpentelemetry.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_opentelemetry,
      version: "6.0.0",
      build_path: "../../_build",
      erlc_options: UMP.strict_erlc_options(),
      erlc_paths: UMP.erlc_paths(),
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:emqx_management | UMP.extra_applications()],
      mod: {:emqx_otel_app, []}
    ]
  end

  def deps() do
    opentelemetry = [
      github: "emqx/opentelemetry-erlang",
      tag: "v1.4.10-emqx",
      override: true
    ]

    [
      {:emqx, in_umbrella: true},
      {:emqx_resource, in_umbrella: true},
      UMP.common_dep(:minirest),
      {:opentelemetry_api, opentelemetry ++ [sparse: "apps/opentelemetry_api"]},
      {:opentelemetry, opentelemetry ++ [sparse: "apps/opentelemetry"]},
      {:opentelemetry_experimental, opentelemetry ++ [sparse: "apps/opentelemetry_experimental"]},
      {:opentelemetry_api_experimental,
       opentelemetry ++ [sparse: "apps/opentelemetry_api_experimental"]},
      {:opentelemetry_exporter, opentelemetry ++ [sparse: "apps/opentelemetry_exporter"]}
    ]
  end
end
