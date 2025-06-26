defmodule EMQXOpentelemetry.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_opentelemetry,
      version: "0.2.12",
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
    [
      {:emqx, in_umbrella: true},
      {:emqx_resource, in_umbrella: true},
      UMP.common_dep(:minirest),
      {:opentelemetry_api,
       github: "emqx/opentelemetry-erlang",
       tag: "v1.4.9-emqx",
       sparse: "apps/opentelemetry_api",
       override: true},
      {:opentelemetry,
       github: "emqx/opentelemetry-erlang",
       tag: "v1.4.9-emqx",
       sparse: "apps/opentelemetry",
       override: true},
      {:opentelemetry_experimental,
       github: "emqx/opentelemetry-erlang",
       tag: "v1.4.9-emqx",
       sparse: "apps/opentelemetry_experimental",
       override: true},
      {:opentelemetry_api_experimental,
       github: "emqx/opentelemetry-erlang",
       tag: "v1.4.9-emqx",
       sparse: "apps/opentelemetry_api_experimental",
       override: true},
      {:opentelemetry_exporter,
       github: "emqx/opentelemetry-erlang",
       tag: "v1.4.9-emqx",
       sparse: "apps/opentelemetry_exporter",
       override: true}
    ]
  end
end
