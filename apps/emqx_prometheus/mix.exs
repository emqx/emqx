defmodule EMQXPrometheus.MixProject do
  use Mix.Project

  def project do
    [
      app: :emqx_prometheus,
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
    [extra_applications: [], mod: {:emqx_prometheus_app, []}]
  end

  def deps() do
    [
      {:emqx, in_umbrella: true},
      {:emqx_utils, in_umbrella: true},
      {:emqx_auth, in_umbrella: true},
      {:emqx_resource, in_umbrella: true},
      {:emqx_durable_storage, in_umbrella: true},
      {:prometheus, git: "https://github.com/emqx/prometheus.erl", tag: "v4.10.0.2"}
    ]
  end
end
