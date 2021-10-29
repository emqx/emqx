defmodule EMQXResource.MixProject do
  use Mix.Project

  def project do
    [
      app: :emqx_resource,
      version: "0.1.0",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      mod: {:emqx_resource_app, []},
      extra_applications: [:logger, :syntax_tools]
    ]
  end

  defp deps do
    [
      {:jsx, "3.1.0"},
      {:gproc, "0.9.0"},
      {:hocon, github: "emqx/hocon"},
      {:emqx_conf, in_umbrella: true}
    ]
  end
end
