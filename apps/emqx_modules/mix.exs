defmodule EMQXModules.MixProject do
  use Mix.Project

  def project do
    [
      app: :emqx_modules,
      version: "4.3.2",
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
      registered: [:emqx_mod_sup],
      mod: {:emqx_modules_app, []},
      extra_applications: [:logger]
    ]
  end

  defp deps do
    []
  end
end
