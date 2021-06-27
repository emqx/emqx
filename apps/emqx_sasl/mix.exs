defmodule EmqxSasl.MixProject do
  use Mix.Project

  def project do
    [
      app: :emqx_sasl,
      version: "0.1.0",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "EMQ X SASL"
    ]
  end

  def application do
    [
      registered: [:emqx_sasl_sup],
      mod: {:emqx_sasl_app, []},
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:emqx, in_umbrella: true},
      {:pbkdf2, github: "emqx/erlang-pbkdf2", tag: "2.0.4"}
    ]
  end
end
