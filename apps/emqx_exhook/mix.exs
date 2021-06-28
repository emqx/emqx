defmodule EMQXExhook.MixProject do
  use Mix.Project

  def project do
    [
      app: :emqx_exhook,
      version: "4.3.1",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "EMQ X Extension for Hook"
    ]
  end

  def application do
    [
      mod: {:emqx_exhook_app, []},
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:grpc, github: "emqx/grpc-erl", tag: "0.6.2"}
    ]
  end
end
