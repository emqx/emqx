defmodule EmqxPskFile.MixProject do
  use Mix.Project

  def project do
    [
      app: :emqx_psk_file,
      version: "4.3.0",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "EMQX PSK Plugin from File"
    ]
  end

  def application do
    [
      registered: [:emqx_psk_file_sup],
      mod: {:emqx_psk_file_app, []},
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:emqx, in_umbrella: true}
    ]
  end
end
