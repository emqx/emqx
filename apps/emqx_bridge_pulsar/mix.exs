defmodule EMQXBridgePulsar.MixProject do
  use Mix.Project

  def project do
    [
      app: :emqx_bridge_pulsar,
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
    [extra_applications: []]
  end

  def deps() do
    [
      ## TODO: remove `mix.exs` from `pulsar` and remove this override
      {:crc32cer, git: "https://github.com/zmstone/crc32cer", tag: "0.1.8", override: true},
      ## TODO: remove `mix.exs` from `pulsar` and remove this override
      {:snappyer, "1.2.9", override: true},
      {:pulsar, github: "emqx/pulsar-client-erl", tag: "0.8.3"},
      {:emqx_connector, in_umbrella: true, runtime: false},
      {:emqx_resource, in_umbrella: true},
      {:emqx_bridge, in_umbrella: true, runtime: false}
    ]
  end
end
