defmodule EMQXBridgeRabbitmq.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_bridge_rabbitmq,
      version: "0.1.0",
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
    [extra_applications: UMP.extra_applications(), mod: {:emqx_bridge_rabbitmq_app, []}]
  end

  def deps() do
    [
      {:thoas, github: "emqx/thoas", tag: "v1.0.0", override: true},
      {:credentials_obfuscation,
       github: "emqx/credentials-obfuscation", tag: "v3.2.0", override: true},
      {:rabbit_common,
       github: "emqx/rabbitmq-server",
       tag: "v3.11.13.2",
       sparse: "deps/rabbit_common",
       override: true},
      {:amqp_client,
       github: "emqx/rabbitmq-server", tag: "v3.11.13.2", sparse: "deps/amqp_client"},
      {:emqx_connector, in_umbrella: true, runtime: false},
      {:emqx_resource, in_umbrella: true},
      {:emqx_bridge, in_umbrella: true, runtime: false}
    ]
  end
end
