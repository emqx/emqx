defmodule EMQXBridgeRabbitmq.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_bridge_rabbitmq,
      version: "6.0.2",
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
    [
      extra_applications: [:rabbit_common | UMP.extra_applications()],
      mod: {:emqx_bridge_rabbitmq_app, []},
      env: [
        emqx_action_info_modules: [:emqx_bridge_rabbitmq_action_info],
        emqx_connector_info_modules: [:emqx_bridge_rabbitmq_connector_info]
      ]
    ]
  end

  def deps() do
    UMP.deps([
      {:thoas, "1.2.1"},
      {:credentials_obfuscation, github: "rabbitmq/credentials-obfuscation", tag: "v3.5.0"},
      {:rabbit_common,
       github: "rabbitmq/rabbitmq-server", tag: "v4.2.1", subdir: "deps/rabbit_common"},
      {:amqp_client,
       github: "rabbitmq/rabbitmq-server", tag: "v4.2.1", subdir: "deps/amqp_client"},
      {:emqx_connector, in_umbrella: true, runtime: false},
      {:emqx_resource, in_umbrella: true},
      {:emqx_bridge, in_umbrella: true, runtime: false}
    ])
  end
end
