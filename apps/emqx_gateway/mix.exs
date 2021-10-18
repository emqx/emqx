defmodule EMQXGateway.MixProject do
  use Mix.Project

  def project do
    [
      app: :emqx_gateway,
      version: "0.1.0",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.12",
      compilers: [:protos | Mix.compilers()],
      aliases: ["compile.protos": &protos/1],
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "The Gateway Management Application"
    ]
  end

  def application do
    [
      registered: [],
      mod: {:emqx_gateway_app, []},
      extra_applications: [:logger]
    ]
  end

  defp protos(_args) do
    app_path = Path.expand("..", __ENV__.file)
    config = [
      :use_packages,
      :maps,
      :strings_as_binaries,
      rename: {:msg_name, :snake_case},
      rename: {:msg_fqname, :base_name},
      i: '.',
      report_errors: false,
      o: app_path |> Path.join("src/exproto") |> to_charlist(),
      module_name_prefix: 'emqx_',
      module_name_suffix: '_pb'
    ]

    app_path
    |> Path.join("src/exproto/protos/*.proto")
    |> Path.wildcard()
    |> Enum.map(&to_charlist/1)
    |> Enum.each(&:gpb_compile.file(&1, config))

    :ok
  end

  defp deps do
    [
      {:gpb, "4.19.1", runtime: false},
      {:emqx, in_umbrella: true},
      {:lwm2m_coap, github: "emqx/lwm2m-coap", tag: "v2.0.0"},
      {:grpc, github: "emqx/grpc-erl", tag: "0.6.2"},
      {:esockd, github: "emqx/esockd", tag: "5.7.4"}
    ]
  end
end
