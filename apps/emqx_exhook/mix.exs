defmodule EMQXExhook.MixProject do
  use Mix.Project

  def project do
    [
      app: :emqx_exhook,
      version: "5.0.0",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.12",
      compilers: [:protos | Mix.compilers()],
      aliases: ["compile.protos": &protos/1],
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "EMQ X Extension for Hook"
    ]
  end

  def application do
    [
      registered: [],
      mod: {:emqx_exhook_app, []},
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:emqx, in_umbrella: true, runtime: false},
      {:grpc, github: "emqx/grpc-erl", tag: "0.6.2"}
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
      o: app_path |> Path.join("src") |> to_charlist(),
      module_name_prefix: 'emqx_',
      module_name_suffix: '_pb'
    ]

    app_path
    |> Path.join("priv/protos/*.proto")
    |> Path.wildcard()
    |> Enum.map(&to_charlist/1)
    |> Enum.each(&:gpb_compile.file(&1, config))

    :ok
  end
end
