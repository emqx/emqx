defmodule EMQXGatewayExproto.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_gateway_exproto,
      version: "6.0.0",
      build_path: "../../_build",
      compilers: [:elixir, :grpc, :erlang, :app],
      # used by our `Mix.Tasks.Compile.Grpc` compiler
      grpc_opts: %{
        gpb_opts: [
          module_name_prefix: ~c"emqx_",
          module_name_suffix: ~c"_pb"
        ],
        proto_dirs: ["priv/protos"],
        out_dir: "src"
      },
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
    [extra_applications: UMP.extra_applications()]
  end

  def deps() do
    test_deps =
      if UMP.test_env?(), do: [{:emqx_exhook, in_umbrella: true, runtime: false}], else: []

    UMP.deps(
      test_deps ++
        [
          {:emqx_mix_utils, in_umbrella: true, runtime: false},
          {:emqx, in_umbrella: true},
          {:emqx_utils, in_umbrella: true},
          {:emqx_gateway, in_umbrella: true},
          UMP.common_dep(:grpc)
        ]
    )
  end
end
