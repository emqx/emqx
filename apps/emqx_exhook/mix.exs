defmodule EMQXExhook.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_exhook,
      version: "6.0.0",
      build_path: "../../_build",
      compilers: [:elixir, :grpc, :erlang, :app, :copy_srcs],
      # used by our `Mix.Tasks.Compile.CopySrcs` compiler
      extra_dirs: extra_dirs(),
      # used by our `Mix.Tasks.Compile.Grpc` compiler
      grpc_opts: %{
        gpb_opts: [
          module_name_prefix: ~c"emqx_",
          module_name_suffix: ~c"_pb",
          o: ~c"src/pb"
        ],
        proto_dirs: ["priv/protos"],
        out_dir: "src/pb"
      },
      xref_ignores: [
        :emqx_exhook_pb,
        :emqx_exproto_pb
      ],
      erlc_options: UMP.strict_erlc_options(),
      erlc_paths: UMP.erlc_paths(),
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [extra_applications: UMP.extra_applications(), mod: {:emqx_exhook_app, []}]
  end

  def deps() do
    UMP.deps([
      {:emqx_mix_utils, in_umbrella: true, runtime: false},
      {:emqx, in_umbrella: true},
      {:emqx_utils, in_umbrella: true},
      :minirest,
      :grpc
    ])
  end

  defp extra_dirs() do
    dirs = []

    if UMP.test_env?() do
      ["test" | dirs]
    else
      dirs
    end
  end
end
