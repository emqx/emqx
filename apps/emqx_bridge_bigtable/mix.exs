defmodule EMQXBridgeBigtable.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  # Note: we store the proto files and scripts in a directory different from `priv`
  # because we don't want to copy them to the release.

  def project do
    [
      app: :emqx_bridge_bigtable,
      version: "6.2.0",
      build_path: "../../_build",
      compilers: [:elixir, :grpc, :erlang, :app, :copy_srcs],
      # used by our `Mix.Tasks.Compile.Grpc` compiler
      grpc_opts: %{
        gpb_opts: [
          :use_packages,
          :maps,
          :strings_as_binaries,
          i: ~c"scripts/protos",
          module_name_prefix: ~c"emqx_bigtable_gen_",
          module_name_suffix: ~c"_pb",
          report_errors: false,
          rename: {:msg_name, :snake_case}
        ],
        generate_server?: false,
        generate_client?: true,
        proto_dirs: ["scripts/protos/google/bigtable/v2"],
        out_dir: "src/generated"
      },
      xref_ignores: [
        :emqx_bigtable_gen_bigtable_pb,
        :emqx_bigtable_gen_data_pb,
        :emqx_bigtable_gen_feature_flags_pb,
        :emqx_bigtable_gen_peer_info_pb,
        :emqx_bigtable_gen_request_stats_pb,
        :emqx_bigtable_gen_response_params_pb,
        :emqx_bigtable_gen_session_pb,
        :emqx_bigtable_gen_types_pb
      ],
      # used by our `Mix.Tasks.Compile.CopySrcs` compiler
      extra_dirs: extra_dirs(),
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
    [
      extra_applications: UMP.extra_applications(),
      mod: {:emqx_bridge_bigtable_app, []},
      env: [
        emqx_action_info_modules: [:emqx_bridge_bigtable_action_info],
        emqx_connector_info_modules: [:emqx_bridge_bigtable_connector_info]
      ]
    ]
  end

  def deps() do
    UMP.deps([
      {:emqx_mix_utils, in_umbrella: true, runtime: false},
      {:emqx_gen_bridge, in_umbrella: true},
      {:emqx_resource, in_umbrella: true},
      {:emqx_connector_jwt, in_umbrella: true},
      {:emqx_bridge_gcp_pubsub, in_umbrella: true},
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
