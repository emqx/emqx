defmodule EMQXBridgeZerobus.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_bridge_zerobus,
      version: "6.3.0",
      build_path: "../../_build",
      compilers: [:elixir, :grpc, :erlang, :app, :copy_srcs],
      # used by our `Mix.Tasks.Compile.Grpc` compiler
      grpc_opts: %{
        gpb_opts: [
          :use_packages,
          :maps,
          :strings_as_binaries,
          i: ~c"scripts/protos",
          module_name_prefix: ~c"emqx_bridge_zerobus_gen_",
          module_name_suffix: ~c"_pb",
          report_errors: false,
          rename: {:msg_name, :snake_case}
        ],
        gpb_opts_overrides: %{
          "scripts/protos/descriptor.proto" => [
            :strings_as_binaries,
            module_name_prefix: ~c"emqx_bridge_zerobus_gen_",
            module_name_suffix: ~c"_pb",
            maps: true,
            maps_key_type: :atom,
            maps_oneof: :flat,
            verify: :always,
            maps_unset_optional: :omitted
          ]
        },
        generate_server?: false,
        generate_client?: false,
        proto_dirs: ["scripts/protos/"],
        out_dir: "src/generated"
      },
      xref_ignores: [
        :emqx_bridge_zerobus_gen_descriptor_pb,
        :emqx_bridge_zerobus_gen_zerobus_service_pb
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
      mod: {:emqx_bridge_zerobus_app, []},
      env: [
        emqx_action_info_modules: [:emqx_bridge_zerobus_action_info],
        emqx_connector_info_modules: [:emqx_bridge_zerobus_connector_info]
      ]
    ]
  end

  def deps() do
    UMP.deps([
      {:emqx_gen_bridge, in_umbrella: true},
      {:emqx_resource, in_umbrella: true},
      {:emqx_schema_registry, in_umbrella: true},
      {:emqx_connector_oauth2, in_umbrella: true},
      UMP.common_dep(:gpb, runtime: true),
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
