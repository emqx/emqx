defmodule EMQX.MixProject do
  use Mix.Project

  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx,
      version: "6.0.0",
      build_path: "../../_build",
      erlc_paths: erlc_paths(),
      erlc_options: [
        {:i, "src"},
        :warn_unused_vars,
        :warn_shadow_vars,
        :warn_unused_import,
        :warn_obsolete_guard,
        :compressed
        | UMP.erlc_options()
      ],
      compilers: [:asn1] ++ Mix.compilers() ++ [:copy_srcs],
      asn1_srcs: asn1_srcs(),
      # used by our `Mix.Tasks.Compile.CopySrcs` compiler
      extra_dirs: extra_dirs(),
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def asn1_srcs() do
    [
      %{
        src: "./src/emqx_persistent_session_ds/DurableSession.asn",
        compile_opts: [:per, :noobj, :no_ok_wrapper, outdir: ~c"gen_src"]
      },
      %{
        src: "./src/emqx_persistent_session_ds/ChannelInfo.asn",
        compile_opts: [:ber, :noobj, :no_ok_wrapper, outdir: ~c"gen_src"]
      }
    ]
  end

  # Run "mix help compile.app" to learn about applications
  def application do
    [
      extra_applications:
        [:public_key, :ssl, :os_mon, :mnesia, :sasl, :mria] ++ UMP.extra_applications(),
      mod: {:emqx_app, []}
    ]
  end

  def deps() do
    UMP.deps(
      [
        {:emqx_mix_utils, in_umbrella: true, runtime: false},
        {:emqx_utils, in_umbrella: true},
        {:emqx_bpapi, in_umbrella: true},
        {:emqx_durable_storage, in_umbrella: true},
        {:emqx_ds_backends, in_umbrella: true},
        {:emqx_durable_timer, in_umbrella: true},
        :gproc,
        :gen_rpc,
        :ekka,
        :esockd,
        :cowboy,
        :lc,
        :hocon,
        :ranch,
        :bcrypt,
        :emqx_http_lib,
        :typerefl,
        :snabbkaffe,
        :recon
      ] ++ UMP.quicer_dep()
    )
  end

  defp erlc_paths() do
    paths = ["gen_src" | UMP.erlc_paths()]

    if UMP.test_env?() do
      ["integration_test" | paths]
    else
      paths
    end
  end

  defp extra_dirs() do
    dirs = ["src", "etc"]

    if UMP.test_env?() do
      ["test", "integration_test" | dirs]
    else
      dirs
    end
  end
end
