defmodule EMQXDurableStorage.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_durable_storage,
      version: "0.1.0",
      build_path: "../../_build",
      compilers: [:yecc, :leex, :elixir, :asn1, :erlang, :app],
      erlc_options: UMP.erlc_options(),
      erlc_paths: ["gen_src" | UMP.erlc_paths()],
      # used by our `compile.asn1` compiler
      asn1_srcs: for i <- ["DurableMessage",
                           "DSBuiltinMetadata",
                           "DSBuiltinSLReference",
                           "DSBuiltinSLSkipstreamV1",
                           "DSBuiltinSLSkipstreamKV",
                           "DSBuiltinStorageLayer"
                          ],
               do: %{src: "./asn.1/#{i}.asn",
                     compile_opts: [:per, :noobj, outdir: ~c"gen_src"]
                    },
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications
  def application do
    [
      extra_applications: [:mria] ++ UMP.extra_applications(),
      mod: {:emqx_ds_app, []}
    ]
  end

  def deps() do
    [
      {:emqx_mix_utils, in_umbrella: true, runtime: false},
      {:emqx_utils, in_umbrella: true},
      UMP.common_dep(:rocksdb),
      UMP.common_dep(:gproc),
      UMP.common_dep(:ra),
    ]
  end
end
