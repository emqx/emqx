defmodule EMQXMQ.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_mq,
      version: "6.0.1",
      build_path: "../../_build",
      compilers: [:elixir, :asn1, :erlang, :app],
      erlc_options: UMP.erlc_options(),
      erlc_paths: ["gen_src" | UMP.erlc_paths()],
      # used by our `compile.asn1` compiler
      asn1_srcs: asn1_srcs(),
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def asn1_srcs() do
    [
      %{src: "asn.1/MessageQueue.asn", compile_opts: [:ber, :noobj, outdir: ~c"gen_src"]}
    ]
  end

  def application do
    [extra_applications: UMP.extra_applications(), mod: {:emqx_mq_app, []}]
  end

  def deps() do
    UMP.deps([
      {:emqx, in_umbrella: true},
      {:emqx_utils, in_umbrella: true},
      {:emqx_durable_storage, in_umbrella: true},
      :minirest,
      :gproc,
      :optvar
    ])
  end
end
