defmodule EMQXStreams.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_streams,
      version: "6.0.1",
      build_path: "../../_build",
      compilers: [:elixir, :asn1, :erlang, :app],
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
    [extra_applications: UMP.extra_applications(), mod: {:emqx_streams_app, []}]
  end

  def deps() do
    UMP.deps([
      {:emqx, in_umbrella: true},
      {:emqx_utils, in_umbrella: true},
      {:emqx_durable_storage, in_umbrella: true},
      {:emqx_extsub, in_umbrella: true},
      :minirest,
      :gproc,
      :ecpool,
      :optvar
    ])
  end
end
