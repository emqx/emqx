defmodule EMQXMixUtils.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_mix_utils,
      version: "6.0.0",
      build_path: "../../_build",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications
  def application do
    [extra_applications: [:eunit, :common_test, :tools, :dialyzer, :asn1]]
  end

  def deps() do
    UMP.deps([
      :gpb,
      :proper
    ])
  end
end
