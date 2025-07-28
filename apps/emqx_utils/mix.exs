defmodule EMQXUtils.MixProject do
  use Mix.Project

  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_utils,
      version: "6.0.0",
      build_path: "../../_build",
      compilers: [:yecc, :leex] ++ Mix.compilers(),
      erlc_options: UMP.erlc_options(),
      erlc_paths: ["etc" | UMP.erlc_paths()],
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: UMP.common_deps() ++ deps() ++ test_deps()
    ]
  end

  def application do
    [
      extra_applications: UMP.extra_applications()
    ]
  end

  def deps() do
    [
      UMP.common_dep(:jiffy),
      UMP.common_dep(:emqx_http_lib),
      UMP.common_dep(:snabbkaffe),
      {:erlang_qq, github: "k32/erlang_qq", tag: "1.0.0", override: true}
    ]
  end

  defp test_deps() do
    if UMP.test_env?() do
      [
        UMP.common_dep(:cowboy)
      ]
    else
      []
    end
  end
end
