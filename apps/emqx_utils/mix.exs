defmodule EMQXUtils.MixProject do
  use Mix.Project

  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_utils,
      version: "0.1.0",
      build_path: "../../_build",
      erlc_options: UMP.erlc_options(),
      erlc_paths: ["etc" | UMP.erlc_paths()],
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: UMP.common_deps() ++ deps()
    ]
  end

  def application do
    [
      extra_applications: UMP.extra_applications()
    ]
  end

  def deps() do
    [
      {:jiffy, github: "emqx/jiffy", tag: "1.0.6"},
      {:emqx_http_lib, github: "emqx/emqx_http_lib", tag: "0.5.3"},
      {:snabbkaffe, github: "kafka4beam/snabbkaffe", tag: "1.0.10", override: true},
    ]
  end
end
