defmodule EMQXEnterprise.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_enterprise,
      version: "0.1.0",
      build_path: "../../_build",
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
    [extra_applications: UMP.extra_applications()]
  end

  def deps() do
    [
      {:snabbkaffe, github: "kafka4beam/snabbkaffe", tag: "1.0.10"},
      {:typerefl, github: "ieQu1/typerefl", tag: "0.9.1"},
      {:hocon, github: "emqx/hocon", tag: "0.42.2"}
    ]
  end
end
