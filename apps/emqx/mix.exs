defmodule EMQX.MixProject do
  use Mix.Project

  def project do
    [
      app: :emqx,
      version: "5.0.0",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "EMQ X"
    ]
  end

  def application do
    [
      mod: {:emqx_app, []},
      extra_applications: [:logger, :os_mon, :syntax_tools]
    ]
  end

  defp deps do
    [
      {:gproc, "~> 0.9"},
      {:recon, "~> 2.5"},
      {:cowboy, github: "emqx/cowboy", tag: "2.8.2"},
      {:esockd, github: "emqx/esockd", tag: "5.8.0"},
      {:ekka, github: "emqx/ekka", tag: "0.10.2"},
      {:gen_rpc, github: "emqx/gen_rpc", tag: "2.5.1"},
      {:cuttlefish, github: "emqx/cuttlefish", tag: "v4.0.1"},
      {:hocon, github: "emqx/hocon"},
      {:pbkdf2, github: "emqx/erlang-pbkdf2", tag: "2.0.4"},
      {:snabbkaffe, github: "kafka4beam/snabbkaffe", tag: "0.14.0"}
    ]
  end
end
