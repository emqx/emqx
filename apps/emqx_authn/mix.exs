defmodule EMQXAuthn.MixProject do
  use Mix.Project

  def project do
    [
      app: :emqx_authn,
      version: "0.1.0",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "EMQ X Authentication"
    ]
  end

  def application do
    [
      registered: [:emqx_authn_sup, :emqx_authn_registry],
      mod: {:emqx_authn_app, []},
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:emqx, in_umbrella: true, runtime: false},
      {:emqx_resource, in_umbrella: true},
      {:emqx_http_lib, github: "emqx/emqx_http_lib", tag: "0.2.1"},
      {:esasl, github: "emqx/esasl", tag: "0.1.0"},
      {:epgsql, github: "epgsql/epgsql", tag: "4.4.0"},
      {:mysql, github: "emqx/mysql-otp", tag: "1.7.1"},
      {:jose, "1.11.2"}
    ]
  end
end
