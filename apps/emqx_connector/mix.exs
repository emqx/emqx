defmodule EMQXConnector.MixProject do
  use Mix.Project
  Code.require_file("../../lib/emqx/mix/common.ex")

  @app :emqx_connector

  def project() do
    EMQX.Mix.Common.project(
      @app,
      deps: deps()
    )
  end

  def application() do
    EMQX.Mix.Common.application(@app)
  end

  defp deps() do
    [
      {:emqx_resource, in_umbrella: true, runtime: false},
      {:epgsql, github: "epgsql/epgsql", tag: "4.4.0"},
      {:mysql, github: "emqx/mysql-otp", tag: "1.7.1"},
      {:emqtt, github: "emqx/emqtt", tag: "1.4.3"},
      {:eredis_cluster, github: "emqx/eredis_cluster", tag: "0.6.7"},
      {:mongodb, github: "emqx/mongodb-erlang", tag: "v3.0.10"}
    ]
  end
end
