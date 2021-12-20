defmodule EMQXStatsd.MixProject do
  use Mix.Project
  Code.require_file("../../lib/emqx/mix/common.ex")

  @app :emqx_statsd

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
    EMQX.Mix.Common.from_rebar_deps!()
  end
end
