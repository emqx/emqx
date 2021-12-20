defmodule EMQXAuthz.MixProject do
  use Mix.Project
  Code.require_file("../../lib/emqx/mix/common.ex")

  @app :emqx_authz

  def project() do
    EMQX.Mix.Common.project(
      @app,
      deps: deps()
    )
  end

  def application() do
    EMQX.Mix.Common.application(@app,
      extra_applications: [:crypto]
    )
  end

  defp deps() do
    []
  end
end
