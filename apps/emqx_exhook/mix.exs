defmodule EMQXExhook.MixProject do
  use Mix.Project
  Code.require_file("../../lib/emqx/mix/common.ex")

  @app :emqx_exhook

  def project() do
    EMQX.Mix.Common.project(
      @app,
      deps: deps(),
      compilers: [:protos | Mix.compilers()],
      aliases: ["compile.protos": &protos/1]
    )
  end

  def application() do
    EMQX.Mix.Common.application(@app)
  end

  defp deps() do
    []
  end

  defp protos(_args) do
    __ENV__.file
    |> Path.dirname()
    |> EMQX.Mix.Common.compile_protos()
  end
end
