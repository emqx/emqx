defmodule EMQXGateway.MixProject do
  use Mix.Project
  Code.require_file("../../lib/emqx/mix/common.ex")

  @app :emqx_gateway

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

  defp protos(_args) do
    __ENV__.file
    |> Path.dirname()
    |> EMQX.Mix.Common.compile_protos()
  end

  defp deps() do
    EMQX.Mix.Common.from_rebar_deps!() ++
      [
        {:emqx, in_umbrella: true}
      ]
  end
end
