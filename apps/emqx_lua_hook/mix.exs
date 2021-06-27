defmodule EmqxLuaHook.MixProject do
  use Mix.Project

  def project do
    [
      app: :emqx_lua_hook,
      version: "4.3.0",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "EMQ X Lua Hooks"
    ]
  end

  def application do
    [
      mod: {:emqx_lua_hook_app, []},
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:emqx, in_umbrella: true},
      # {:luerl, github: "emqx/luerl", manager: :rebar3, tag: "v0.3.1"}
      {:luerl, github: "rvirding/luerl", manager: :rebar3}
    ]
  end
end
