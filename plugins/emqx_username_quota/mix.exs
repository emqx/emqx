defmodule EMQXUsernameQuota.MixProject do
  use Mix.Project

  def project do
    [
      app: :emqx_username_quota,
      version: version(),
      emqx_plugin: emqx_plugin(),
      build_path: "../../_build",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      erlc_options: erlc_options(),
      erlc_paths: erlc_paths(),
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def test_env?() do
    env = to_string(Mix.env())
    env =~ ~r/-test$/
  end

  def erlc_paths() do
    if test_env?() do
      ["src", "test"]
    else
      ["src"]
    end
  end

  def erlc_options() do
    if test_env?() do
      [:debug_info, {:d, :TEST}, {:parse_transform, :cth_readable_transform}]
    else
      [:debug_info]
    end
  end

  def version do
    File.read!("VERSION") |> String.trim()
  end

  def application do
    [
      extra_applications: [],
      mod: {:emqx_username_quota_app, []}
    ]
  end

  def deps() do
    [
      {:emqx_mix, path: "../..", env: emqx_mix_env(), runtime: false}
    ] ++
      if test_env?() do
        [{:cth_readable, "1.5.1"}]
      else
        []
      end
  end

  defp emqx_mix_env() do
    if test_env?() do
      :"emqx-enterprise-test"
    else
      :"emqx-enterprise"
    end
  end

  defp emqx_plugin do
    [
      rel_vsn: version(),
      metadata: [
        authors: ["EMQX"],
        builder: [
          name: "EMQX",
          contact: "developer@emqx.io",
          website: "https://www.emqx.com"
        ],
        repo: "https://github.com/emqx/emqx",
        functionality: ["Username Quota"],
        compatibility: [
          emqx: "~> 6.0"
        ],
        description: "Plugin for username-based quota checks."
      ]
    ]
  end
end
