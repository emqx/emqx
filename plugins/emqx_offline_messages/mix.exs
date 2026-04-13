defmodule EMQXOfflineMessages.MixProject do
  use Mix.Project

  def project do
    [
      app: :emqx_offline_messages,
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
    to_string(Mix.env()) =~ ~r/-test$/
  end

  def erlc_paths() do
    if test_env?(), do: ["src", "test"], else: ["src"]
  end

  def erlc_options() do
    common = [:debug_info]

    if test_env?(),
      do: common ++ [{:d, :TEST}, {:parse_transform, :cth_readable_transform}],
      else: common
  end

  def version do
    File.read!("VERSION") |> String.trim()
  end

  def application do
    [
      extra_applications: [],
      mod: {:emqx_offline_messages_app, []}
    ]
  end

  def deps() do
    [
      {:emqx_mix, path: "../..", env: emqx_mix_env(), runtime: false}
    ] ++
      if test_env?() do
        [
          {:cth_readable, "1.5.1"}
        ]
      else
        []
      end
  end

  defp emqx_mix_env() do
    if test_env?(), do: :"emqx-enterprise-test", else: :"emqx-enterprise"
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
        functionality: ["Offline message persistence"],
        compatibility: [
          emqx: "~> #{emqx_major_minor()}"
        ],
        description: "Offline message persistence plugin for EMQX."
      ]
    ]
  end

  defp emqx_major_minor do
    root = Path.expand("../..", __DIR__)
    {vsn, 0} = System.cmd(Path.join(root, "pkg-vsn.sh"), [], cd: root)
    vsn |> String.trim() |> String.split(".") |> Enum.take(2) |> Enum.join(".")
  end
end
