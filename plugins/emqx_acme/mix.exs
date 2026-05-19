defmodule EMQXAcme.MixProject do
  use Mix.Project

  def project do
    [
      app: :emqx_acme,
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
    common = [
      :debug_info,
      :warnings_as_errors,
      :warn_unused_vars,
      :warn_shadow_vars,
      :warn_unused_import,
      :warn_obsolete_guard,
      :warnings_as_errors
    ]

    if test_env?() do
      common ++ [{:d, :TEST}, {:parse_transform, :cth_readable_transform}]
    else
      common
    end
  end

  def version do
    File.read!("VERSION") |> String.trim()
  end

  def application do
    [
      extra_applications: [],
      mod: {:emqx_acme_app, []}
    ]
  end

  def deps() do
    [
      {:emqx_mix, path: "../..", env: emqx_mix_env(), runtime: false},
      {:acme_client, github: "emqx/acme-erlang-client", tag: "2.0.5"},
      # acme_client's rebar.config pulls jose from hex; the EMQX umbrella pins
      # jose from a git fork. Override here so the plugin and the umbrella
      # agree, otherwise mix deps.get errors with "different specs".
      {:jose, github: "potatosalad/erlang-jose", tag: "1.11.12", manager: :rebar3, override: true}
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
        functionality: ["ACME Certificate Management"],
        compatibility: [
          emqx: "~> #{emqx_major_minor()}"
        ],
        description: "ACME client plugin for automatic TLS certificate issuance and renewal.",
        index: "/ui"
      ]
    ]
  end

  defp emqx_major_minor do
    root = Path.expand("../..", __DIR__)
    {vsn, 0} = System.cmd(Path.join(root, "pkg-vsn.sh"), [], cd: root)
    vsn |> String.trim() |> String.split(".") |> Enum.take(2) |> Enum.join(".")
  end
end
