defmodule EMQXUsernameQuota.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_username_quota,
      version: version(),
      emqx_plugin: emqx_plugin(),
      build_path: "../../_build",
      erlc_options: UMP.erlc_options(),
      erlc_paths: UMP.erlc_paths(),
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def version do
    File.read!("VERSION") |> String.trim()
  end

  def application do
    [extra_applications: UMP.extra_applications(), mod: {:emqx_username_quota_app, []}]
  end

  def deps() do
    UMP.deps([
      {:emqx, in_umbrella: true},
      {:emqx_utils, in_umbrella: true}
    ])
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
