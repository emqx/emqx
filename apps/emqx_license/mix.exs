defmodule EMQXLicense.MixProject do
  use Mix.Project
  alias EMQXUmbrella.MixProject, as: UMP

  def project do
    [
      app: :emqx_license,
      version: "6.0.5",
      build_path: "../../_build",
      compilers: Mix.compilers() ++ [:copy_srcs],
      # used by our `Mix.Tasks.Compile.CopySrcs` compiler
      extra_dirs: extra_dirs(),
      erlc_options: UMP.erlc_options(),
      erlc_paths: UMP.erlc_paths(),
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [extra_applications: UMP.extra_applications(), mod: {:emqx_license_app, []}]
  end

  def deps() do
    UMP.deps(
      [
        {:emqx_mix_utils, in_umbrella: true, runtime: false},
        {:emqx, in_umbrella: true},
        {:emqx_utils, in_umbrella: true},
        {:emqx_ctl, in_umbrella: true},
        {:emqx_gateway, in_umbrella: true}
      ] ++ test_deps()
    )
  end

  defp test_deps() do
    if UMP.test_env?() do
      [
        {:emqx_gateway_stomp, in_umbrella: true, runtime: false},
        {:emqx_gateway_nats, in_umbrella: true, runtime: false},
        {:emqx_gateway_coap, in_umbrella: true, runtime: false},
        {:emqx_gateway_lwm2m, in_umbrella: true, runtime: false},
        {:emqx_gateway_mqttsn, in_umbrella: true, runtime: false},
        {:emqx_gateway_ocpp, in_umbrella: true, runtime: false},
        {:emqx_gateway_jt808, in_umbrella: true, runtime: false}
      ]
    else
      []
    end
  end

  defp extra_dirs() do
    dirs = []

    if UMP.test_env?() do
      ["test" | dirs]
    else
      dirs
    end
  end
end
