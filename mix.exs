defmodule EmqxUmbrella.MixProject do
  use Mix.Project

  def project do
    [
      apps_path: "apps",
      version: "0.1.0",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  defp deps do
    [
      {:jiffy, github: "emqx/jiffy", tag: "1.0.5", override: true},
      {:gun, github: "emqx/gun", tag: "1.3.4", override: true},
      {:hocon, github: "emqx/hocon", override: true},
      {:cuttlefish,
       github: "emqx/cuttlefish",
       manager: :rebar3,
       system_env: [{"CUTTLEFISH_ESCRIPT", "true"}],
       override: true},
      {:getopt, github: "emqx/getopt", tag: "v1.0.2", override: true},
      {:cowboy, github: "emqx/cowboy", tag: "2.8.2", override: true},
      {:cowlib, "~> 2.8", override: true},
      {:poolboy, github: "emqx/poolboy", tag: "1.5.2", override: true},
      {:esockd, github: "emqx/esockd", tag: "5.8.0", override: true},
      {:gproc, "~> 0.9", override: true},
      {:eetcd, "~> 0.3", override: true},
      {:grpc, github: "emqx/grpc-erl", tag: "0.6.2", override: true},
      {:pbkdf2, github: "emqx/erlang-pbkdf2", tag: "2.0.4", override: true},
      {:typerefl, github: "k32/typerefl", tag: "0.6.2", manager: :rebar3, override: true}
    ]
  end
end
