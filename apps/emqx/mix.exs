defmodule EMQX.MixProject do
  use Mix.Project
  Code.require_file("../../lib/emqx/mix/common.ex")

  @app :emqx

  def project() do
    EMQX.Mix.Common.project(
      @app,
      deps: deps()
    )
  end

  def application() do
    [
      mod: EMQX.Mix.Common.from_erl!(:emqx, :mod),
      applications: EMQX.Mix.Common.from_erl!(:emqx, :applications)
    ]
  end

  # since emqx app is more complicated than others, we manually set
  # its dependencies here
  defp deps() do
    [
      {:lc, git: "https://github.com/qzhuyan/lc.git", tag: "0.1.2"},
      {:gproc, git: "https://github.com/uwiger/gproc", tag: "0.8.0"},
      {:typerefl, git: "https://github.com/k32/typerefl", tag: "0.8.5"},
      {:jiffy, git: "https://github.com/emqx/jiffy", tag: "1.0.5"},
      {:cowboy, git: "https://github.com/emqx/cowboy", tag: "2.9.0"},
      {:esockd, git: "https://github.com/emqx/esockd", tag: "5.9.0"},
      {:ekka, git: "https://github.com/emqx/ekka", tag: "0.11.1"},
      {:gen_rpc, git: "https://github.com/emqx/gen_rpc", tag: "2.5.1"},
      {:hocon, git: "https://github.com/emqx/hocon.git", tag: "0.22.0"},
      {:pbkdf2, git: "https://github.com/emqx/erlang-pbkdf2.git", tag: "2.0.4"},
      {:recon, git: "https://github.com/ferd/recon", tag: "2.5.1"},
      {:snabbkaffe, git: "https://github.com/kafka4beam/snabbkaffe.git", tag: "0.16.0"}
    ]
  end
end
