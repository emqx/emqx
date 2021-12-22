defmodule EMQXUmbrella.MixProject do
  use Mix.Project

  # Temporary hack while 1.13.2 is not released
  System.version()
  |> Version.parse!()
  |> Version.compare(Version.parse!("1.13.2"))
  |> Kernel.==(:lt)
  |> if(do: Code.require_file("lib/mix/release.exs"))

  def project do
    [
      app: :emqx_mix,
      version: pkg_vsn(),
      deps: deps(),
      releases: releases()
    ]
  end

  defp deps do
    # we need several overrides here because dependencies specify
    # other exact versions, and not ranges.
    [
      {:lc, github: "qzhuyan/lc", tag: "0.1.2"},
      {:typerefl, github: "k32/typerefl", tag: "0.8.5", override: true},
      {:ehttpc, github: "emqx/ehttpc", tag: "0.1.12"},
      {:gproc, "0.8.0", override: true},
      {:jiffy, github: "emqx/jiffy", tag: "1.0.5", override: true},
      {:cowboy, github: "emqx/cowboy", tag: "2.9.0", override: true},
      {:esockd, github: "emqx/esockd", tag: "5.9.0", override: true},
      {:mria, github: "emqx/mria", tag: "0.1.5", override: true},
      {:ekka, github: "emqx/ekka", tag: "0.11.1", override: true},
      {:gen_rpc, github: "emqx/gen_rpc", tag: "2.5.1", override: true},
      {:minirest, github: "emqx/minirest", tag: "1.2.7", override: true},
      {:ecpool, github: "emqx/ecpool", tag: "0.5.1"},
      {:replayq, "0.3.3", override: true},
      {:pbkdf2, github: "emqx/erlang-pbkdf2", tag: "2.0.4", override: true},
      {:emqtt, github: "emqx/emqtt", tag: "1.4.3", override: true},
      {:rulesql, github: "emqx/rulesql", tag: "0.1.4"},
      {:observer_cli, "1.7.1"},
      {:system_monitor, github: "klarna-incubator/system_monitor", tag: "2.2.0"},
      # in conflict by emqtt and hocon
      {:getopt, "1.0.2", override: true},
      {:snabbkaffe, github: "kafka4beam/snabbkaffe", tag: "0.16.0", override: true},
      {:hocon, github: "emqx/hocon", tag: "0.22.0", override: true},
      {:emqx_http_lib, github: "emqx/emqx_http_lib", tag: "0.4.1", override: true},
      {:esasl, github: "emqx/esasl", tag: "0.2.0"},
      {:jose, github: "potatosalad/erlang-jose", tag: "1.11.2"},
      # in conflict by ehttpc and emqtt
      {:gun, github: "emqx/gun", tag: "1.3.6", override: true},
      # in conflict by emqx_connectior and system_monitor
      {:epgsql, github: "epgsql/epgsql", tag: "4.6.0", override: true},
      # in conflict by mongodb and eredis_cluster
      {:poolboy, github: "emqx/poolboy", tag: "1.5.2", override: true},
      # in conflict by gun and emqtt
      {:cowlib, "2.8.0", override: true},
      # in conflict by cowboy_swagger and cowboy
      {:ranch, "1.8.0", override: true},
      # in conflict by emqx and observer_cli
      {:recon, github: "ferd/recon", tag: "2.5.1", override: true},
      {:jsx, github: "talentdeficit/jsx", tag: "v3.1.0", override: true}
    ] ++
      Enum.map(
        [
          :emqx,
          :emqx_conf,
          :emqx_machine,
          :emqx_plugin_libs,
          :emqx_resource,
          :emqx_connector,
          :emqx_authn,
          :emqx_authz,
          :emqx_auto_subscribe,
          :emqx_gateway,
          :emqx_exhook,
          :emqx_bridge,
          :emqx_rule_engine,
          :emqx_modules,
          :emqx_management,
          :emqx_dashboard,
          :emqx_statsd,
          :emqx_retainer,
          :emqx_prometheus,
          :emqx_psk,
          :emqx_slow_subs,
          :emqx_plugins
        ],
        &umbrella/1
      ) ++ bcrypt_dep() ++ quicer_dep()
  end

  defp umbrella(app), do: {app, path: "apps/#{app}", manager: :rebar3, override: true}

  defp releases() do
    [
      emqx_base: [
        applications: [
          logger: :permanent,
          esasl: :load,
          crypto: :permanent,
          public_key: :permanent,
          asn1: :permanent,
          syntax_tools: :permanent,
          ssl: :permanent,
          os_mon: :permanent,
          inets: :permanent,
          compiler: :permanent,
          runtime_tools: :permanent,
          hocon: :load,
          emqx: :load,
          emqx_conf: :load,
          emqx_machine: :permanent,
          mria: :load,
          mnesia: :load,
          ekka: :load,
          emqx_plugin_libs: :load,
          emqx_http_lib: :permanent,
          emqx_resource: :permanent,
          emqx_connector: :permanent,
          emqx_authn: :permanent,
          emqx_authz: :permanent,
          emqx_auto_subscribe: :permanent,
          emqx_gateway: :permanent,
          emqx_exhook: :permanent,
          emqx_bridge: :permanent,
          emqx_rule_engine: :permanent,
          emqx_modules: :permanent,
          emqx_management: :permanent,
          emqx_dashboard: :permanent,
          emqx_statsd: :permanent,
          emqx_retainer: :permanent,
          emqx_prometheus: :permanent,
          emqx_psk: :permanent,
          emqx_slow_subs: :permanent,
          emqx_plugins: :permanent,
          emqx_mix: :none
        ],
        skip_mode_validation_for: [
          :emqx_gateway,
          :emqx_dashboard,
          :emqx_resource,
          :emqx_connector,
          :emqx_exhook,
          :emqx_bridge,
          :emqx_modules,
          :emqx_management,
          :emqx_statsd,
          :emqx_retainer,
          :emqx_prometheus,
          :emqx_plugins
        ],
        steps: [
          :assemble,
          &create_RELEASES/1,
          &copy_files/1,
          &copy_nodetool/1
        ]
      ]
    ]
  end

  def copy_files(release) do
    overwrite? = Keyword.get(release.options, :overwrite, false)

    bin = Path.join(release.path, "bin")
    etc = Path.join(release.path, "etc")

    Mix.Generator.create_directory(bin)
    Mix.Generator.create_directory(etc)
    Mix.Generator.create_directory(Path.join(etc, "certs"))

    Mix.Generator.copy_file(
      "apps/emqx_authz/etc/acl.conf",
      Path.join(etc, "acl.conf"),
      force: overwrite?
    )

    # FIXME: check if cloud/edge???
    Mix.Generator.copy_file(
      "apps/emqx/etc/emqx_cloud/vm.args",
      Path.join(etc, "vm.args"),
      force: overwrite?
    )

    # FIXME: check if cloud/edge!!
    Mix.Generator.copy_file(
      "apps/emqx/etc/emqx_cloud/vm.args",
      Path.join(release.version_path, "vm.args"),
      force: overwrite?
    )

    # required by emqx_authz
    File.cp_r!(
      "apps/emqx/etc/certs",
      Path.join(etc, "certs")
    )

    # this is required by the produced escript / nodetool
    Mix.Generator.copy_file(
      Path.join(release.version_path, "start_clean.boot"),
      Path.join(bin, "no_dot_erlang.boot"),
      force: overwrite?
    )

    # FIXME: change variables by package type???
    assigns = [
      platform_bin_dir: "bin",
      platform_data_dir: "data",
      platform_etc_dir: "etc",
      platform_lib_dir: "lib",
      platform_log_dir: "log",
      platform_plugins_dir: "plugins",
      runner_root_dir: "$(cd $(dirname $(readlink $0 || echo $0))/..; pwd -P)",
      runner_bin_dir: "$RUNNER_ROOT_DIR/bin",
      runner_etc_dir: "$RUNNER_ROOT_DIR/etc",
      runner_lib_dir: "$RUNNER_ROOT_DIR/lib",
      runner_log_dir: "$RUNNER_ROOT_DIR/log",
      runner_data_dir: "$RUNNER_ROOT_DIR/data",
      runner_user: "",
      release_version: release.version,
      erts_vsn: release.erts_version,
      # FIXME: this is empty in `make emqx` ???
      erl_opts: "",
      # FIXME: varies with edge/community/enterprise
      emqx_description: "EMQ X Community Edition"
    ]

    # This is generated by `scripts/merge-config.escript` or `make
    # conf-segs`.  So, this should be run before the release.
    # TODO: run as a "compiler" step???
    conf_rendered =
      File.read!("apps/emqx_conf/etc/emqx.conf.all")
      |> from_rebar_to_eex_template()
      |> EEx.eval_string(assigns)

    File.write!(
      Path.join(etc, "emqx.conf"),
      conf_rendered
    )

    vars_rendered =
      File.read!("data/emqx_vars")
      |> from_rebar_to_eex_template()
      |> EEx.eval_string(assigns)

    File.write!(
      Path.join([release.path, "releases", "emqx_vars"]),
      vars_rendered
    )

    Enum.each(
      [
        "common_defs.sh",
        "common_defs2.sh",
        "common_functions.sh",
      ],
      &Mix.Generator.copy_file(
        "bin/#{&1}",
        Path.join(bin, &1),
        force: overwrite?
      )
    )

    release
  end

  # needed by nodetool and by release_handler
  def create_RELEASES(release) do
    apps =
      Enum.map(release.applications, fn {app_name, app_props} ->
        app_vsn = Keyword.fetch!(app_props, :vsn)

        app_path =
          "./lib"
          |> Path.join("#{app_name}-#{app_vsn}")
          |> to_charlist()

        {app_name, app_vsn, app_path}
      end)

    release_entry = [
      {
        :release,
        to_charlist(release.name),
        to_charlist(release.version),
        release.erts_version,
        apps,
        :permanent
      }
    ]

    release.path
    |> Path.join("releases")
    |> Path.join("RELEASES")
    |> File.open!([:write, :utf8], fn handle ->
      IO.puts(handle, "%% coding: utf-8")
      :io.format(handle, '~tp.~n', [release_entry])
    end)

    release
  end

  def copy_nodetool(release) do
    [shebang, rest] =
      "bin/nodetool"
      |> File.read!()
      |> String.split("\n", parts: 2)

    path = Path.join([release.path, "bin", "nodetool"])
    # the elixir version of escript + start.boot required the boot_var
    # RELEASE_LIB to be defined.
    boot_var = "%%!-boot_var RELEASE_LIB $RUNNER_ROOT_DIR/lib"
    File.write!(path, [shebang, "\n", boot_var, "\n", rest])

    release
  end

  def bcrypt_dep() do
    if enable_bcrypt?(),
      do: [{:bcrypt, github: "emqx/erlang-bcrypt", tag: "0.6.0", override: true}],
      else: []
  end

  def quicer_dep() do
    if enable_quicer?(),
      # in conflict with emqx and emqtt
      do: [{:quicer, github: "emqx/quic", tag: "0.0.9", override: true}],
      else: []
  end

  def enable_bcrypt?() do
    not win32?()
  end

  def enable_quicer?() do
    not Enum.any?([
      build_without_quic?(),
      win32?(),
      centos6?()
    ])
  end

  def project_path() do
    Path.expand("..", __ENV__.file)
  end

  def pkg_vsn() do
    project_path()
    |> Path.join("pkg-vsn.sh")
    |> System.cmd([])
    |> elem(0)
    |> String.trim()
    |> String.split("-")
    |> Enum.reverse()
    |> tl()
    |> Enum.reverse()
    |> fix_vsn()
    |> Enum.join("-")
  end

  # FIXME: remove hack
  defp fix_vsn([vsn | extras]) do
    if Version.parse(vsn) == :error do
      [vsn <> ".0" | extras]
    else
      [vsn | extras]
    end
  end

  defp win32?(),
    do: match?({:win_32, _}, :os.type())

  defp centos6?() do
    case File.read("/etc/centos-release") do
      {:ok, "CentOS release 6" <> _} ->
        true

      _ ->
        false
    end
  end

  defp build_without_quic?() do
    opt = System.get_env("BUILD_WITHOUT_QUIC", "false")

    String.downcase(opt) != "false"
  end

  defp from_rebar_to_eex_template(str) do
    # we must not consider surrounding space in the template var name
    # because some help strings contain informative variables that
    # should not be interpolated, and those have no spaces.
    Regex.replace(
      ~r/\{\{ ([a-zA-Z0-9_]+) \}\}/,
      str,
      "<%= \\g{1} %>"
    )
  end
end
