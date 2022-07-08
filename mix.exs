defmodule EMQXUmbrella.MixProject do
  use Mix.Project

  @moduledoc """

  The purpose of this file is to configure the release of EMQX under
  Mix.  Since EMQX uses its own configuration conventions and startup
  procedures, one cannot simply use `iex -S mix`.  Instead, it's
  recommendd to build and use the release.

  ## Profiles

  To control the profile and edition to build, we case split on the
  MIX_ENV value.

  The following profiles are valid:

    * `emqx`
    * `emqx-enterprise`
    * `emqx-pkg`
    * `emqx-enterprise-pkg`
    * `dev` -> same as `emqx`, for convenience

  ## Release Environment Variables

  The release build is controlled by a few environment variables.

    * `ELIXIR_MAKE_TAR` - If set to `yes`, will produce a `.tar.gz`
      tarball along with the release.
  """

  def project() do
    check_profile!()

    [
      app: :emqx_mix,
      version: pkg_vsn(),
      deps: deps(),
      releases: releases()
    ]
  end

  defp deps() do
    # we need several overrides here because dependencies specify
    # other exact versions, and not ranges.
    [
      {:lc, github: "emqx/lc", tag: "0.3.1"},
      {:redbug, "2.0.7"},
      {:typerefl, github: "ieQu1/typerefl", tag: "0.9.1", override: true},
      {:ehttpc, github: "emqx/ehttpc", tag: "0.2.1"},
      {:gproc, github: "uwiger/gproc", tag: "0.8.0", override: true},
      {:jiffy, github: "emqx/jiffy", tag: "1.0.5", override: true},
      {:cowboy, github: "emqx/cowboy", tag: "2.9.0", override: true},
      {:esockd, github: "emqx/esockd", tag: "5.9.3", override: true},
      {:ekka, github: "emqx/ekka", tag: "0.13.1", override: true},
      {:gen_rpc, github: "emqx/gen_rpc", tag: "2.8.1", override: true},
      {:minirest, github: "emqx/minirest", tag: "1.3.5", override: true},
      {:ecpool, github: "emqx/ecpool", tag: "0.5.2"},
      {:replayq, "0.3.4", override: true},
      {:pbkdf2, github: "emqx/erlang-pbkdf2", tag: "2.0.4", override: true},
      {:emqtt, github: "emqx/emqtt", tag: "1.6.0", override: true},
      {:rulesql, github: "emqx/rulesql", tag: "0.1.4"},
      {:observer_cli, "1.7.1"},
      {:system_monitor, github: "ieQu1/system_monitor", tag: "3.0.3"},
      # in conflict by emqtt and hocon
      {:getopt, "1.0.2", override: true},
      {:snabbkaffe, github: "kafka4beam/snabbkaffe", tag: "1.0.0", override: true},
      {:hocon, github: "emqx/hocon", tag: "0.28.3", override: true},
      {:emqx_http_lib, github: "emqx/emqx_http_lib", tag: "0.5.1", override: true},
      {:esasl, github: "emqx/esasl", tag: "0.2.0"},
      {:jose, github: "potatosalad/erlang-jose", tag: "1.11.2"},
      # in conflict by ehttpc and emqtt
      {:gun, github: "emqx/gun", tag: "1.3.7", override: true},
      # in conflict by emqx_connectior and system_monitor
      {:epgsql, github: "emqx/epgsql", tag: "4.7-emqx.2", override: true},
      # in conflict by mongodb and eredis_cluster
      {:poolboy, github: "emqx/poolboy", tag: "1.5.2", override: true},
      # in conflict by emqx and observer_cli
      {:recon, github: "ferd/recon", tag: "2.5.1", override: true},
      {:jsx, github: "talentdeficit/jsx", tag: "v3.1.0", override: true},
      # dependencies of dependencies; we choose specific refs to match
      # what rebar3 chooses.
      # in conflict by gun and emqtt
      {:cowlib,
       github: "ninenines/cowlib", ref: "c6553f8308a2ca5dcd69d845f0a7d098c40c3363", override: true},
      # in conflict by cowboy_swagger and cowboy
      {:ranch,
       github: "ninenines/ranch", ref: "a692f44567034dacf5efcaa24a24183788594eb7", override: true},
      # in conflict by grpc and eetcd
      {:gpb, "4.11.2", override: true, runtime: false}
    ] ++ umbrella_apps() ++ bcrypt_dep() ++ jq_dep() ++ quicer_dep()
  end

  defp umbrella_apps() do
    "apps/*"
    |> Path.wildcard()
    |> Enum.map(fn path ->
      app =
        path
        |> String.trim_leading("apps/")
        |> String.to_atom()

      {app, path: path, manager: :rebar3, override: true}
    end)
  end

  defp releases() do
    [
      emqx: fn ->
        %{
          release_type: release_type,
          package_type: package_type,
          edition_type: edition_type
        } = check_profile!()

        base_steps = [
          &make_docs(&1),
          :assemble,
          &create_RELEASES/1,
          &copy_files(&1, release_type, package_type, edition_type),
          &copy_escript(&1, "nodetool"),
          &copy_escript(&1, "install_upgrade.escript")
        ]

        steps =
          if System.get_env("ELIXIR_MAKE_TAR") == "yes" do
            base_steps ++ [&prepare_tar_overlays/1, :tar]
          else
            base_steps
          end

        [
          applications: applications(edition_type),
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
            :emqx_auto_subscribe,
            :emqx_slow_subs,
            :emqx_plugins
          ],
          steps: steps,
          strip_beams: false
        ]
      end
    ]
  end

  def applications(edition_type) do
    [
      crypto: :permanent,
      public_key: :permanent,
      asn1: :permanent,
      syntax_tools: :permanent,
      ssl: :permanent,
      os_mon: :permanent,
      inets: :permanent,
      compiler: :permanent,
      runtime_tools: :permanent,
      redbug: :permanent,
      xmerl: :permanent,
      hocon: :load,
      emqx: :load,
      emqx_conf: :load,
      emqx_machine: :permanent,
      mria: :load,
      mnesia: :load,
      ekka: :load,
      emqx_plugin_libs: :load,
      esasl: :load,
      observer_cli: :permanent,
      system_monitor: :load,
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
      emqx_retainer: :permanent,
      emqx_statsd: :permanent,
      emqx_prometheus: :permanent,
      emqx_psk: :permanent,
      emqx_slow_subs: :permanent,
      emqx_plugins: :permanent,
      emqx_mix: :none
    ] ++
      if(enable_quicer?(), do: [quicer: :permanent], else: []) ++
      if(enable_bcrypt?(), do: [bcrypt: :permanent], else: []) ++
      if(enable_jq?(), do: [jq: :permanent], else: []) ++
      if(is_app(:observer),
        do: [observer: :load],
        else: []
      ) ++
      if(edition_type == :enterprise,
        do: [
          emqx_license: :permanent,
          emqx_enterprise_conf: :load
        ],
        else: []
      )
  end

  defp is_app(name) do
    case Application.load(name) do
      :ok ->
        true

      {:error, {:already_loaded, _}} ->
        true

      _ ->
        false
    end
  end

  def check_profile!() do
    valid_envs = [
      :dev,
      :emqx,
      :"emqx-pkg",
      :"emqx-enterprise",
      :"emqx-enterprise-pkg"
    ]

    if Mix.env() not in valid_envs do
      formatted_envs =
        valid_envs
        |> Enum.map(&"  * #{&1}")
        |> Enum.join("\n")

      Mix.raise("""
      Invalid env #{Mix.env()}.  Valid options are:
      #{formatted_envs}
      """)
    end

    {
      release_type,
      package_type,
      edition_type
    } =
      case Mix.env() do
        :dev ->
          {:cloud, :bin, :community}

        :emqx ->
          {:cloud, :bin, :community}

        :"emqx-enterprise" ->
          {:cloud, :bin, :enterprise}

        :"emqx-pkg" ->
          {:cloud, :pkg, :community}

        :"emqx-enterprise-pkg" ->
          {:cloud, :pkg, :enterprise}
      end

    normalize_env!()

    %{
      release_type: release_type,
      package_type: package_type,
      edition_type: edition_type
    }
  end

  #############################################################################
  #  Custom Steps
  #############################################################################

  defp make_docs(release) do
    profile = System.get_env("MIX_ENV")
    os_cmd("build", [profile, "docs"])
    release
  end

  defp copy_files(release, release_type, package_type, edition_type) do
    overwrite? = Keyword.get(release.options, :overwrite, false)

    bin = Path.join(release.path, "bin")
    etc = Path.join(release.path, "etc")
    log = Path.join(release.path, "log")

    Mix.Generator.create_directory(bin)
    Mix.Generator.create_directory(etc)
    Mix.Generator.create_directory(log)
    Mix.Generator.create_directory(Path.join(etc, "certs"))

    Enum.each(
      ["mnesia", "configs", "patches", "scripts"],
      fn dir ->
        path = Path.join([release.path, "data", dir])
        Mix.Generator.create_directory(path)
      end
    )

    Mix.Generator.copy_file(
      "apps/emqx_authz/etc/acl.conf",
      Path.join(etc, "acl.conf"),
      force: overwrite?
    )

    # required by emqx_authz
    File.cp_r!(
      "apps/emqx/etc/certs",
      Path.join(etc, "certs")
    )

    Mix.Generator.copy_file(
      "apps/emqx_dashboard/etc/emqx.conf.en.example",
      Path.join(etc, "emqx-example.conf"),
      force: overwrite?
    )

    # this is required by the produced escript / nodetool
    Mix.Generator.copy_file(
      Path.join(release.version_path, "start_clean.boot"),
      Path.join(bin, "no_dot_erlang.boot"),
      force: overwrite?
    )

    assigns = template_vars(release, release_type, package_type, edition_type)

    # This is generated by `scripts/merge-config.escript` or `make
    # conf-segs`.  So, this should be run before the release.
    # TODO: run as a "compiler" step???
    render_template(
      "apps/emqx_conf/etc/emqx.conf.all",
      assigns,
      Path.join(etc, "emqx.conf")
    )

    if edition_type == :enterprise do
      render_template(
        "apps/emqx_conf/etc/emqx_enterprise.conf.all",
        assigns,
        Path.join(etc, "emqx_enterprise.conf")
      )
    end

    render_template(
      "rel/emqx_vars",
      assigns,
      Path.join([release.path, "releases", "emqx_vars"])
    )

    vm_args_template_path =
      case release_type do
        :cloud ->
          "apps/emqx/etc/vm.args.cloud"
      end

    render_template(
      vm_args_template_path,
      assigns,
      [
        Path.join(etc, "vm.args"),
        Path.join(release.version_path, "vm.args")
      ]
    )

    for name <- [
          "emqx",
          "emqx_ctl"
        ] do
      Mix.Generator.copy_file(
        "bin/#{name}",
        Path.join(bin, name),
        force: overwrite?
      )

      # Files with the version appended are expected by the release
      # upgrade script `install_upgrade.escript`
      Mix.Generator.copy_file(
        Path.join(bin, name),
        Path.join(bin, name <> "-#{release.version}"),
        force: overwrite?
      )
    end

    for base_name <- ["emqx", "emqx_ctl"],
        suffix <- ["", "-#{release.version}"] do
      name = base_name <> suffix
      File.chmod!(Path.join(bin, name), 0o755)
    end

    Mix.Generator.copy_file(
      "bin/node_dump",
      Path.join(bin, "node_dump"),
      force: overwrite?
    )

    File.chmod!(Path.join(bin, "node_dump"), 0o755)

    render_template(
      "rel/BUILD_INFO",
      assigns,
      Path.join(release.version_path, "BUILD_INFO")
    )

    release
  end

  defp render_template(template, assigns, target) when is_binary(target) do
    render_template(template, assigns, [target])
  end

  defp render_template(template, assigns, tartgets) when is_list(tartgets) do
    rendered =
      File.read!(template)
      |> from_rebar_to_eex_template()
      |> EEx.eval_string(assigns)

    for target <- tartgets do
      File.write!(target, rendered)
    end
  end

  # needed by nodetool and by release_handler
  defp create_RELEASES(release) do
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

  defp copy_escript(release, escript_name) do
    [shebang, rest] =
      "bin/#{escript_name}"
      |> File.read!()
      |> String.split("\n", parts: 2)

    # the elixir version of escript + start.boot required the boot_var
    # RELEASE_LIB to be defined.
    boot_var = "%%!-boot_var RELEASE_LIB $RUNNER_ROOT_DIR/lib"

    # Files with the version appended are expected by the release
    # upgrade script `install_upgrade.escript`
    Enum.each(
      [escript_name, escript_name <> "-" <> release.version],
      fn name ->
        path = Path.join([release.path, "bin", name])
        File.write!(path, [shebang, "\n", boot_var, "\n", rest])
      end
    )

    release
  end

  # The `:tar` built-in step in Mix Release does not currently add the
  # `etc` directory into the resulting tarball.  The workaround is to
  # add those to the `:overlays` key before running `:tar`.
  # See: https://hexdocs.pm/mix/1.13.4/Mix.Release.html#__struct__/0
  defp prepare_tar_overlays(release) do
    Map.update!(
      release,
      :overlays,
      &[
        "etc",
        "data",
        "bin/node_dump"
        | &1
      ]
    )
  end

  #############################################################################
  #  Helper functions
  #############################################################################

  defp template_vars(release, release_type, :bin = _package_type, edition_type) do
    [
      platform_data_dir: "data",
      platform_etc_dir: "etc",
      platform_log_dir: "log",
      platform_plugins_dir: "plugins",
      runner_bin_dir: "$RUNNER_ROOT_DIR/bin",
      emqx_etc_dir: "$RUNNER_ROOT_DIR/etc",
      runner_lib_dir: "$RUNNER_ROOT_DIR/lib",
      runner_log_dir: "$RUNNER_ROOT_DIR/log",
      runner_user: "",
      release_version: release.version,
      erts_vsn: release.erts_version,
      # FIXME: this is empty in `make emqx` ???
      erl_opts: "",
      emqx_description: emqx_description(release_type, edition_type),
      emqx_schema_mod: emqx_schema_mod(edition_type),
      is_elixir: "yes",
      is_enterprise: if(edition_type == :enterprise, do: "yes", else: "no")
    ] ++ build_info()
  end

  defp template_vars(release, release_type, :pkg = _package_type, edition_type) do
    [
      platform_data_dir: "/var/lib/emqx",
      platform_etc_dir: "/etc/emqx",
      platform_log_dir: "/var/log/emqx",
      platform_plugins_dir: "/var/lib/emqx/plugins",
      runner_bin_dir: "/usr/bin",
      emqx_etc_dir: "/etc/emqx",
      runner_lib_dir: "$RUNNER_ROOT_DIR/lib",
      runner_log_dir: "/var/log/emqx",
      runner_user: "emqx",
      release_version: release.version,
      erts_vsn: release.erts_version,
      # FIXME: this is empty in `make emqx` ???
      erl_opts: "",
      emqx_description: emqx_description(release_type, edition_type),
      emqx_schema_mod: emqx_schema_mod(edition_type),
      is_elixir: "yes",
      is_enterprise: if(edition_type == :enterprise, do: "yes", else: "no")
    ] ++ build_info()
  end

  defp emqx_description(release_type, edition_type) do
    case {release_type, edition_type} do
      {:cloud, :enterprise} ->
        "EMQX Enterprise"

      {:cloud, :community} ->
        "EMQX"
    end
  end

  defp emqx_schema_mod(:enterprise), do: :emqx_enterprise_conf_schema
  defp emqx_schema_mod(:community), do: :emqx_conf_schema

  defp bcrypt_dep() do
    if enable_bcrypt?(),
      do: [{:bcrypt, github: "emqx/erlang-bcrypt", tag: "0.6.0", override: true}],
      else: []
  end

  defp jq_dep() do
    if enable_jq?(),
      do: [{:jq, github: "emqx/jq", tag: "v0.3.5", override: true}],
      else: []
  end

  defp quicer_dep() do
    if enable_quicer?(),
      # in conflict with emqx and emqtt
      do: [{:quicer, github: "emqx/quic", tag: "0.0.14", override: true}],
      else: []
  end

  defp enable_bcrypt?() do
    not win32?()
  end

  defp enable_jq?() do
    not Enum.any?([
      build_without_jq?(),
      win32?()
    ]) or "1" == System.get_env("BUILD_WITH_JQ")
  end

  defp enable_quicer?() do
    not Enum.any?([
      build_without_quic?(),
      win32?(),
      centos6?(),
      macos?()
    ]) or "1" == System.get_env("BUILD_WITH_QUIC")
  end

  defp pkg_vsn() do
    %{edition_type: edition_type} = check_profile!()
    basedir = Path.dirname(__ENV__.file)
    script = Path.join(basedir, "pkg-vsn.sh")
    os_cmd(script, [Atom.to_string(edition_type)])
  end

  defp os_cmd(script, args) do
    {str, 0} = System.cmd("bash", [script | args])
    String.trim(str)
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

  defp macos?() do
    {:unix, :darwin} == :os.type()
  end

  defp build_without_jq?() do
    opt = System.get_env("BUILD_WITHOUT_JQ", "false")

    String.downcase(opt) != "false"
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

  defp build_info() do
    [
      build_info_arch: to_string(:erlang.system_info(:system_architecture)),
      build_info_wordsize: wordsize(),
      build_info_os: os_cmd("./scripts/get-distro.sh", []),
      build_info_erlang: otp_release(),
      build_info_elixir: System.version(),
      build_info_relform: System.get_env("EMQX_REL_FORM", "tgz")
    ]
  end

  # https://github.com/erlang/rebar3/blob/e3108ac187b88fff01eca6001a856283a3e0ec87/src/rebar_utils.erl#L142
  defp wordsize() do
    size =
      try do
        :erlang.system_info({:wordsize, :external})
      rescue
        ErlangError ->
          :erlang.system_info(:wordsize)
      end

    to_string(8 * size)
  end

  defp normalize_env!() do
    env =
      case Mix.env() do
        :dev ->
          :emqx

        env ->
          env
      end

    Mix.env(env)
  end

  # As from Erlang/OTP 17, the OTP release number corresponds to the
  # major OTP version number. No erlang:system_info() argument gives
  # the exact OTP version.
  # https://www.erlang.org/doc/man/erlang.html#system_info_otp_release
  # https://github.com/erlang/rebar3/blob/e3108ac187b88fff01eca6001a856283a3e0ec87/src/rebar_utils.erl#L572-L577
  defp otp_release() do
    major_version = System.otp_release()
    root_dir = to_string(:code.root_dir())

    [root_dir, "releases", major_version, "OTP_VERSION"]
    |> Path.join()
    |> File.read()
    |> case do
      {:error, _} ->
        major_version

      {:ok, version} ->
        version
        |> String.trim()
        |> String.split("**")
        |> List.first()
    end
  end
end
