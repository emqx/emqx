defmodule EMQXUmbrella.MixProject do
  use Mix.Project

  @moduledoc """

  The purpose of this file is to configure the release of EMQX under
  Mix.  Since EMQX uses its own configuration conventions and startup
  procedures, one cannot simply use `iex -S mix`.  Instead, it's
  recommended to build and use the release.

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
    profile_info = check_profile!()
    version = pkg_vsn()

    [
      app: :emqx_mix,
      version: version,
      deps: deps(profile_info, version),
      releases: releases()
    ]
  end

  defp deps(profile_info, version) do
    # we need several overrides here because dependencies specify
    # other exact versions, and not ranges.
    [
      {:lc, github: "emqx/lc", tag: "0.3.2", override: true},
      {:redbug, "2.0.8"},
      {:covertool, github: "zmstone/covertool", tag: "2.0.4.1", override: true},
      {:typerefl, github: "ieQu1/typerefl", tag: "0.9.1", override: true},
      {:ehttpc, github: "emqx/ehttpc", tag: "0.4.11", override: true},
      {:gproc, github: "emqx/gproc", tag: "0.9.0.1", override: true},
      {:jiffy, github: "emqx/jiffy", tag: "1.0.5", override: true},
      {:cowboy, github: "emqx/cowboy", tag: "2.9.2", override: true},
      {:esockd, github: "emqx/esockd", tag: "5.9.7", override: true},
      {:rocksdb, github: "emqx/erlang-rocksdb", tag: "1.8.0-emqx-1", override: true},
      {:ekka, github: "emqx/ekka", tag: "0.15.16", override: true},
      {:gen_rpc, github: "emqx/gen_rpc", tag: "3.2.0", override: true},
      {:grpc, github: "emqx/grpc-erl", tag: "0.6.8", override: true},
      {:minirest, github: "emqx/minirest", tag: "1.3.13", override: true},
      {:ecpool, github: "emqx/ecpool", tag: "0.5.4", override: true},
      {:replayq, github: "emqx/replayq", tag: "0.3.7", override: true},
      {:pbkdf2, github: "emqx/erlang-pbkdf2", tag: "2.0.4", override: true},
      # maybe forbid to fetch quicer
      {:emqtt,
       github: "emqx/emqtt", tag: "1.9.0", override: true, system_env: maybe_no_quic_env()},
      {:rulesql, github: "emqx/rulesql", tag: "0.1.7"},
      {:observer_cli, "1.7.1"},
      {:system_monitor, github: "ieQu1/system_monitor", tag: "3.0.3"},
      {:telemetry, "1.1.0"},
      # in conflict by emqtt and hocon
      {:getopt, "1.0.2", override: true},
      {:snabbkaffe, github: "kafka4beam/snabbkaffe", tag: "1.0.8", override: true},
      {:hocon, github: "emqx/hocon", tag: "0.39.16", override: true},
      {:emqx_http_lib, github: "emqx/emqx_http_lib", tag: "0.5.3", override: true},
      {:esasl, github: "emqx/esasl", tag: "0.2.0"},
      {:jose, github: "potatosalad/erlang-jose", tag: "1.11.2"},
      # in conflict by ehttpc and emqtt
      {:gun, github: "emqx/gun", tag: "1.3.9", override: true},
      # in conflict by emqx_connector and system_monitor
      {:epgsql, github: "emqx/epgsql", tag: "4.7.0.1", override: true},
      # in conflict by emqx and observer_cli
      {:recon, github: "ferd/recon", tag: "2.5.1", override: true},
      {:jsx, github: "talentdeficit/jsx", tag: "v3.1.0", override: true},
      # in conflict by erlavro and rocketmq
      {:jsone, github: "emqx/jsone", tag: "1.7.1", override: true},
      # dependencies of dependencies; we choose specific refs to match
      # what rebar3 chooses.
      # in conflict by gun and emqtt
      {:cowlib,
       github: "ninenines/cowlib", ref: "c6553f8308a2ca5dcd69d845f0a7d098c40c3363", override: true},
      # in conflict by cowboy_swagger and cowboy
      {:ranch, github: "emqx/ranch", tag: "1.8.1-emqx", override: true},
      # in conflict by grpc and eetcd
      {:gpb, "4.19.9", override: true, runtime: false},
      {:hackney, github: "emqx/hackney", tag: "1.18.1-1", override: true},
      # set by hackney (dependency)
      {:ssl_verify_fun, "1.1.6", override: true},
      {:uuid, github: "okeuday/uuid", tag: "v2.0.6", override: true},
      {:quickrand, github: "okeuday/quickrand", tag: "v2.0.6", override: true},
      {:opentelemetry_api,
       github: "emqx/opentelemetry-erlang",
       sparse: "apps/opentelemetry_api",
       tag: "v1.3.0-emqx",
       override: true,
       runtime: false},
      {:opentelemetry,
       github: "emqx/opentelemetry-erlang",
       sparse: "apps/opentelemetry",
       tag: "v1.3.0-emqx",
       override: true,
       runtime: false},
      {:opentelemetry_api_experimental,
       github: "emqx/opentelemetry-erlang",
       sparse: "apps/opentelemetry_api_experimental",
       tag: "v1.3.0-emqx",
       override: true,
       runtime: false},
      {:opentelemetry_experimental,
       github: "emqx/opentelemetry-erlang",
       sparse: "apps/opentelemetry_experimental",
       tag: "v1.3.0-emqx",
       override: true,
       runtime: false},
      {:opentelemetry_exporter,
       github: "emqx/opentelemetry-erlang",
       sparse: "apps/opentelemetry_exporter",
       tag: "v1.3.0-emqx",
       override: true,
       runtime: false}
    ] ++
      emqx_apps(profile_info, version) ++
      enterprise_deps(profile_info) ++ bcrypt_dep() ++ jq_dep() ++ quicer_dep()
  end

  defp emqx_apps(profile_info, version) do
    apps = umbrella_apps(profile_info) ++ enterprise_apps(profile_info)
    set_emqx_app_system_env(apps, profile_info, version)
  end

  defp umbrella_apps(profile_info) do
    enterprise_apps = enterprise_umbrella_apps()

    "apps/*"
    |> Path.wildcard()
    |> Enum.map(fn path ->
      app =
        path
        |> Path.basename()
        |> String.to_atom()

      {app, path: path, manager: :rebar3, override: true}
    end)
    |> Enum.reject(fn dep_spec ->
      dep_spec
      |> elem(0)
      |> then(&MapSet.member?(enterprise_apps, &1))
    end)
    |> Enum.reject(fn {app, _} ->
      case profile_info do
        %{edition_type: :enterprise} ->
          app == :emqx_telemetry

        _ ->
          false
      end
    end)
  end

  defp enterprise_apps(_profile_info = %{edition_type: :enterprise}) do
    Enum.map(enterprise_umbrella_apps(), fn app_name ->
      path = "apps/#{app_name}"
      {app_name, path: path, manager: :rebar3, override: true}
    end)
  end

  defp enterprise_apps(_profile_info) do
    []
  end

  # need to remove those when listing `/apps/`...
  defp enterprise_umbrella_apps() do
    MapSet.new([
      :emqx_bridge_kafka,
      :emqx_bridge_gcp_pubsub,
      :emqx_bridge_cassandra,
      :emqx_bridge_opents,
      :emqx_bridge_dynamo,
      :emqx_bridge_greptimedb,
      :emqx_bridge_hstreamdb,
      :emqx_bridge_influxdb,
      :emqx_bridge_iotdb,
      :emqx_bridge_matrix,
      :emqx_bridge_mongodb,
      :emqx_bridge_mysql,
      :emqx_bridge_pgsql,
      :emqx_bridge_redis,
      :emqx_bridge_rocketmq,
      :emqx_bridge_tdengine,
      :emqx_bridge_timescale,
      :emqx_bridge_sqlserver,
      :emqx_bridge_pulsar,
      :emqx_oracle,
      :emqx_bridge_oracle,
      :emqx_bridge_rabbitmq,
      :emqx_bridge_clickhouse,
      :emqx_ft,
      :emqx_license,
      :emqx_s3,
      :emqx_schema_registry,
      :emqx_enterprise,
      :emqx_bridge_kinesis,
      :emqx_bridge_azure_event_hub,
      :emqx_gcp_device,
      :emqx_dashboard_rbac,
      :emqx_dashboard_sso
    ])
  end

  defp enterprise_deps(_profile_info = %{edition_type: :enterprise}) do
    [
      {:hstreamdb_erl, github: "hstreamdb/hstreamdb_erl", tag: "0.4.5+v0.16.1"},
      {:influxdb, github: "emqx/influxdb-client-erl", tag: "1.1.11", override: true},
      {:wolff, github: "kafka4beam/wolff", tag: "1.7.7"},
      {:kafka_protocol, github: "kafka4beam/kafka_protocol", tag: "4.1.3", override: true},
      {:brod_gssapi, github: "kafka4beam/brod_gssapi", tag: "v0.1.0"},
      {:brod, github: "kafka4beam/brod", tag: "3.16.8"},
      {:snappyer, "1.2.9", override: true},
      {:crc32cer, "0.1.8", override: true},
      {:supervisor3, "1.1.12", override: true},
      {:opentsdb, github: "emqx/opentsdb-client-erl", tag: "v0.5.1", override: true},
      {:greptimedb, github: "GreptimeTeam/greptimedb-client-erl", tag: "v0.1.2", override: true},
      # The following two are dependencies of rabbit_common. They are needed here to
      # make mix not complain about conflicting versions
      {:thoas, github: "emqx/thoas", tag: "v1.0.0", override: true},
      {:credentials_obfuscation,
       github: "emqx/credentials-obfuscation", tag: "v3.2.0", override: true},
      {:rabbit_common,
       github: "emqx/rabbitmq-server",
       tag: "v3.11.13-emqx",
       sparse: "deps/rabbit_common",
       override: true},
      {:amqp_client,
       github: "emqx/rabbitmq-server",
       tag: "v3.11.13-emqx",
       sparse: "deps/amqp_client",
       override: true}
    ]
  end

  defp enterprise_deps(_profile_info) do
    []
  end

  defp set_emqx_app_system_env(apps, profile_info, version) do
    system_env = emqx_app_system_env(profile_info, version) ++ maybe_no_quic_env()

    Enum.map(
      apps,
      fn {app, opts} ->
        {app,
         Keyword.update(
           opts,
           :system_env,
           system_env,
           &Keyword.merge(&1, system_env)
         )}
      end
    )
  end

  def emqx_app_system_env(profile_info, version) do
    erlc_options(profile_info, version)
    |> dump_as_erl()
    |> then(&[{"ERL_COMPILER_OPTIONS", &1}])
  end

  defp erlc_options(%{edition_type: edition_type}, version) do
    [
      :debug_info,
      {:compile_info, [{:emqx_vsn, String.to_charlist(version)}]},
      {:d, :EMQX_RELEASE_EDITION, erlang_edition(edition_type)},
      {:d, :snk_kind, :msg}
    ]
  end

  def maybe_no_quic_env() do
    if not enable_quicer?() do
      [{"BUILD_WITHOUT_QUIC", "true"}]
    else
      []
    end
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
            :emqx_mix,
            :emqx_gateway,
            :emqx_gateway_stomp,
            :emqx_gateway_mqttsn,
            :emqx_gateway_coap,
            :emqx_gateway_lwm2m,
            :emqx_gateway_exproto,
            :emqx_dashboard,
            :emqx_resource,
            :emqx_connector,
            :emqx_exhook,
            :emqx_bridge,
            :emqx_bridge_mqtt,
            :emqx_modules,
            :emqx_management,
            :emqx_retainer,
            :emqx_prometheus,
            :emqx_rule_engine,
            :emqx_auto_subscribe,
            :emqx_slow_subs,
            :emqx_plugins,
            :emqx_ft,
            :emqx_s3,
            :emqx_opentelemetry,
            :emqx_durable_storage,
            :rabbit_common
          ],
          steps: steps,
          strip_beams: false
        ]
      end
    ]
  end

  def applications(edition_type) do
    {:ok,
     [
       %{
         db_apps: db_apps,
         system_apps: system_apps,
         common_business_apps: common_business_apps,
         ee_business_apps: ee_business_apps,
         ce_business_apps: ce_business_apps
       }
     ]} = :file.consult("apps/emqx_machine/priv/reboot_lists.eterm")

    edition_specific_apps =
      if edition_type == :enterprise do
        ee_business_apps
      else
        ce_business_apps
      end

    business_apps = common_business_apps ++ edition_specific_apps

    excluded_apps = excluded_apps()

    system_apps =
      Enum.map(system_apps, fn app ->
        if is_atom(app), do: {app, :permanent}, else: app
      end)

    db_apps = Enum.map(db_apps, &{&1, :load})
    business_apps = Enum.map(business_apps, &{&1, :load})

    [system_apps, db_apps, [emqx_machine: :permanent], business_apps]
    |> List.flatten()
    |> Keyword.reject(fn {app, _type} -> app in excluded_apps end)
  end

  defp excluded_apps() do
    %{
      mnesia_rocksdb: enable_rocksdb?(),
      quicer: enable_quicer?(),
      bcrypt: enable_bcrypt?(),
      jq: enable_jq?(),
      observer: is_app?(:observer),
      os_mon: enable_os_mon?()
    }
    |> Enum.reject(&elem(&1, 1))
    |> Enum.map(&elem(&1, 0))
  end

  defp is_app?(name) do
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
      :emqx,
      :"emqx-pkg",
      :"emqx-enterprise",
      :"emqx-enterprise-pkg"
    ]

    if Mix.env() == :dev do
      env_profile = System.get_env("PROFILE")

      if env_profile do
        # copy from PROFILE env var
        System.get_env("PROFILE")
        |> String.to_atom()
        |> Mix.env()
      else
        IO.puts(
          IO.ANSI.format([
            :yellow,
            "Warning: env var PROFILE is unset; defaulting to emqx"
          ])
        )

        Mix.env(:emqx)
      end
    end

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
    plugins = Path.join(release.path, "plugins")

    Mix.Generator.create_directory(bin)
    Mix.Generator.create_directory(etc)
    Mix.Generator.create_directory(log)
    Mix.Generator.create_directory(plugins)
    Mix.Generator.create_directory(Path.join(etc, "certs"))

    Enum.each(
      ["mnesia", "configs", "patches", "scripts"],
      fn dir ->
        path = Path.join([release.path, "data", dir])
        Mix.Generator.create_directory(path)
      end
    )

    Mix.Generator.copy_file(
      "apps/emqx_auth/etc/acl.conf",
      Path.join(etc, "acl.conf"),
      force: overwrite?
    )

    # required by emqx_auth
    File.cp_r!(
      "apps/emqx/etc/certs",
      Path.join(etc, "certs")
    )

    profile = System.get_env("MIX_ENV")

    File.cp_r!(
      "rel/config/examples",
      Path.join(etc, "examples"),
      force: overwrite?
    )

    # copy /rel/config/ee-examples if profile is enterprise
    case profile do
      "emqx-enterprise" ->
        File.cp_r!(
          "rel/config/ee-examples",
          Path.join(etc, "examples"),
          force: overwrite?
        )

      _ ->
        :ok
    end

    # this is required by the produced escript / nodetool
    Mix.Generator.copy_file(
      Path.join(release.version_path, "start_clean.boot"),
      Path.join(bin, "no_dot_erlang.boot"),
      force: overwrite?
    )

    assigns = template_vars(release, release_type, package_type, edition_type)

    # This is generated by `scripts/merge-config.escript` or `make merge-config`
    # So, this should be run before the release.
    # TODO: run as a "compiler" step???
    render_template(
      "apps/emqx_conf/etc/emqx.conf.all",
      assigns,
      Path.join(etc, "emqx.conf")
    )

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

    Mix.Generator.copy_file(
      "bin/emqx_cluster_rescue",
      Path.join(bin, "emqx_cluster_rescue"),
      force: overwrite?
    )

    File.chmod!(Path.join(bin, "emqx_cluster_rescue"), 0o755)

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
      :io.format(handle, ~c"~tp.~n", [release_entry])
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
        "plugins",
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
      emqx_default_erlang_cookie: default_cookie(),
      emqx_configuration_doc: emqx_configuration_doc(edition_type),
      platform_data_dir: "data",
      platform_etc_dir: "etc",
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
      emqx_default_erlang_cookie: default_cookie(),
      emqx_configuration_doc: emqx_configuration_doc(edition_type),
      platform_data_dir: "/var/lib/emqx",
      platform_etc_dir: "/etc/emqx",
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

  defp default_cookie() do
    "emqx50elixir"
  end

  defp emqx_description(release_type, edition_type) do
    case {release_type, edition_type} do
      {:cloud, :enterprise} ->
        "EMQX Enterprise"

      {:cloud, :community} ->
        "EMQX"
    end
  end

  defp emqx_configuration_doc(:enterprise),
    do: "https://docs.emqx.com/en/enterprise/v5.0/configuration/configuration.html"

  defp emqx_configuration_doc(:community),
    do: "https://www.emqx.io/docs/en/v5.0/configuration/configuration.html"

  defp emqx_schema_mod(:enterprise), do: :emqx_enterprise_schema
  defp emqx_schema_mod(:community), do: :emqx_conf_schema

  defp bcrypt_dep() do
    if enable_bcrypt?(),
      do: [{:bcrypt, github: "emqx/erlang-bcrypt", tag: "0.6.1", override: true}],
      else: []
  end

  defp jq_dep() do
    if enable_jq?(),
      do: [{:jq, github: "emqx/jq", tag: "v0.3.10", override: true}],
      else: []
  end

  defp quicer_dep() do
    if enable_quicer?(),
      # in conflict with emqx and emqtt
      do: [{:quicer, github: "emqx/quic", tag: "0.0.202", override: true}],
      else: []
  end

  defp enable_bcrypt?() do
    not win32?()
  end

  defp enable_os_mon?() do
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

  defp enable_rocksdb?() do
    not Enum.any?([
      build_without_rocksdb?(),
      raspbian?()
    ]) or "1" == System.get_env("BUILD_WITH_ROCKSDB")
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

  defp raspbian?() do
    os_cmd("./scripts/get-distro.sh", []) =~ "raspbian"
  end

  defp build_without_jq?() do
    opt = System.get_env("BUILD_WITHOUT_JQ", "false")

    String.downcase(opt) != "false"
  end

  defp build_without_quic?() do
    opt = System.get_env("BUILD_WITHOUT_QUIC", "false")

    String.downcase(opt) != "false"
  end

  defp build_without_rocksdb?() do
    opt = System.get_env("BUILD_WITHOUT_ROCKSDB", "false")

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

  defp dump_as_erl(term) do
    term
    |> then(&:io_lib.format("~0p", [&1]))
    |> :erlang.iolist_to_binary()
  end

  defp erlang_edition(:community), do: :ce
  defp erlang_edition(:enterprise), do: :ee
end
