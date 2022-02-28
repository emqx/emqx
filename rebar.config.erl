-module('rebar.config').

-export([do/2]).

do(Dir, CONFIG) ->
    ok = assert_otp(),
    case iolist_to_binary(Dir) of
        <<".">> ->
            C1 = deps(CONFIG),
            Config = dialyzer(C1),
            maybe_dump(Config ++ [{overrides, overrides()}] ++ coveralls() ++ config());
        _ ->
            CONFIG
    end.

assert_otp() ->
    Oldest = 23,
    Latest = 24,
    OtpRelease = list_to_integer(erlang:system_info(otp_release)),
    case OtpRelease < Oldest orelse OtpRelease > Latest of
        true ->
            io:format(standard_error, "ERROR: Erlang/OTP version ~p found. min=~p, recommended=~p~n",
                      [OtpRelease, Oldest, Latest]),
            halt(1);
        false when OtpRelease =/= Latest ->
            io:format("WARNING: Erlang/OTP version ~p found, recommended==~p~n",
                      [OtpRelease, Latest]);
        false ->
            ok
    end.

bcrypt() ->
    {bcrypt, {git, "https://github.com/emqx/erlang-bcrypt.git", {tag, "0.6.0"}}}.

quicer() ->
    {quicer, {git, "https://github.com/emqx/quic.git", {tag, "0.0.9"}}}.

deps(Config) ->
    {deps, OldDeps} = lists:keyfind(deps, 1, Config),
    MoreDeps = [bcrypt() || provide_bcrypt_dep()] ++
        [quicer() || is_quicer_supported()],
    lists:keystore(deps, 1, Config, {deps, OldDeps ++ MoreDeps}).

overrides() ->
    [ {add, [ {extra_src_dirs, [{"etc", [{recursive, true}]}]}
            ]}
    ] ++ snabbkaffe_overrides().

%% Temporary workaround for a rebar3 erl_opts duplication
%% bug. Ideally, we want to set this define globally
snabbkaffe_overrides() ->
    Apps = [snabbkaffe, ekka, mria],
    [{add, App, [{erl_opts, [{d, snk_kind, msg}]}]} || App <- Apps].

config() ->
    [ {cover_enabled, is_cover_enabled()}
    , {profiles, profiles()}
    , {plugins, plugins()}
    ].

is_cover_enabled() ->
    case os:getenv("ENABLE_COVER_COMPILE") of
        "1"-> true;
        "true" -> true;
        _ -> false
    end.

is_enterprise(ce) -> false;
is_enterprise(ee) -> true.

is_quicer_supported() ->
    not (false =/= os:getenv("BUILD_WITHOUT_QUIC") orelse
         is_win32() orelse is_centos_6()
        ).

is_centos_6() ->
    %% reason:
    %% glibc is too old
    case file:read_file("/etc/centos-release") of
        {ok, <<"CentOS release 6", _/binary >>} ->
            true;
        _ ->
            false
    end.

is_win32() ->
    win32 =:= element(1, os:type()).

project_app_dirs(Edition) ->
    ["apps/*"] ++
    case is_enterprise(Edition) of
        true -> ["lib-ee/*"];
        false -> []
    end.

plugins() ->
    [ {relup_helper,{git,"https://github.com/emqx/relup_helper", {tag, "2.0.0"}}}
    , {er_coap_client, {git, "https://github.com/emqx/er_coap_client", {tag, "v1.0.4"}}}
      %% emqx main project does not require port-compiler
      %% pin at root level for deterministic
    , {pc, {git, "https://github.com/emqx/port_compiler.git", {tag, "v1.11.1"}}}
    ]
    %% test plugins are concatenated to default profile plugins
    %% otherwise rebar3 test profile runs are super slow
    ++ test_plugins().

test_plugins() ->
    [ {rebar3_proper, "0.12.1"}
    , {coveralls, {git, "https://github.com/emqx/coveralls-erl", {tag, "v2.2.0-emqx-1"}}}
    ].

test_deps() ->
    [ {bbmustache, "1.10.0"}
    , {meck, "0.9.2"}
    , {proper, "1.4.0"}
    ].

common_compile_opts(Vsn) ->
    [ debug_info % always include debug_info
    , {compile_info, [{emqx_vsn, Vsn}]}
    ] ++
    [{d, 'EMQX_BENCHMARK'} || os:getenv("EMQX_BENCHMARK") =:= "1" ].

prod_compile_opts(Vsn) ->
    [ compressed
    , deterministic
    , warnings_as_errors
    | common_compile_opts(Vsn)
    ].

prod_overrides() ->
    [{add, [ {erl_opts, [deterministic]}]}].

profiles() ->
    profiles_ce() ++ profiles_ee() ++ profiles_dev().

profiles_ce() ->
    Vsn = get_vsn(emqx),
    [ {'emqx',
       [ {erl_opts, prod_compile_opts(Vsn)}
       , {relx, relx(Vsn, cloud, bin, ce)}
       , {overrides, prod_overrides()}
       , {project_app_dirs, project_app_dirs(ce)}
       , {post_hooks, [{compile, "bash build emqx doc"}]}
       ]}
    , {'emqx-pkg',
       [ {erl_opts, prod_compile_opts(Vsn)}
       , {relx, relx(Vsn, cloud, pkg, ce)}
       , {overrides, prod_overrides()}
       , {project_app_dirs, project_app_dirs(ce)}
       , {post_hooks, [{compile, "bash build emqx-pkg doc"}]}
       ]}
    , {'emqx-edge',
       [ {erl_opts, prod_compile_opts(Vsn)}
       , {relx, relx(Vsn, edge, bin, ce)}
       , {overrides, prod_overrides()}
       , {project_app_dirs, project_app_dirs(ce)}
       , {post_hooks, [{compile, "bash build emqx-edge doc"}]}
       ]}
    , {'emqx-edge-pkg',
       [ {erl_opts, prod_compile_opts(Vsn)}
       , {relx, relx(Vsn, edge, pkg, ce)}
       , {overrides, prod_overrides()}
       , {project_app_dirs, project_app_dirs(ce)}
       , {post_hooks, [{compile, "bash build emqx-edge-pkg doc"}]}
       ]}
    ].

profiles_ee() ->
    Vsn = get_vsn('emqx-enterprise'),
    EE = {d, 'EMQX_ENTERPRISE'},
    [ {'emqx-enterprise',
       [ {erl_opts, [EE | prod_compile_opts(Vsn)]}
       , {relx, relx(Vsn, cloud, bin, ee)}
       , {overrides, prod_overrides()}
       , {project_app_dirs, project_app_dirs(ee)}
       , {post_hooks, [{compile, "bash build emqx-enterprise doc"}]}
       ]}
    , {'emqx-enterprise-pkg',
       [ {erl_opts, [EE | prod_compile_opts(Vsn)]}
       , {relx, relx(Vsn, cloud, pkg, ee)}
       , {overrides, prod_overrides()}
       , {project_app_dirs, project_app_dirs(ee)}
       , {post_hooks, [{compile, "bash build emqx-enterprise-pkg doc"}]}
       ]}
    ].

%% EE has more files than CE, always test/check with EE options.
profiles_dev() ->
    Vsn = get_vsn('emqx-enterprise'),
    EE = {d, 'EMQX_ENTERPRISE'},
    [ {check,
       [ {erl_opts, [EE | common_compile_opts(Vsn)]}
       , {project_app_dirs, project_app_dirs(ee)}
       ]}
    , {test,
       [ {deps, test_deps()}
       , {erl_opts, [EE | common_compile_opts(Vsn) ++ erl_opts_i()]}
       , {extra_src_dirs, [{"test", [{recursive, true}]}]}
       , {project_app_dirs, project_app_dirs(ee)}
       ]}
    ].

%% RelType: cloud (full size) | edge (slim size)
%% PkgType: bin | pkg
%% Edition: ce (community) | ee (enterprise)
relx(Vsn, RelType, PkgType, Edition) ->
    [ {include_src,false}
    , {include_erts, true}
    , {extended_start_script,false}
    , {generate_start_script,false}
    , {sys_config,false}
    , {vm_args,false}
    , {release, {emqx, Vsn}, relx_apps(RelType, Edition)}
    , {overlay, relx_overlay(RelType, Edition)}
    , {overlay_vars, [ {built_on_arch, rebar_utils:get_arch()}
                     , {emqx_description, emqx_description(RelType, Edition)}
                     | overlay_vars(RelType, PkgType, Edition)]}
    ].

emqx_description(cloud, ee) -> "EMQX Enterprise Edition";
emqx_description(cloud, ce) -> "EMQX Community Edition";
emqx_description(edge, ce)  -> "EMQX Edge Edition".

overlay_vars(RelType, PkgType, Edition) ->
    overlay_vars_rel(RelType)
    ++ overlay_vars_pkg(PkgType)
    ++ overlay_vars_edition(Edition).

%% vars per release type, cloud or edge
overlay_vars_rel(RelType) ->
    VmArgs = case RelType of
                 cloud -> "vm.args";
                 edge -> "vm.args.edge"
             end,

    [ {vm_args_file, VmArgs}
    ].

overlay_vars_edition(ce) ->
    [ {emqx_schema_mod, emqx_conf_schema}
    , {is_enterprise, "no"}
    , {emqx_machine_boot_apps, emqx_machine_boot_app_list(ce)}
    ];
overlay_vars_edition(ee) ->
    [ {emqx_schema_mod, emqx_enterprise_conf_schema}
    , {is_enterprise, "yes"}
    , {emqx_machine_boot_apps, emqx_machine_boot_app_list(ee)}
    ].

%% vars per packaging type, bin(zip/tar.gz/docker) or pkg(rpm/deb)
overlay_vars_pkg(bin) ->
    [ {platform_bin_dir, "bin"}
    , {platform_data_dir, "data"}
    , {platform_etc_dir, "etc"}
    , {platform_lib_dir, "lib"}
    , {platform_log_dir, "log"}
    , {platform_plugins_dir, "plugins"}
    , {runner_root_dir, "$(cd $(dirname $(readlink $0 || echo $0))/..; pwd -P)"}
    , {runner_bin_dir, "$RUNNER_ROOT_DIR/bin"}
    , {runner_etc_dir, "$RUNNER_ROOT_DIR/etc"}
    , {runner_lib_dir, "$RUNNER_ROOT_DIR/lib"}
    , {runner_log_dir, "$RUNNER_ROOT_DIR/log"}
    , {runner_user, ""}
    , {is_elixir, "no"}
    ];
overlay_vars_pkg(pkg) ->
    [ {platform_bin_dir, ""}
    , {platform_data_dir, "/var/lib/emqx"}
    , {platform_etc_dir, "/etc/emqx"}
    , {platform_lib_dir, ""}
    , {platform_log_dir, "/var/log/emqx"}
    , {platform_plugins_dir, "/var/lib/emqx/plugins"}
    , {runner_root_dir, "/usr/lib/emqx"}
    , {runner_bin_dir, "/usr/bin"}
    , {runner_etc_dir, "/etc/emqx"}
    , {runner_lib_dir, "$RUNNER_ROOT_DIR/lib"}
    , {runner_log_dir, "/var/log/emqx"}
    , {runner_user, "emqx"}
    , {is_elixir, "no"}
    ].

relx_apps(ReleaseType, Edition) ->
    [ kernel
    , sasl
    , crypto
    , public_key
    , asn1
    , syntax_tools
    , ssl
    , os_mon
    , inets
    , compiler
    , runtime_tools
    , redbug
    , {hocon, load}
    , {emqx, load} % started by emqx_machine
    , {emqx_conf, load}
    , emqx_machine
    , {mnesia, load}
    , {ekka, load}
    , {emqx_plugin_libs, load}
    , {esasl, load}
    , observer_cli
    , {system_monitor, load} % started by emqx_machine
    , emqx_http_lib
    , emqx_resource
    , emqx_connector
    , emqx_authn
    , emqx_authz
    , emqx_auto_subscribe
    , emqx_gateway
    , emqx_exhook
    , emqx_bridge
    , emqx_rule_engine
    , emqx_modules
    , emqx_management
    , emqx_dashboard
    , emqx_retainer
    , emqx_statsd
    , emqx_prometheus
    , emqx_psk
    , emqx_slow_subs
    , emqx_plugins
    ]
    ++ [quicer || is_quicer_supported()]
    ++ [bcrypt || provide_bcrypt_release(ReleaseType)]
    ++ relx_apps_per_rel(ReleaseType)
    ++ relx_additional_apps(ReleaseType, Edition).

relx_apps_per_rel(cloud) ->
    [ xmerl
    | [{observer, load} || is_app(observer)]
    ];
relx_apps_per_rel(edge) ->
    [].

is_app(Name) ->
    case application:load(Name) of
        ok -> true;
        {error,{already_loaded, _}} -> true;
        _ -> false
    end.

relx_additional_apps(ReleaseType, Edition) ->
    relx_plugin_apps_per_rel(ReleaseType)
    ++ relx_apps_per_edition(Edition).

relx_plugin_apps_per_rel(cloud) ->
    [];
relx_plugin_apps_per_rel(edge) ->
    [].

relx_apps_per_edition(ee) ->
    [ emqx_license
    , {emqx_enterprise_conf, load}
    ];

relx_apps_per_edition(ce) -> [].

emqx_machine_boot_apps(ce) ->
    [ emqx_prometheus
    , emqx_modules
    , emqx_dashboard
    , emqx_connector
    , emqx_gateway
    , emqx_statsd
    , emqx_resource
    , emqx_rule_engine
    , emqx_bridge
    , emqx_plugin_libs
    , emqx_management
    , emqx_retainer
    , emqx_exhook
    , emqx_authn
    , emqx_authz
    , emqx_plugin
    ];

emqx_machine_boot_apps(ee) ->
    emqx_machine_boot_apps(ce) ++
    [].

emqx_machine_boot_app_list(Edition) ->
    string:join(
      [atom_to_list(AppName) || AppName <- emqx_machine_boot_apps(Edition)],
      ", ").

relx_overlay(ReleaseType, Edition) ->
    [ {mkdir, "log/"}
    , {mkdir, "data/"}
    , {mkdir, "plugins"}
    , {mkdir, "data/mnesia"}
    , {mkdir, "data/configs"}
    , {mkdir, "data/patches"}
    , {mkdir, "data/scripts"}
    , {template, "rel/emqx_vars", "releases/emqx_vars"}
    , {template, "rel/BUILT_ON", "releases/{{release_version}}/BUILT_ON"}
    , {copy, "bin/emqx", "bin/emqx"}
    , {copy, "bin/emqx_ctl", "bin/emqx_ctl"}
    , {copy, "bin/node_dump", "bin/node_dump"}
    , {copy, "bin/install_upgrade.escript", "bin/install_upgrade.escript"}
    , {copy, "bin/emqx", "bin/emqx-{{release_version}}"} %% for relup
    , {copy, "bin/emqx_ctl", "bin/emqx_ctl-{{release_version}}"} %% for relup
    , {copy, "bin/install_upgrade.escript", "bin/install_upgrade.escript-{{release_version}}"} %% for relup
    , {copy, "apps/emqx_gateway/src/lwm2m/lwm2m_xml", "etc/lwm2m_xml"}
    , {copy, "apps/emqx_authz/etc/acl.conf", "etc/acl.conf"}
    , {template, "bin/emqx.cmd", "bin/emqx.cmd"}
    , {template, "bin/emqx_ctl.cmd", "bin/emqx_ctl.cmd"}
    , {copy, "bin/nodetool", "bin/nodetool"}
    , {copy, "bin/nodetool", "bin/nodetool-{{release_version}}"}
    ] ++ etc_overlay(ReleaseType, Edition).

etc_overlay(ReleaseType, Edition) ->
    Templates = emqx_etc_overlay(ReleaseType, Edition),
    [ {mkdir, "etc/"}
    , {copy, "{{base_dir}}/lib/emqx/etc/certs","etc/"}
    ] ++
    lists:map(
      fun({From, To}) -> {template, From, To};
         (FromTo)     -> {template, FromTo, FromTo}
      end, Templates).

emqx_etc_overlay(ReleaseType, Edition) ->
    emqx_etc_overlay_per_rel(ReleaseType)
    ++ emqx_etc_overlay_per_edition(Edition)
    ++ emqx_etc_overlay_common().

emqx_etc_overlay_per_rel(cloud) ->
    [ {"{{base_dir}}/lib/emqx/etc/emqx_cloud/vm.args","etc/vm.args"}
    ];
emqx_etc_overlay_per_rel(edge) ->
    [ {"{{base_dir}}/lib/emqx/etc/emqx_edge/vm.args","etc/vm.args"}
    ].

emqx_etc_overlay_common() ->
    [ {"{{base_dir}}/lib/emqx/etc/ssl_dist.conf", "etc/ssl_dist.conf"}
    ].

emqx_etc_overlay_per_edition(ce) ->
    [ {"{{base_dir}}/lib/emqx_conf/etc/emqx.conf.all", "etc/emqx.conf"}
    ];
emqx_etc_overlay_per_edition(ee) ->
    [ {"{{base_dir}}/lib/emqx_conf/etc/emqx_enterprise.conf.all", "etc/emqx_enterprise.conf"}
    , {"{{base_dir}}/lib/emqx_conf/etc/emqx.conf.all", "etc/emqx.conf"}
    ].

get_vsn(Profile) ->
    %% to make it compatible to Linux and Windows,
    %% we must use bash to execute the bash file
    %% because "./" will not be recognized as an internal or external command
    PkgVsn = os:cmd("bash pkg-vsn.sh " ++ atom_to_list(Profile)),
    re:replace(PkgVsn, "\n", "", [{return ,list}]).

maybe_dump(Config) ->
    is_debug() andalso file:write_file("rebar.config.rendered", [io_lib:format("~p.\n", [I]) || I <- Config]),
    Config.

is_debug() -> is_debug("DEBUG") orelse is_debug("DIAGNOSTIC").

is_debug(VarName) ->
    case os:getenv(VarName) of
        false -> false;
        "" -> false;
        _ -> true
    end.

provide_bcrypt_dep() ->
    not is_win32().

provide_bcrypt_release(ReleaseType) ->
    provide_bcrypt_dep() andalso ReleaseType =:= cloud.

erl_opts_i() ->
    [{i, "apps"}] ++
    [{i, Dir}  || Dir <- filelib:wildcard(filename:join(["apps", "*", "include"]))] ++
    [{i, Dir}  || Dir <- filelib:wildcard(filename:join(["lib-ee", "*", "include"]))].

dialyzer(Config) ->
    {dialyzer, OldDialyzerConfig} = lists:keyfind(dialyzer, 1, Config),

    AppsToAnalyse = case os:getenv("DIALYZER_ANALYSE_APP") of
        false ->
            [];
        Value ->
            [ list_to_atom(App) || App <- string:tokens(Value, ",")]
    end,

    AppNames = app_names(),

    KnownApps = [Name ||  Name <- AppsToAnalyse, lists:member(Name, AppNames)],

    AppsToExclude = AppNames -- KnownApps,

    case length(AppsToAnalyse) > 0 of
        true ->
            lists:keystore(dialyzer, 1, Config, {dialyzer, OldDialyzerConfig ++ [{exclude_apps, AppsToExclude}]});
        false ->
            Config
    end.

coveralls() ->
    case {os:getenv("GITHUB_ACTIONS"), os:getenv("GITHUB_TOKEN")} of
      {"true", Token} when is_list(Token) ->
        Cfgs = [{coveralls_repo_token, Token},
                {coveralls_service_job_id, os:getenv("GITHUB_RUN_ID")},
                {coveralls_commit_sha, os:getenv("GITHUB_SHA")},
                {coveralls_coverdata, "_build/test/cover/*.coverdata"},
                {coveralls_service_name, "github"}],
        case os:getenv("GITHUB_EVENT_NAME") =:= "pull_request"
            andalso string:tokens(os:getenv("GITHUB_REF"), "/") of
            [_, "pull", PRNO, _] ->
                [{coveralls_service_pull_request, PRNO} | Cfgs];
            _ ->
                Cfgs
        end;
      _ ->
        []
    end.

app_names() -> list_dir("apps") ++ list_dir("lib-ee").

list_dir(Dir) ->
    case filelib:is_dir(Dir) of
        true ->
            {ok, Names} = file:list_dir(Dir),
            [list_to_atom(Name) || Name <- Names, filelib:is_dir(filename:join([Dir, Name]))];
        false ->
            []
    end.
