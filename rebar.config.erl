-module('rebar.config').

-export([do/2]).

do(Dir, CONFIG) ->
    ok = assert_otp(),
    ok = warn_profile_env(),
    case iolist_to_binary(Dir) of
        <<".">> ->
            C1 = deps(CONFIG),
            Config = dialyzer(C1),
            maybe_dump(Config ++ [{overrides, overrides()}] ++ config());
        _ ->
            CONFIG
    end.

assert_otp() ->
    Oldest = 26,
    Latest = 27,
    OtpRelease = list_to_integer(erlang:system_info(otp_release)),
    case OtpRelease < Oldest orelse OtpRelease > Latest of
        true ->
            io:format(
                standard_error,
                "ERROR: Erlang/OTP version ~p found. min=~p, recommended=~p~n",
                [OtpRelease, Oldest, Latest]
            ),
            halt(1);
        false when OtpRelease =/= Latest ->
            io:format(
                "WARNING: Erlang/OTP version ~p found, recommended==~p~n",
                [OtpRelease, Latest]
            );
        false ->
            ok
    end.

quicer() ->
    {quicer, {git, "https://github.com/emqx/quic.git", {tag, "0.2.4"}}}.

jq() ->
    {jq, {git, "https://github.com/emqx/jq", {tag, "v0.3.12"}}}.

deps(Config) ->
    {deps, OldDeps} = lists:keyfind(deps, 1, Config),
    MoreDeps =
        [jq() || is_jq_supported()] ++
            [quicer() || is_quicer_supported()],
    lists:keystore(deps, 1, Config, {deps, OldDeps ++ MoreDeps}).

overrides() ->
    [
        {add, [{extra_src_dirs, [{"etc", [{recursive, true}]}]}]}
    ] ++ snabbkaffe_overrides().

%% Temporary workaround for a rebar3 erl_opts duplication
%% bug. Ideally, we want to set this define globally
snabbkaffe_overrides() ->
    Apps = [snabbkaffe, ekka, mria, gen_rpc],
    [{add, App, [{erl_opts, [{d, snk_kind, msg}]}]} || App <- Apps].

config() ->
    [
        {cover_enabled, is_cover_enabled()},
        {profiles, profiles()},
        {plugins, plugins()}
    ].

is_cover_enabled() ->
    case os:getenv("ENABLE_COVER_COMPILE") of
        "1" -> true;
        "true" -> true;
        _ -> false
    end.

%% BUILD_WITHOUT_JQ
%% BUILD_WITHOUT_QUIC
%% BUILD_WITHOUT_ROCKSDB
is_build_without(Name) ->
    "1" =:= os:getenv("BUILD_WITHOUT_" ++ Name).

is_jq_supported() ->
    not is_build_without("JQ").

is_quicer_supported() ->
    not is_build_without("QUIC").

is_rocksdb_supported() ->
    %% there is no way one can build rocksdb on raspbian
    %% so no need to check is_build_with
    Distro = os_cmd("./scripts/get-distro.sh"),
    is_rocksdb_supported(Distro).

is_rocksdb_supported("respbian" ++ _) ->
    false;
is_rocksdb_supported(_) ->
    not is_build_without("ROCKSDB").

get_emqx_flavor() ->
    case os:getenv("EMQX_FLAVOR") of
        X when X =:= false; X =:= "" -> official;
        Flavor -> list_to_atom(Flavor)
    end.

project_app_dirs() ->
    #{reltype := RelType} = get_edition_from_profile_env(),
    project_app_dirs(RelType).

project_app_dirs(RelType) ->
    ExcludedApps = unavailable_apps(RelType),
    UmbrellaApps = [
        Path
     || Path <- filelib:wildcard("apps/*"),
        not project_app_excluded(Path, ExcludedApps)
    ],
    UmbrellaApps.

project_app_excluded("apps/" ++ AppStr, ExcludedApps) ->
    App = list_to_atom(AppStr),
    lists:member(App, ExcludedApps).

plugins() ->
    [
        {emqx_relup, {git, "https://github.com/emqx/emqx-relup.git", {tag, "0.2.2"}}},
        %% emqx main project does not require port-compiler
        %% pin at root level for deterministic
        {pc, "1.15.0"}
    ] ++
        %% test plugins are concatenated to default profile plugins
        %% otherwise rebar3 test profile runs are super slow
        test_plugins().

test_plugins() ->
    [
        {rebar3_proper, "0.12.1"}
    ].

test_deps() ->
    [
        {bbmustache, "1.10.0"},
        {meck, "0.9.2"},
        %% TODO: {proper, "1.5.0"} when it's published to hex.pm
        {proper, {git, "https://github.com/proper-testing/proper", {tag, "v1.5.0"}}},
        {er_coap_client, {git, "https://github.com/emqx/er_coap_client", {tag, "v1.0.5"}}},
        {erl_csv, "0.2.0"},
        {eministat, "0.10.1"}
    ].

common_compile_opts() ->
    common_compile_opts(undefined).

common_compile_opts(Vsn) ->
    % always include debug_info
    [
        debug_info,
        {compile_info, [{emqx_vsn, Vsn} || Vsn /= undefined]},
        %% TODO: remove
        {d, 'EMQX_RELEASE_EDITION', ee}
    ] ++
        [{d, 'EMQX_BENCHMARK'} || os:getenv("EMQX_BENCHMARK") =:= "1"] ++
        [{d, 'STORE_STATE_IN_DS'} || os:getenv("STORE_STATE_IN_DS") =:= "1"] ++
        [{d, 'EMQX_FLAVOR', get_emqx_flavor()}] ++
        [{d, 'BUILD_WITHOUT_QUIC'} || not is_quicer_supported()].

warn_profile_env() ->
    case os:getenv("PROFILE") of
        false ->
            io:format(
                standard_error,
                "WARN: environment variable PROFILE is not set, using 'emqx-enterprise'~n",
                []
            );
        _ ->
            ok
    end.

%% this function is only used for test/check profiles
get_edition_from_profile_env() ->
    case os:getenv("PROFILE") of
        "emqx-enterprise" ++ _ ->
            #{reltype => standard};
        false ->
            #{reltype => standard};
        V ->
            io:format(standard_error, "ERROR: bad_PROFILE ~p~n", [V]),
            exit(bad_PROFILE)
    end.

prod_compile_opts(Vsn) ->
    [
        compressed,
        deterministic,
        warnings_as_errors
        | common_compile_opts(Vsn)
    ].

prod_overrides() ->
    [{add, [{erl_opts, [deterministic]}]}].

profiles() ->
    #{reltype := RelType} = get_edition_from_profile_env(),
    profiles_ee(RelType) ++ profiles_dev(RelType).

profiles_ee(RelType) ->
    Vsn = get_vsn('emqx-enterprise'),
    [
        {'emqx-enterprise', [
            {erl_opts, prod_compile_opts(Vsn)},
            {relx, relx(Vsn, RelType, bin)},
            {overrides, prod_overrides()},
            {project_app_dirs, project_app_dirs(RelType)}
        ]},
        {'emqx-enterprise-pkg', [
            {erl_opts, prod_compile_opts(Vsn)},
            {relx, relx(Vsn, RelType, pkg)},
            {overrides, prod_overrides()},
            {project_app_dirs, project_app_dirs(RelType)}
        ]}
    ].

profiles_dev(_RelType) ->
    [
        {check, [
            {erl_opts, common_compile_opts()},
            {project_app_dirs, project_app_dirs()}
        ]},
        {test, [
            {deps, test_deps()},
            {erl_opts, common_compile_opts() ++ erl_opts_i()},
            {extra_src_dirs, [{"test", [{recursive, true}]}]},
            {project_app_dirs, project_app_dirs()}
        ]}
    ].

%% RelType: standard
%% PkgType: bin | pkg
relx(Vsn, RelType, PkgType) ->
    [
        {include_src, false},
        {include_erts, true},
        {extended_start_script, false},
        {generate_start_script, false},
        {sys_config, false},
        {vm_args, false},
        {release, {emqx, Vsn}, relx_apps(RelType)},
        {exclude_apps, excluded_apps(RelType)},
        {tar_hooks, [
            "scripts/rel/cleanup-release-package.sh",
            "scripts/rel/macos-sign-binaries.sh",
            "scripts/rel/macos-notarize-package.sh"
        ]},
        {overlay, relx_overlay(RelType)},
        {overlay_vars_values,
            build_info() ++
                [
                    {emqx_description, emqx_description()}
                    | overlay_vars(RelType, PkgType)
                ]}
    ].

%% Make a HOCON compatible format
build_info() ->
    Os = os_cmd("./scripts/get-distro.sh"),
    [
        {build_info_arch, erlang:system_info(system_architecture)},
        {build_info_wordsize, rebar_utils:wordsize()},
        {build_info_os, Os},
        {build_info_erlang, rebar_utils:otp_release()},
        {build_info_elixir, none},
        {build_info_relform, relform()}
    ].

relform() ->
    case os:getenv("EMQX_REL_FORM") of
        false -> "tgz";
        Other -> Other
    end.

emqx_description() ->
    case get_emqx_flavor() of
        official -> "EMQX Enterprise";
        Flavor -> io_lib:format("EMQX Enterprise(~s)", [Flavor])
    end.

overlay_vars(_RelType, PkgType) ->
    [
        {emqx_default_erlang_cookie, "emqxsecretcookie"}
    ] ++
        overlay_vars_pkg(PkgType) ++
        overlay_vars_edition().

overlay_vars_edition() ->
    [
        {emqx_schema_mod, emqx_enterprise_schema},
        {is_enterprise, "yes"},
        {emqx_configuration_doc,
            "https://docs.emqx.com/en/enterprise/latest/configuration/configuration.html"},
        {emqx_configuration_doc_log,
            "https://docs.emqx.com/en/enterprise/latest/configuration/logs.html"}
    ].

%% vars per packaging type, bin(zip/tar.gz/docker) or pkg(rpm/deb)
overlay_vars_pkg(bin) ->
    [
        {platform_data_dir, "data"},
        {platform_etc_dir, "etc"},
        {platform_plugins_dir, "plugins"},
        {runner_bin_dir, "$RUNNER_ROOT_DIR/bin"},
        {emqx_etc_dir, "$BASE_RUNNER_ROOT_DIR/etc"},
        {runner_lib_dir, "$RUNNER_ROOT_DIR/lib"},
        {runner_log_dir, "$BASE_RUNNER_ROOT_DIR/log"},
        {runner_user, ""},
        {is_elixir, "no"}
    ];
overlay_vars_pkg(pkg) ->
    [
        {platform_data_dir, "/var/lib/emqx"},
        {platform_etc_dir, "/etc/emqx"},
        {platform_plugins_dir, "/var/lib/emqx/plugins"},
        {runner_bin_dir, "/usr/bin"},
        {emqx_etc_dir, "/etc/emqx"},
        {runner_lib_dir, "$RUNNER_ROOT_DIR/lib"},
        {runner_log_dir, "/var/log/emqx"},
        {runner_user, "emqx"},
        {is_elixir, "no"}
    ].

relx_apps(ReleaseType) ->
    {ok, [
        #{
            db_apps := DBApps,
            system_apps := SystemApps,
            common_business_apps := CommonBusinessApps,
            ee_business_apps := EEBusinessApps
        }
    ]} = file:consult("apps/emqx_machine/priv/reboot_lists.eterm"),
    BusinessApps = CommonBusinessApps ++ EEBusinessApps,
    UnavailableApps = unavailable_apps(ReleaseType),
    Apps =
        [App || App <- SystemApps, not lists:member(App, UnavailableApps)] ++
            %% EMQX starts the DB and the business applications:
            [{App, load} || App <- DBApps, not lists:member(App, UnavailableApps)] ++
            [emqx_machine] ++
            [{App, load} || App <- BusinessApps, not lists:member(App, UnavailableApps)],
    Apps.

unavailable_apps(standard) ->
    AppAvailability = [
        {quicer, is_quicer_supported()},
        {jq, is_jq_supported()},
        {observer, is_app(observer)},
        {mnesia_rocksdb, is_rocksdb_supported()}
    ],
    [App || {App, false} <- AppAvailability];
unavailable_apps(platform) ->
    AppAvailability = [
        {quicer, is_quicer_supported()},
        {jq, is_jq_supported()},
        {observer, is_app(observer)},
        {mnesia_rocksdb, is_rocksdb_supported()}
    ],
    [App || {App, false} <- AppAvailability].

excluded_apps(_) ->
    [
        %% Pulled in as an _optional application_ for `observer` (as of OTP-27.2)
        %% Exclude as it needs a bunch of extra libraries installed on the host system.
        wx
    ].

is_app(Name) ->
    case application:load(Name) of
        ok -> true;
        {error, {already_loaded, _}} -> true;
        _ -> false
    end.

relx_overlay(ReleaseType) ->
    [
        {mkdir, "log/"},
        {mkdir, "data/"},
        {mkdir, "plugins"},
        {mkdir, "data/mnesia"},
        {mkdir, "data/configs"},
        {mkdir, "data/patches"},
        {mkdir, "data/scripts"},
        {template, "rel/emqx_vars", "releases/emqx_vars"},
        {template, "rel/BUILD_INFO", "releases/{{release_version}}/BUILD_INFO"},
        {copy, "bin/emqx", "bin/emqx"},
        {copy, "bin/emqx_ctl", "bin/emqx_ctl"},
        {copy, "bin/emqx_cluster_rescue", "bin/emqx_cluster_rescue"},
        {copy, "bin/emqx_fw", "bin/emqx_fw"},
        {copy, "bin/node_dump", "bin/node_dump"},
        {copy, "bin/install_upgrade.escript", "bin/install_upgrade.escript"},
        {copy, "bin/emqx", "bin/emqx-{{release_version}}"},
        {copy, "bin/emqx_ctl", "bin/emqx_ctl-{{release_version}}"},
        {copy, "bin/install_upgrade.escript", "bin/install_upgrade.escript-{{release_version}}"},
        {copy, "apps/emqx_gateway_lwm2m/lwm2m_xml", "etc/lwm2m_xml"},
        {copy, "apps/emqx_auth/etc/acl.conf", "etc/acl.conf"},
        {copy, "apps/emqx_auth/etc/auth-built-in-db-bootstrap.csv",
            "etc/auth-built-in-db-bootstrap.csv"},
        {template, "bin/emqx.cmd", "bin/emqx.cmd"},
        {template, "bin/emqx_ctl.cmd", "bin/emqx_ctl.cmd"},
        {copy, "bin/nodetool", "bin/nodetool"},
        {copy, "bin/nodetool", "bin/nodetool-{{release_version}}"}
    ] ++ etc_overlay(ReleaseType).

etc_overlay(ReleaseType) ->
    Templates = emqx_etc_overlay(ReleaseType),
    [
        {mkdir, "etc/"},
        {copy, "{{base_dir}}/lib/emqx/etc/certs", "etc/"},
        {copy, "rel/config/examples", "etc/"}
    ] ++
        lists:map(
            fun
                ({From, To}) -> {template, From, To};
                (FromTo) -> {template, FromTo, FromTo}
            end,
            Templates
        ).

emqx_etc_overlay(ReleaseType) ->
    emqx_etc_overlay_per_rel(ReleaseType) ++
        emqx_etc_overlay().

emqx_etc_overlay_per_rel(_RelType) ->
    [{"{{base_dir}}/lib/emqx/etc/vm.args.cloud", "etc/vm.args"}].

emqx_etc_overlay() ->
    [
        {"{{base_dir}}/lib/emqx/etc/ssl_dist.conf", "etc/ssl_dist.conf"},
        {"{{base_dir}}/lib/emqx_conf/etc/emqx.conf.all", "etc/emqx.conf"},
        {"{{base_dir}}/lib/emqx_conf/etc/base.hocon", "etc/base.hocon"}
    ].

get_vsn(Profile) ->
    case os:getenv("PKG_VSN") of
        false ->
            os_cmd("pkg-vsn.sh " ++ atom_to_list(Profile));
        Vsn ->
            Vsn
    end.

%% to make it compatible to Linux and Windows,
%% we must use bash to execute the bash file
%% because "./" will not be recognized as an internal or external command
os_cmd(Cmd) ->
    Output = os:cmd("bash " ++ Cmd),
    re:replace(Output, "\n", "", [{return, list}]).

maybe_dump(Config) ->
    is_debug() andalso
        file:write_file("rebar.config.rendered", [io_lib:format("~p.\n", [I]) || I <- Config]),
    Config.

is_debug() -> is_debug("DEBUG") orelse is_debug("DIAGNOSTIC").

is_debug(VarName) ->
    case os:getenv(VarName) of
        false -> false;
        "" -> false;
        _ -> true
    end.

erl_opts_i() ->
    [{i, "apps"}] ++
        [{i, Dir} || Dir <- filelib:wildcard(filename:join(["apps", "*", "include"]))].

dialyzer(Config) ->
    {dialyzer, OldDialyzerConfig} = lists:keyfind(dialyzer, 1, Config),

    AppsToAnalyse =
        case os:getenv("DIALYZER_ANALYSE_APP") of
            false ->
                [];
            Value ->
                [list_to_atom(App) || App <- string:tokens(Value, ",")]
        end,

    AppNames = app_names(),
    KnownApps = [Name || Name <- AppsToAnalyse, lists:member(Name, AppNames)],
    UnavailableApps = unavailable_apps(standard),
    ExcludedApps = excluded_apps(standard),
    AppsToExclude = UnavailableApps ++ ExcludedApps ++ (AppNames -- KnownApps),

    Extra =
        [system_monitor, tools] ++
            [jq || is_jq_supported()] ++
            [quicer || is_quicer_supported()],
    NewDialyzerConfig =
        OldDialyzerConfig ++
            [{exclude_apps, AppsToExclude} || length(AppsToAnalyse) > 0] ++
            [{plt_extra_apps, Extra} || length(Extra) > 0],
    lists:keystore(
        dialyzer,
        1,
        Config,
        {dialyzer, NewDialyzerConfig}
    ).

app_names() -> list_dir("apps").

list_dir(Dir) ->
    case filelib:is_dir(Dir) of
        true ->
            {ok, Names} = file:list_dir(Dir),
            [list_to_atom(Name) || Name <- Names, filelib:is_dir(filename:join([Dir, Name]))];
        false ->
            []
    end.
