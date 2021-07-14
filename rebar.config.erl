-module('rebar.config').

-export([do/2]).

do(Dir, CONFIG) ->
    case iolist_to_binary(Dir) of
        <<".">> ->
            {HasElixir, C1} = deps(CONFIG),
            Config = dialyzer(C1),
            maybe_dump(Config ++ [{overrides, overrides()}] ++ coveralls() ++ config(HasElixir));
        _ ->
            CONFIG
    end.

bcrypt() ->
    {bcrypt, {git, "https://github.com/emqx/erlang-bcrypt.git", {branch, "0.6.0"}}}.

quicer() ->
    %% @todo use tag
    {quicer, {git, "https://github.com/emqx/quic.git", {branch, "main"}}}.

deps(Config) ->
    {deps, OldDeps} = lists:keyfind(deps, 1, Config),
    MoreDeps = [bcrypt() || provide_bcrypt_dep()] ++
        [quicer() || is_quicer_supported()],
    {HasElixir, ExtraDeps} = extra_deps(),
    {HasElixir, lists:keystore(deps, 1, Config, {deps, OldDeps ++ MoreDeps ++ ExtraDeps})}.

extra_deps() ->
    {ok, Proplist} = file:consult("lib-extra/plugins"),
    ErlPlugins0 = proplists:get_value(erlang_plugins, Proplist),
    ExPlugins0 = proplists:get_value(elixir_plugins, Proplist),
    Filter = string:split(os:getenv("EMQX_EXTRA_PLUGINS", ""), ",", all),
    ErlPlugins = filter_extra_deps(ErlPlugins0, Filter),
    ExPlugins = filter_extra_deps(ExPlugins0, Filter),
    {ExPlugins =/= [], ErlPlugins ++ ExPlugins}.

filter_extra_deps(AllPlugins, ["all"]) ->
    AllPlugins;
filter_extra_deps(AllPlugins, Filter) ->
    filter_extra_deps(AllPlugins, Filter, []).
filter_extra_deps([], _, Acc) ->
    lists:reverse(Acc);
filter_extra_deps([{Plugin, _} = P | More], Filter, Acc) ->
    case lists:member(atom_to_list(Plugin), Filter) of
        true ->
            filter_extra_deps(More, Filter, [P | Acc]);
        false ->
            filter_extra_deps(More, Filter, Acc)
    end.

overrides() ->
    [ {add, [ {extra_src_dirs, [{"etc", [{recursive,true}]}]}
            , {erl_opts, [{compile_info, [{emqx_vsn, get_vsn()}]}]}
            ]}
    , {add, snabbkaffe,
       [{erl_opts, common_compile_opts()}]}
    ] ++ community_plugin_overrides().

community_plugin_overrides() ->
    [{add, App, [ {erl_opts, [{i, "include"}]}]} || App <- relx_plugin_apps_extra()].

config(HasElixir) ->
    [ {cover_enabled, is_cover_enabled()}
    , {profiles, profiles()}
    , {project_app_dirs, project_app_dirs()}
    , {plugins, plugins(HasElixir)}
    | [ {provider_hooks, [ {pre,  [{compile, {mix, find_elixir_libs}}]}
                         , {post, [{compile, {mix, consolidate_protocols}}]}
                         ]} || HasElixir ]
    ].

is_cover_enabled() ->
    case os:getenv("ENABLE_COVER_COMPILE") of
        "1"-> true;
        "true" -> true;
        _ -> false
    end.

is_enterprise() ->
    filelib:is_regular("EMQX_ENTERPRISE").

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

project_app_dirs() ->
    ["apps/*"] ++
    case is_enterprise() of
        true -> ["lib-ee/*"];
        false -> []
    end.

plugins(HasElixir) ->
    [ {relup_helper,{git,"https://github.com/emqx/relup_helper", {tag, "2.0.0"}}}
    , {er_coap_client, {git, "https://github.com/emqx/er_coap_client", {tag, "v1.0.2"}}}
      %% emqx main project does not require port-compiler
      %% pin at root level for deterministic
    , {pc, {git, "https://github.com/emqx/port_compiler.git", {tag, "v1.11.1"}}}
    | [ rebar_mix || HasElixir ]
    ]
    %% test plugins are concatenated to default profile plugins
    %% otherwise rebar3 test profile runs are super slow
    ++ test_plugins().

test_plugins() ->
    [ rebar3_proper,
      {coveralls, {git, "https://github.com/emqx/coveralls-erl", {branch, "fix-git-info"}}}
    ].

test_deps() ->
    [ {bbmustache, "1.10.0"}
    %, {emqx_ct_helpers, {git, "https://github.com/emqx/emqx-ct-helpers", {tag, "2.0.0"}}}
    , {emqx_ct_helpers, {git, "https://github.com/emqx/emqx-ct-helpers", {branch, "hocon"}}}
    , meck
    ].

common_compile_opts() ->
    [ debug_info % alwyas include debug_info
    , {compile_info, [{emqx_vsn, get_vsn()}]}
    , {d, snk_kind, msg}
    ] ++
    [{d, 'EMQX_ENTERPRISE'} || is_enterprise()] ++
    [{d, 'EMQX_BENCHMARK'} || os:getenv("EMQX_BENCHMARK") =:= "1" ].

prod_compile_opts() ->
    [ compressed
    , deterministic
    , warnings_as_errors
    | common_compile_opts()
    ].

prod_overrides() ->
    [{add, [ {erl_opts, [deterministic]}]}].

relup_deps(_Profile) ->
    % {post_hooks, [{"(linux|darwin|solaris|freebsd|netbsd|openbsd)", compile, "scripts/inject-deps.escript " ++ atom_to_list(Profile)}]}.
    {post_hooks, []}.

profiles() ->
    Vsn = get_vsn(),
    [ {'emqx',          [ {erl_opts, prod_compile_opts()}
                        , {relx, relx(Vsn, cloud, bin)}
                        , {overrides, prod_overrides()}
                        , relup_deps('emqx')
                        ]}
    , {'emqx-pkg',      [ {erl_opts, prod_compile_opts()}
                        , {relx, relx(Vsn, cloud, pkg)}
                        , {overrides, prod_overrides()}
                        , relup_deps('emqx-pkg')
                        ]}
    , {'emqx-edge',     [ {erl_opts, prod_compile_opts()}
                        , {relx, relx(Vsn, edge, bin)}
                        , {overrides, prod_overrides()}
                        , relup_deps('emqx-edge')
                        ]}
    , {'emqx-edge-pkg', [ {erl_opts, prod_compile_opts()}
                        , {relx, relx(Vsn, edge, pkg)}
                        , {overrides, prod_overrides()}
                        , relup_deps('emqx-edge-pkg')
                        ]}
    , {check,           [ {erl_opts, common_compile_opts()}
                        ]}
    , {test,            [ {deps, test_deps()}
                        , {erl_opts, common_compile_opts() ++ erl_opts_i()}
                        , {extra_src_dirs, [{"test", [{recursive,true}]}]}
                        ]}
    ] ++ ee_profiles(Vsn).

%% RelType: cloud (full size) | edge (slim size)
%% PkgType: bin | pkg
relx(Vsn, RelType, PkgType) ->
    IsEnterprise = is_enterprise(),
    [ {include_src,false}
    , {include_erts, true}
    , {extended_start_script,false}
    , {generate_start_script,false}
    , {sys_config,false}
    , {vm_args,false}
    , {release, {emqx, Vsn}, relx_apps(RelType)}
    , {overlay, relx_overlay(RelType)}
    , {overlay_vars, [ {built_on_arch, rebar_utils:get_arch()}
                     , {emqx_description, emqx_description(RelType, IsEnterprise)}
                     | overlay_vars(RelType, PkgType, IsEnterprise)]}
    ].

emqx_description(cloud, true) -> "EMQ X Enterprise";
emqx_description(cloud, false) -> "EMQ X Broker";
emqx_description(edge, _) -> "EMQ X Edge".

overlay_vars(_RelType, PkgType, true) ->
    ee_overlay_vars(PkgType);
overlay_vars(RelType, PkgType, false) ->
    overlay_vars_rel(RelType) ++ overlay_vars_pkg(PkgType).

%% vars per release type, cloud or edge
overlay_vars_rel(RelType) ->
    VmArgs = case RelType of
                 cloud -> "vm.args";
                 edge -> "vm.args.edge"
             end,

    [ {vm_args_file, VmArgs}
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
    , {runner_data_dir, "$RUNNER_ROOT_DIR/data"}
    , {runner_user, ""}
    ];
overlay_vars_pkg(pkg) ->
    [ {platform_bin_dir, ""}
    , {platform_data_dir, "/var/lib/emqx"}
    , {platform_etc_dir, "/etc/emqx"}
    , {platform_lib_dir, ""}
    , {platform_log_dir, "/var/log/emqx"}
    , {platform_plugins_dir, "/var/lib/enqx/plugins"}
    , {runner_root_dir, "/usr/lib/emqx"}
    , {runner_bin_dir, "/usr/bin"}
    , {runner_etc_dir, "/etc/emqx"}
    , {runner_lib_dir, "$RUNNER_ROOT_DIR/lib"}
    , {runner_log_dir, "/var/log/emqx"}
    , {runner_data_dir, "/var/lib/emqx"}
    , {runner_user, "emqx"}
    ].

relx_apps(ReleaseType) ->
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
    , cuttlefish
    , emqx
    , {mnesia, load}
    , {ekka, load}
    , {emqx_plugin_libs, load}
    , observer_cli
    , emqx_http_lib
    , emqx_resource
    , emqx_connector
    , emqx_authn
    , emqx_authz
    , emqx_gateway
    , emqx_data_bridge
    , emqx_rule_engine
    , emqx_rule_actions
    , emqx_bridge_mqtt
    , emqx_modules
    , emqx_management
    , emqx_retainer
    , emqx_statsd
    ]
    ++ [quicer || is_quicer_supported()]
    ++ [emqx_telemetry || not is_enterprise()]
    ++ [emqx_license || is_enterprise()]
    ++ [bcrypt || provide_bcrypt_release(ReleaseType)]
    ++ relx_apps_per_rel(ReleaseType)
    ++ [{N, load} || N <- relx_plugin_apps(ReleaseType)].

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

relx_plugin_apps(ReleaseType) ->
    []
    ++ relx_plugin_apps_per_rel(ReleaseType)
    ++ relx_plugin_apps_enterprise(is_enterprise())
    ++ relx_plugin_apps_extra().

relx_plugin_apps_per_rel(cloud) ->
    [];
relx_plugin_apps_per_rel(edge) ->
    [].

relx_plugin_apps_enterprise(true) ->
    [list_to_atom(A) || A <- filelib:wildcard("*", "lib-ee"),
                        filelib:is_dir(filename:join(["lib-ee", A]))];
relx_plugin_apps_enterprise(false) -> [].

relx_plugin_apps_extra() ->
    {_HasElixir, ExtraDeps} = extra_deps(),
    [Plugin || {Plugin, _} <- ExtraDeps].

relx_overlay(ReleaseType) ->
    [ {mkdir, "log/"}
    , {mkdir, "data/"}
    , {mkdir, "plugins"}
    , {mkdir, "data/mnesia"}
    , {mkdir, "data/configs"}
    , {mkdir, "data/patches"}
    , {mkdir, "data/scripts"}
    , {template, "data/emqx_vars", "releases/emqx_vars"}
    , {template, "data/BUILT_ON", "releases/{{release_version}}/BUILT_ON"}
    , {copy, "bin/emqx", "bin/emqx"}
    , {copy, "bin/emqx_ctl", "bin/emqx_ctl"}
    , {copy, "bin/node_dump", "bin/node_dump"}
    , {copy, "bin/install_upgrade.escript", "bin/install_upgrade.escript"}
    , {copy, "bin/emqx", "bin/emqx-{{release_version}}"} %% for relup
    , {copy, "bin/emqx_ctl", "bin/emqx_ctl-{{release_version}}"} %% for relup
    , {copy, "bin/install_upgrade.escript", "bin/install_upgrade.escript-{{release_version}}"} %% for relup
    , {template, "bin/emqx.cmd", "bin/emqx.cmd"}
    , {template, "bin/emqx_ctl.cmd", "bin/emqx_ctl.cmd"}
    , {copy, "bin/nodetool", "bin/nodetool"}
    , {copy, "bin/nodetool", "bin/nodetool-{{release_version}}"}
    ] ++ case is_enterprise() of
             true -> ee_etc_overlay(ReleaseType);
             false -> etc_overlay(ReleaseType)
         end.

etc_overlay(ReleaseType) ->
    Templates = emqx_etc_overlay(ReleaseType),
    [ {mkdir, "etc/"}
    , {copy, "{{base_dir}}/lib/emqx/etc/certs","etc/"}
    ] ++
    lists:map(
      fun({From, To}) -> {template, From, To};
         (FromTo)     -> {template, FromTo, FromTo}
      end, Templates)
    ++ extra_overlay(ReleaseType).

extra_overlay(cloud) ->
    [
    ];
extra_overlay(edge) ->
    [].
emqx_etc_overlay(cloud) ->
    emqx_etc_overlay_common() ++
    [ {"{{base_dir}}/lib/emqx/etc/emqx_cloud/vm.args","etc/vm.args"}
    ];
emqx_etc_overlay(edge) ->
    emqx_etc_overlay_common() ++
    [ {"{{base_dir}}/lib/emqx/etc/emqx_edge/vm.args","etc/vm.args"}
    ].

emqx_etc_overlay_common() ->
    [ {"{{base_dir}}/lib/emqx/etc/emqx.conf.all", "etc/emqx.conf"}
    , {"{{base_dir}}/lib/emqx/etc/ssl_dist.conf", "etc/ssl_dist.conf"}
    ].

get_vsn() ->
    PkgVsn = os:cmd("./pkg-vsn.sh"),
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
    case os:type() of
        {win32, _} -> false;
        _ -> true
    end.

provide_bcrypt_release(ReleaseType) ->
    provide_bcrypt_dep() andalso ReleaseType =:= cloud.

erl_opts_i() ->
    [{i, "apps"}] ++
    [{i, Dir}  || Dir <- filelib:wildcard(filename:join(["apps", "*", "include"]))] ++
    case is_enterprise() of
        true ->
            [{i, Dir}  || Dir <- filelib:wildcard(filename:join(["lib-ee", "*", "include"]))];
        false -> []
    end.

dialyzer(Config) ->
    {dialyzer, OldDialyzerConfig} = lists:keyfind(dialyzer, 1, Config),

    AppsToAnalyse = case os:getenv("DIALYZER_ANALYSE_APP") of
        false ->
            [];
        Value ->
            [ list_to_atom(App) || App <- string:tokens(Value, ",")]
    end,

    AppNames = [list_dir("apps")] ++ 
               case is_enterprise() of
                    true -> [list_dir("lib-ee")];
                    false -> []
               end,

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

list_dir(Dir) ->
    {ok, Names} = file:list_dir(Dir),
    [list_to_atom(Name) || Name <- Names, filelib:is_dir(filename:join([Dir, Name]))].

%% ==== Enterprise supports below ==================================================================

ee_profiles(_Vsn) -> [].
ee_etc_overlay(_) -> [].
ee_overlay_vars(_PkgType) -> [].
