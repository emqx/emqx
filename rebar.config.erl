-module('rebar.config').

-export([do/2]).

do(_Dir, CONFIG) ->
    C1 = deps(CONFIG),
    Config = dialyzer(C1),
    maybe_dump(Config ++ [{overrides, overrides()}] ++ coveralls() ++ config()).

bcrypt() ->
    {bcrypt, {git, "https://github.com/emqx/erlang-bcrypt.git", {branch, "0.6.0"}}}.

deps(Config) ->
    {deps, OldDpes} = lists:keyfind(deps, 1, Config),
    MoreDeps = case provide_bcrypt_dep() of
        true -> [bcrypt()];
        false -> []
    end,
    lists:keystore(deps, 1, Config, {deps, OldDpes ++ MoreDeps}).

overrides() ->
    [ {add, [ {extra_src_dirs, [{"etc", [{recursive,true}]}]}
            , {erl_opts, [ deterministic
                         , {compile_info, [{emqx_vsn, get_vsn()}]}
                         ]}
            ]}
    ].

config() ->
    [ {plugins, plugins()}
    , {profiles, profiles()}
    , {project_app_dirs, project_app_dirs()}
    ].

is_enterprise() ->
    filelib:is_regular("EMQX_ENTERPRISE").

extra_lib_dir() ->
    case is_enterprise() of
        true -> "lib-ee";
        false -> "lib-ce"
    end.

project_app_dirs() ->
    ["apps/*", extra_lib_dir() ++ "/*", "."].

plugins() ->
    [ {relup_helper,{git,"https://github.com/emqx/relup_helper", {branch,"master"}}},
      {er_coap_client, {git, "https://github.com/emqx/er_coap_client", {tag, "v1.0"}}}
    ].

test_plugins() ->
    [ rebar3_proper,
      {coveralls, {git, "https://github.com/emqx/coveralls-erl", {branch, "github"}}}
    ].

test_deps() ->
    [ {bbmustache, "1.10.0"}
    , {emqx_ct_helpers, {git, "https://github.com/emqx/emqx-ct-helpers", {tag, "1.3.6"}}}
    , meck
    ].

common_compile_opts() ->
    [ deterministic
    , {compile_info, [{emqx_vsn, get_vsn()}]}
    | [{d, 'EMQX_ENTERPRISE'} || is_enterprise()]
    ].

prod_compile_opts() ->
    [ compressed
    , no_debug_info
    , warnings_as_errors
    | common_compile_opts()
    ].

test_compile_opts() ->
    [ debug_info
    | common_compile_opts()
    ].

profiles() ->
    [ {'emqx',          [ {erl_opts, prod_compile_opts()}
                        , {relx, relx('emqx')}
                        ]}
    , {'emqx-pkg',      [ {erl_opts, prod_compile_opts()}
                        , {relx, relx('emqx-pkg')}
                        ]}
    , {'emqx-edge',     [ {erl_opts, prod_compile_opts()}
                        , {relx, relx('emqx-edge')}
                        ]}
    , {'emqx-edge-pkg', [ {erl_opts, prod_compile_opts()}
                        , {relx, relx('emqx-edge-pkg')}
                        ]}
    , {check,           [ {erl_opts, test_compile_opts()}
                        ]}
    , {test,            [ {deps, test_deps()}
                        , {plugins, test_plugins()}
                        , {erl_opts, test_compile_opts() ++ erl_opts_i()}
                        , {extra_src_dirs, [{"test", [{recursive,true}]}]}
                        ]}
    ].

relx(Profile) ->
    Vsn = get_vsn(),
    [ {include_src,false}
    , {include_erts, true}
    , {extended_start_script,false}
    , {generate_start_script,false}
    , {sys_config,false}
    , {vm_args,false}
    ] ++ do_relx(Profile, Vsn).

do_relx('emqx', Vsn) ->
    [ {release, {emqx, Vsn}, relx_apps(cloud)}
    , {overlay, relx_overlay(cloud)}
    , {overlay_vars, overlay_vars(["vars/vars-cloud.config","vars/vars-bin.config"])}
    ];
do_relx('emqx-pkg', Vsn) ->
    [ {release, {emqx, Vsn}, relx_apps(cloud)}
    , {overlay, relx_overlay(cloud)}
    , {overlay_vars, overlay_vars(["vars/vars-cloud.config","vars/vars-pkg.config"])}
    ];
do_relx('emqx-edge', Vsn) ->
    [ {release, {emqx, Vsn}, relx_apps(edge)}
    , {overlay, relx_overlay(edge)}
    , {overlay_vars, overlay_vars(["vars/vars-edge.config","vars/vars-bin.config"])}
    ];
do_relx('emqx-edge-pkg', Vsn) ->
    [ {release, {emqx, Vsn}, relx_apps(edge)}
    , {overlay, relx_overlay(edge)}
    , {overlay_vars, overlay_vars(["vars/vars-edge.config","vars/vars-pkg.config"])}
    ].

overlay_vars(Files) ->
    [{built_on_arch, rebar_utils:get_arch()} | Files].

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
    , emqx_modules
    ]
    ++ [bcrypt || provide_bcrypt_release(ReleaseType)]
    ++ relx_apps_per_rel(ReleaseType)
    ++ [{N, load} || N <- relx_plugin_apps(ReleaseType)].

relx_apps_per_rel(cloud) ->
    [ {observer, load}
    , luerl
    , xmerl
    ];
relx_apps_per_rel(edge) ->
    [].

relx_plugin_apps(ReleaseType) ->
    [ emqx_retainer
    , emqx_management
    , emqx_dashboard
    , emqx_bridge_mqtt
    , emqx_sn
    , emqx_coap
    , emqx_stomp
    , emqx_auth_http
    , emqx_auth_mysql
    , emqx_auth_jwt
    , emqx_auth_mnesia
    , emqx_web_hook
    , emqx_recon
    , emqx_rule_engine
    , emqx_sasl
    , emqx_telemetry
    , emqx_modules
    ]
    ++ relx_plugin_apps_per_rel(ReleaseType)
    ++ relx_plugin_apps_enterprise(is_enterprise()).

relx_plugin_apps_per_rel(cloud) ->
    [ emqx_lwm2m
    , emqx_auth_ldap
    , emqx_auth_pgsql
    , emqx_auth_redis
    , emqx_auth_mongo
    , emqx_lua_hook
    , emqx_exhook
    , emqx_exproto
    , emqx_prometheus
    , emqx_psk_file
    ];
relx_plugin_apps_per_rel(edge) ->
    [].

relx_plugin_apps_enterprise(true) ->
    [list_to_atom(A) || A <- filelib:wildcard("*", "lib-ee"),
                        filelib:is_dir(filename:join(["lib-ee", A]))];
relx_plugin_apps_enterprise(false) -> [].

relx_overlay(ReleaseType) ->
    [ {mkdir,"log/"}
    , {mkdir,"data/"}
    , {mkdir,"data/mnesia"}
    , {mkdir,"data/configs"}
    , {mkdir,"data/scripts"}
    , {template, "data/loaded_plugins.tmpl", "data/loaded_plugins"}
    , {template, "data/loaded_modules.tmpl", "data/loaded_modules"}
    , {template,"data/emqx_vars","releases/emqx_vars"}
    , {copy,"bin/emqx","bin/emqx"}
    , {copy,"bin/emqx_ctl","bin/emqx_ctl"}
    , {copy,"bin/install_upgrade.escript", "bin/install_upgrade.escript"}
    , {copy,"bin/emqx","bin/emqx-{{release_version}}"} %% for relup
    , {copy,"bin/emqx_ctl","bin/emqx_ctl-{{release_version}}"} %% for relup
    , {copy,"bin/install_upgrade.escript", "bin/install_upgrade.escript-{{release_version}}"} %% for relup
    , {template,"bin/emqx.cmd","bin/emqx.cmd"}
    , {template,"bin/emqx_ctl.cmd","bin/emqx_ctl.cmd"}
    , {copy,"bin/nodetool","bin/nodetool"}
    , {copy,"bin/nodetool","bin/nodetool-{{release_version}}"}
    , {copy,"_build/default/lib/cuttlefish/cuttlefish","bin/cuttlefish"}
    , {copy,"_build/default/lib/cuttlefish/cuttlefish","bin/cuttlefish-{{release_version}}"}
    , {copy,"priv/emqx.schema","releases/{{release_version}}/"}
    ] ++ etc_overlay(ReleaseType).

etc_overlay(ReleaseType) ->
    PluginApps = relx_plugin_apps(ReleaseType),
    Templates = emqx_etc_overlay(ReleaseType) ++
                lists:append([plugin_etc_overlays(App) || App <- PluginApps]),
    [ {mkdir, "etc/"}
    , {mkdir, "etc/plugins"}
    , {template, "etc/BUILT_ON", "releases/{{release_version}}/BUILT_ON"}
    , {copy, "{{base_dir}}/lib/emqx/etc/certs","etc/"}
    ] ++
    lists:map(
      fun({From, To}) -> {template, From, To};
         (FromTo)     -> {template, FromTo, FromTo}
      end, Templates)
    ++ extra_overlay(ReleaseType).

extra_overlay(cloud) ->
    [ {copy,"{{base_dir}}/lib/emqx_lwm2m/lwm2m_xml","etc/"}
    , {copy, "{{base_dir}}/lib/emqx_psk_file/etc/psk.txt", "etc/psk.txt"}
    ];
extra_overlay(edge) ->
    [].
emqx_etc_overlay(cloud) ->
    emqx_etc_overlay_common() ++
    [ {"etc/emqx_cloud/vm.args","etc/vm.args"}
    ];
emqx_etc_overlay(edge) ->
    emqx_etc_overlay_common() ++
    [ {"etc/emqx_edge/vm.args","etc/vm.args"}
    ].

emqx_etc_overlay_common() ->
    ["etc/acl.conf", "etc/emqx.conf", "etc/ssl_dist.conf",
     %% TODO: check why it has to end with .paho
     %% and why it is put to etc/plugins dir
     {"etc/acl.conf.paho", "etc/plugins/acl.conf.paho"}].

plugin_etc_overlays(App0) ->
    App = atom_to_list(App0),
    ConfFiles = find_conf_files(App),
    %% NOTE: not filename:join here since relx translates it for windows
    [{"{{base_dir}}/lib/"++ App ++"/etc/" ++ F, "etc/plugins/" ++ F}
     || F <- ConfFiles].

%% NOTE: for apps fetched as rebar dependency (there is so far no such an app)
%% the overlay should be hand-coded but not to rely on build-time wildcards.
find_conf_files(App) ->
    Dir1 = filename:join(["apps", App, "etc"]),
    Dir2 = filename:join([extra_lib_dir(), App, "etc"]),
    filelib:wildcard("*.conf", Dir1) ++ filelib:wildcard("*.conf", Dir2).

env(Name, Default) ->
    case os:getenv(Name) of
        "" -> Default;
        false -> Default;
        Value -> Value
    end.

get_vsn() ->
    PkgVsn = case env("PKG_VSN", false) of
                 false -> os:cmd("./pkg-vsn.sh");
                 Vsn -> Vsn
             end,
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
    [{i, Dir}  || Dir <- filelib:wildcard(filename:join([extra_lib_dir(), "*", "include"]))].

dialyzer(Config) ->
    {dialyzer, OldDialyzerConfig} = lists:keyfind(dialyzer, 1, Config),

    AppsToAnalyse = case os:getenv("DIALYZER_ANALYSE_APP") of
        false ->
            [];
        Value ->
            [ list_to_atom(App) || App <- string:tokens(Value, ",")]
    end,

    AppNames = [emqx | list_dir("apps")] ++ list_dir(extra_lib_dir()),

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
                {coveralls_service_number, os:getenv("GITHUB_RUN_NUMBER")},
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
