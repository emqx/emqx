-module('rebar.config').

-export([do/2]).

do(Dir, CONFIG) ->
    ok = compile_and_load_pase_transforms(Dir),
    dump(deps(CONFIG) ++ dialyzer(CONFIG) ++ config()).

bcrypt() ->
    {bcrypt, {git, "https://github.com/emqx/erlang-bcrypt.git", {branch, "0.6.0"}}}.

deps(Config) ->
    {deps, OldDpes} = lists:keyfind(deps, 1, Config),
    MoreDeps = case provide_bcrypt_dep() of
        true -> [bcrypt()];
        false -> []
    end,
    lists:keystore(deps, 1, Config, {deps, OldDpes ++ MoreDeps}).

config() ->
    [ {plugins, plugins()}
    , {profiles, profiles()}
    ].

plugins() ->
    [ {relup_helper,{git,"https://github.com/emqx/relup_helper", {branch,"master"}}},
      {er_coap_client, {git, "https://github.com/emqx/er_coap_client", {tag, "v1.0"}}}
    ].

test_deps() ->
    [ {bbmustache, "1.10.0"}
    , {emqx_ct_helpers, {git, "https://github.com/emqx/emqx-ct-helpers", {tag, "1.3.4"}}}
    , meck
    ].

profiles() ->
    [ {'emqx',          [ {erl_opts, [no_debug_info, warnings_as_errors, {parse_transform, mod_vsn}]}
                        , {relx, relx('emqx')}
                        ]}
    , {'emqx-pkg',      [ {erl_opts, [no_debug_info, warnings_as_errors, {parse_transform, mod_vsn}]}
                        , {relx, relx('emqx-pkg')}
                        ]}
    , {'emqx-edge',     [ {erl_opts, [no_debug_info, warnings_as_errors, {parse_transform, mod_vsn}]}
                        , {relx, relx('emqx-edge')}
                        ]}
    , {'emqx-edge-pkg', [ {erl_opts, [no_debug_info, warnings_as_errors, {parse_transform, mod_vsn}]}
                        , {relx, relx('emqx-edge-pkg')}
                        ]}
    , {check,           [ {erl_opts, [debug_info, warnings_as_errors, {parse_transform, mod_vsn}]}
                        ]}
    , {test,            [ {deps, test_deps()}
                        , {erl_opts, [debug_info, {parse_transform, mod_vsn}] ++ erl_opts_i()}
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
    ] ++ relx_plugin_apps_per_rel(ReleaseType).

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
    , emqx_plugin_template
    ];
relx_plugin_apps_per_rel(edge) ->
    [].

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
    [ {"etc/emqx_cloud.d/vm.args","etc/vm.args"}
    ];
emqx_etc_overlay(edge) ->
    emqx_etc_overlay_common() ++
    [ {"etc/emqx_edge.d/vm.args","etc/vm.args"}
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
    Dir = filename:join(["apps", App, "etc"]),
    filelib:wildcard("*.conf", Dir) ++ filelib:wildcard("*.config", Dir).

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
    Vsn2 = re:replace(PkgVsn, "v", "", [{return ,list}]),
    re:replace(Vsn2, "\n", "", [{return ,list}]).

dump(Config) ->
    file:write_file("rebar.config.rendered", [io_lib:format("~p.\n", [I]) || I <- Config]),
    Config.

provide_bcrypt_dep() ->
    case os:type() of
        {win32, _} -> false;
        _ -> true
    end.

provide_bcrypt_release(ReleaseType) ->
    provide_bcrypt_dep() andalso ReleaseType =:= cloud.

%% this is a silly but working patch.
%% rebar3 does not handle umberella project's cross-app parse_transform well
compile_and_load_pase_transforms(Dir) ->
    PtFiles =
        [ "apps/emqx_rule_engine/src/emqx_rule_actions_trans.erl"
        ],
    CompileOpts = [verbose,report_errors,report_warnings,return_errors,debug_info],
    lists:foreach(fun(PtFile) -> {ok, _Mod} = compile:file(path(Dir, PtFile), CompileOpts) end, PtFiles).

path(Dir, Path) -> str(filename:join([Dir, Path])).

str(L) when is_list(L) -> L;
str(B) when is_binary(B) -> unicode:characters_to_list(B, utf8).

erl_opts_i() ->
    [{i, "apps"}] ++ [{i, Dir}  || Dir <- filelib:wildcard(filename:join(["apps", "**", "include"]))].

dialyzer(Config) ->
    {dialyzer, OldDialyzerConfig} = lists:keyfind(dialyzer, 1, Config),

    AppsToAnalyse = case os:getenv("DIALYZER_ANALYSE_APP") of
        false ->
            [];
        Value ->
            [ list_to_atom(App) || App <- string:tokens(Value, ",")]
    end,

    AppsDir = "apps",
    AppNames = [emqx | list_dir(AppsDir)],

    KnownApps = [Name ||  Name <- AppsToAnalyse, lists:member(Name, AppNames)],
    UnknownApps = AppsToAnalyse -- KnownApps,
    io:format("Unknown Apps ~p ~n", [UnknownApps]),

    AppsToExclude = AppNames -- KnownApps,

    case length(AppsToAnalyse) > 0 of
        true ->
            lists:keystore(dialyzer, 1, Config, {dialyzer, OldDialyzerConfig ++ [{exclude_apps, AppsToExclude}]});
        false ->
            Config
    end.

list_dir(Dir) ->
    {ok, Names} = file:list_dir(Dir),
    [list_to_atom(Name) || Name <- Names, filelib:is_dir(filename:join([Dir, Name]))].
