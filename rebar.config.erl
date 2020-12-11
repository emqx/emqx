-module('rebar.config').

-export([do/2]).

do(Dir, CONFIG) ->
    ok = compile_and_load_pase_transforms(Dir),
    dump(deps(CONFIG) ++ config()).

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
    [ {relup_helper,{git,"https://github.com/emqx/relup_helper", {branch,"master"}}}
    ].

test_deps() ->
    [ {bbmustache, "1.10.0"}
    , {emqx_ct_helpers, {git, "https://github.com/emqx/emqx-ct-helpers", {tag, "1.3.0"}}}
    , meck
    ].

profiles() ->
    [ {'emqx',          [ {relx, relx('emqx')}
                        , {erl_opts, [no_debug_info]}
                        ]}
    , {'emqx-pkg',      [ {relx, relx('emqx-pkg')}
                        , {erl_opts, [no_debug_info]}
                        ]}
    , {'emqx-edge',     [ {relx, relx('emqx-edge')}
                        , {erl_opts, [no_debug_info]}
                        ]}
    , {'emqx-edge-pkg', [ {relx, relx('emqx-edge-pkg')}
                        , {erl_opts, [no_debug_info]}
                        ]}
    , {check,           [ {erl_opts, [debug_info]}
                        ]}
    , {test,            [ {deps, test_deps()}
                        , {erl_opts, [debug_info] ++ erl_opts_i()}
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
    , {overlay_vars,["vars/vars-cloud.config","vars/vars-bin.config"]}
    ];
do_relx('emqx-pkg', Vsn) ->
    [ {release, {emqx, Vsn}, relx_apps(cloud)}
    , {overlay, relx_overlay(cloud)}
    , {overlay_vars,["vars/vars-cloud.config","vars/vars-pkg.config"]}
    ];
do_relx('emqx-edge', Vsn) ->
    [ {release, {emqx, Vsn}, relx_apps(edge)}
    , {overlay, relx_overlay(edge)}
    , {overlay_vars,["vars/vars-edge.config","vars/vars-bin.config"]}
    ];
do_relx('emqx-edge-pkg', Vsn) ->
    [ {release, {emqx, Vsn}, relx_apps(edge)}
    , {overlay, relx_overlay(edge)}
    , {overlay_vars,["vars/vars-edge.config","vars/vars-pkg.config"]}
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
    , {emqx_retainer, load}
    , {emqx_management, load}
    , {emqx_dashboard, load}
    , {emqx_bridge_mqtt, load}
    , {emqx_sn, load}
    , {emqx_coap, load}
    , {emqx_stomp, load}
    , {emqx_auth_http, load}
    , {emqx_auth_mysql, load}
    , {emqx_auth_jwt, load}
    , {emqx_auth_mnesia, load}
    , {emqx_web_hook, load}
    , {emqx_recon, load}
    , {emqx_rule_engine, load}
    , {emqx_sasl, load}
    , {emqx_telemetry, load}
    ] ++ do_relx_apps(ReleaseType) ++ [bcrypt || provide_bcrypt_release(ReleaseType)].

do_relx_apps(cloud) ->
    [ {emqx_lwm2m, load}
    , {emqx_auth_ldap, load}
    , {emqx_auth_pgsql, load}
    , {emqx_auth_redis, load}
    , {emqx_auth_mongo, load}
    , {emqx_lua_hook, load}
    , {emqx_exhook, load}
    , {emqx_exproto, load}
    , {emqx_prometheus, load}
    , {emqx_psk_file, load}
    , {emqx_plugin_template, load}
    , {observer, load}
    , luerl
    , xmerl
    ];
do_relx_apps(_) ->
    [].

relx_overlay(ReleaseType) ->
    [ {mkdir,"etc/"}
    , {mkdir,"etc/emqx.d/"}
    , {mkdir,"log/"}
    , {mkdir,"data/"}
    , {mkdir,"data/mnesia"}
    , {mkdir,"data/configs"}
    , {mkdir,"data/scripts"}
    , {template, "data/loaded_plugins.tmpl", "data/loaded_plugins"}
    , {template, "data/loaded_modules.tmpl", "data/loaded_modules"}
    , {template,"data/emqx_vars","releases/emqx_vars"}
    , {copy,"_build/default/lib/cuttlefish/cuttlefish","bin/"}
    , {copy,"bin/*","bin/"}
    , {template,"etc/*.conf","etc/"}
    , {template,"etc/emqx.d/*.conf","etc/emqx.d/"}
    , {copy,"priv/emqx.schema","releases/{{release_version}}/"}
    , {copy, "etc/certs","etc/"}
    , {copy,"bin/emqx.cmd","bin/emqx.cmd-{{release_version}}"}
    , {copy,"bin/emqx_ctl.cmd","bin/emqx_ctl.cmd-{{release_version}}"}
    , {copy,"bin/emqx","bin/emqx-{{release_version}}"}
    , {copy,"bin/emqx_ctl","bin/emqx_ctl-{{release_version}}"}
    , {copy,"bin/install_upgrade.escript", "bin/install_upgrade.escript-{{release_version}}"}
    , {copy,"bin/nodetool","bin/nodetool-{{release_version}}"}
    , {copy,"_build/default/lib/cuttlefish/cuttlefish","bin/cuttlefish-{{release_version}}"}
    ] ++ do_relx_overlay(ReleaseType).

do_relx_overlay(cloud) ->
    [ {template,"etc/emqx_cloud.d/*.conf","etc/emqx.d/"}
    , {template,"etc/emqx_cloud.d/vm.args","etc/vm.args"}
    ];
do_relx_overlay(edge) ->
    [ {template,"etc/emqx_edge.d/*.conf","etc/emqx.d/"}
    , {template,"etc/emqx_edge.d/vm.args.edge","etc/vm.args"}
    ].

env(Name, Default) ->
    case os:getenv(Name) of
        "" -> Default;
        false -> Default;
        Value -> Value
    end.

get_vsn() ->
    PkgVsn = case env("PKG_VSN", false) of
                 false -> os:cmd("git describe --tags --always");
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
    [{i, Dir}  || Dir <- filelib:wildcard("apps/**/include")].
