-module('rebar.config').

-export([do/2]).

do(_Dir, CONFIG) ->
    maybe_dump(CONFIG ++ config()).

config() ->
    [ {plugins, plugins()}
    , {profiles, profiles()}
    ].

plugins() ->
    [ {relup_helper,{git,"https://github.com/emqx/relup_helper", {branch,"master"}}}
    ].

test_deps() ->
    [ {bbmustache, "1.10.0"}
    , {emqtt, {git, "https://github.com/emqx/emqtt", {tag, "1.2.0"}}}
    , {emqx_ct_helpers, {git, "https://github.com/emqx/emqx-ct-helpers", {tag, "1.3.0"}}}
    , meck
    ].

profiles() ->
    [ {'emqx',          [{relx, relx('emqx')}]}
    , {'emqx-pkg',      [{relx, relx('emqx-pkg')}]}
    , {'emqx-edge',     [{relx, relx('emqx-edge')}]}
    , {'emqx-edge-pkg', [{relx, relx('emqx-edge-pkg')}]}
    , {test,            [{deps, test_deps()}, {erl_opts, [debug_info]}]}
    ].

relx(Profile) ->
    Vsn = get_vsn(),
    [ {include_src,false}
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
    ] ++ do_relx_apps(ReleaseType).

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
    , {copy,"priv/emqx.schema","releases/{{rel_vsn}}/"}
    , {copy, "etc/certs","etc/"}
    , {copy,"bin/emqx.cmd","bin/emqx.cmd-{{rel_vsn}}"}
    , {copy,"bin/emqx_ctl.cmd","bin/emqx_ctl.cmd-{{rel_vsn}}"}
    , {copy,"bin/emqx","bin/emqx-{{rel_vsn}}"}
    , {copy,"bin/emqx_ctl","bin/emqx_ctl-{{rel_vsn}}"}
    , {copy,"bin/install_upgrade.escript", "bin/install_upgrade.escript-{{rel_vsn}}"}
    , {copy,"bin/nodetool","bin/nodetool-{{rel_vsn}}"}
    , {copy,"_build/default/lib/cuttlefish/cuttlefish","bin/cuttlefish-{{rel_vsn}}"}
    ] ++ do_relx_overlay(ReleaseType).

do_relx_overlay(cloud) ->
    [ {template,"etc/emqx_cloud.d/*.conf","etc/emqx.d/"}
    , {template,"etc/emqx_cloud.d/vm.args","etc/vm.args"}
    ];
do_relx_overlay(edge) ->
    [ {template,"etc/emqx_edge.d/*.conf","etc/emqx.d/"}
    , {template,"etc/emqx_edge.d/vm.args.edge","etc/vm.args"}
    ].

get_vsn() ->
    PkgVsn = case os:getenv("PKG_VSN") of
                 false -> os:cmd("git describe --tags");
                 Vsn -> Vsn
             end,
    Vsn2 = re:replace(PkgVsn, "v", "", [{return ,list}]),
    re:replace(Vsn2, "\n", "", [{return ,list}]).

maybe_dump(Config) ->
    case os:getenv("DEBUG") of
        "" -> ok;
        false -> ok;
        _ -> file:write_file("rebar.config.rendered", [io_lib:format("~p.\n", [I]) || I <- Config])
    end,
    Config.
