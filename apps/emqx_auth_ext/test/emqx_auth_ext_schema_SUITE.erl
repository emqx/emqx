%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_auth_ext_schema_SUITE).
-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(BASE_CONF,
    "\n"
    "    listeners.ssl.auth_ext.bind = 28883\n"
    "    listeners.ssl.auth_ext.enable = true\n"
    "    listeners.ssl.auth_ext.ssl_options.partial_chain = false\n"
    "    listeners.ssl.auth_ext.ssl_options.verify = verify_peer\n"
    "    listeners.ssl.auth_ext.ssl_options.verify_peer_ext_key_usage = \"clientAuth\"\n"
    "    "
).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    %% injection happens when module is loaded.
    code:load_file(emqx_auth_ext),
    Apps = emqx_cth_suite:start(
        [
            emqx,
            {emqx_conf, ?BASE_CONF}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    emqx_listeners:restart(),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    ok = emqx_cth_suite:stop(Apps),
    code:delete(emqx_auth_ext),
    code:purge(emqx_auth_ext),
    ok.

t_conf_check_default(_Config) ->
    Opts = esockd:get_options({'ssl:default', {{0, 0, 0, 0}, 8883}}),
    SSLOpts = proplists:get_value(ssl_options, Opts),
    ?assertEqual(none, proplists:lookup(partial_chain, SSLOpts)),
    ?assertEqual(none, proplists:lookup(verify_fun, SSLOpts)).

t_conf_check_auth_ext(_Config) ->
    Opts = esockd:get_options({'ssl:auth_ext', 28883}),
    SSLOpts = proplists:get_value(ssl_options, Opts),
    %% Even when partial_chain is set to `false`
    ?assertMatch(Fun when is_function(Fun), proplists:get_value(partial_chain, SSLOpts)),
    ?assertMatch({Fun, _} when is_function(Fun), proplists:get_value(verify_fun, SSLOpts)).
