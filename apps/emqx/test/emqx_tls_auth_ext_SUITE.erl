%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_tls_auth_ext_SUITE).
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
    [{group, legacy}, {group, hardened}].

groups() ->
    Tests = emqx_common_test_helpers:all(?MODULE),
    [{legacy, [], Tests}, {hardened, [], Tests}].

init_per_suite(Config) ->
    emqx_common_test_helpers:clear_security_profile(),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:clear_security_profile().

init_per_group(Profile, Config) when Profile =:= legacy; Profile =:= hardened ->
    ok = emqx_common_test_helpers:set_security_profile(Profile),
    Apps = emqx_cth_suite:start(
        [{emqx, ?BASE_CONF}],
        #{work_dir => emqx_cth_suite:work_dir(Profile, Config)}
    ),
    emqx_listeners:restart(),
    [{apps, Apps}, {security_profile, Profile} | Config].

end_per_group(Profile, Config) when Profile =:= legacy; Profile =:= hardened ->
    emqx_cth_suite:stop(?config(apps, Config)),
    emqx_common_test_helpers:clear_security_profile().

t_conf_check_default(_Config) ->
    Opts = esockd:get_options(listener_ref(ssl, default)),
    SSLOpts = proplists:get_value(ssl_options, Opts),
    ?assertEqual(none, proplists:lookup(partial_chain, SSLOpts)),
    ?assertEqual(none, proplists:lookup(verify_fun, SSLOpts)).

t_conf_check_auth_ext(_Config) ->
    Opts = esockd:get_options(listener_ref(ssl, auth_ext)),
    SSLOpts = proplists:get_value(ssl_options, Opts),
    %% Even when partial_chain is set to `false`
    ?assertMatch(Fun when is_function(Fun), proplists:get_value(partial_chain, SSLOpts)),
    ?assertMatch({Fun, _} when is_function(Fun), proplists:get_value(verify_fun, SSLOpts)).

listener_ref(Type, Name) ->
    {emqx_listeners:listener_id(Type, Name), emqx_config:get([listeners, Type, Name, bind])}.
