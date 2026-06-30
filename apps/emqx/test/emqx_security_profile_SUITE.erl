%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_security_profile_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/asserts.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(PROFILE_ENV_VAR, "EMQX_SECURITY_PROFILE").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    WorkDir = emqx_cth_suite:work_dir(Config),
    Apps = emqx_cth_suite:start(
        [emqx],
        #{work_dir => WorkDir}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(t_legacy, Config) ->
    emqx_common_test_helpers:set_security_profile("legacy"),
    Config;
init_per_testcase(t_hardened, Config) ->
    emqx_common_test_helpers:set_security_profile("hardened"),
    Config;
init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, _Config) ->
    emqx_common_test_helpers:clear_security_profile().

t_legacy(_) ->
    {ok, _} = emqx:update_config([listeners], #{}),

    %% verify the mode itself
    ?assertEqual(legacy, emqx_security_profile:profile()),
    ?assertEqual(ignore, emqx_security_profile:policy(authn_backend_failure)),
    ?assertEqual(ignore, emqx_security_profile:policy(authz_backend_failure)),
    ?assertEqual(verify_none, emqx_security_profile:policy(outbound_tls_verify)),
    ?assertEqual(ignore, emqx_security_profile:policy(authn_jwt_missing)),

    %% Full defaults
    ?assertEqual({{0, 0, 0, 0}, 1883}, emqx:get_config([listeners, tcp, default, bind])),
    ?assertEqual({{0, 0, 0, 0}, 8883}, emqx:get_config([listeners, ssl, default, bind])),
    ?assertEqual({{0, 0, 0, 0}, 8083}, emqx:get_config([listeners, ws, default, bind])),
    ?assertEqual({{0, 0, 0, 0}, 8084}, emqx:get_config([listeners, wss, default, bind])),

    %% Default bind values - only port values for legacy profile behavior
    {ok, _} = emqx:update_config([listeners], #{
        <<"tcp">> => #{<<"default">> => #{}},
        <<"ssl">> => #{<<"default">> => #{}},
        <<"ws">> => #{<<"default">> => #{}},
        <<"wss">> => #{<<"default">> => #{}}
    }),
    ?assertEqual(1883, emqx:get_config([listeners, tcp, default, bind])),
    ?assertEqual(8883, emqx:get_config([listeners, ssl, default, bind])),
    ?assertEqual(8083, emqx:get_config([listeners, ws, default, bind])),
    ?assertEqual(8084, emqx:get_config([listeners, wss, default, bind])),

    %% Port values as bind values - no change for legacy profile behavior
    {ok, _} = emqx:update_config([listeners], #{
        <<"tcp">> => #{<<"default">> => #{<<"bind">> => 1883}},
        <<"ssl">> => #{<<"default">> => #{<<"bind">> => 8883}},
        <<"ws">> => #{<<"default">> => #{<<"bind">> => 8083}},
        <<"wss">> => #{<<"default">> => #{<<"bind">> => 8084}}
    }),
    ?assertEqual(1883, emqx:get_config([listeners, tcp, default, bind])),
    ?assertEqual(8883, emqx:get_config([listeners, ssl, default, bind])),
    ?assertEqual(8083, emqx:get_config([listeners, ws, default, bind])),
    ?assertEqual(8084, emqx:get_config([listeners, wss, default, bind])).

t_hardened(_) ->
    {ok, _} = emqx:update_config([listeners], #{}),

    %% verify the mode itself
    ?assertEqual(hardened, emqx_security_profile:profile()),
    ?assertEqual(deny, emqx_security_profile:policy(authn_backend_failure)),
    ?assertEqual(deny, emqx_security_profile:policy(authz_backend_failure)),
    ?assertEqual(verify_peer, emqx_security_profile:policy(outbound_tls_verify)),
    ?assertEqual(deny, emqx_security_profile:policy(authn_jwt_missing)),

    %% Full defaults are secure in hardened profile
    ?assertEqual({{127, 0, 0, 1}, 1883}, emqx:get_config([listeners, tcp, default, bind])),
    ?assertEqual({{127, 0, 0, 1}, 8883}, emqx:get_config([listeners, ssl, default, bind])),
    ?assertEqual({{127, 0, 0, 1}, 8083}, emqx:get_config([listeners, ws, default, bind])),
    ?assertEqual({{127, 0, 0, 1}, 8084}, emqx:get_config([listeners, wss, default, bind])),

    %% Default bind values, port + loopback should be used for hardened profile behavior
    {ok, _} = emqx:update_config([listeners], #{
        <<"tcp">> => #{<<"default">> => #{}},
        <<"ssl">> => #{<<"default">> => #{}},
        <<"ws">> => #{<<"default">> => #{}},
        <<"wss">> => #{<<"default">> => #{}}
    }),
    ?assertEqual({{127, 0, 0, 1}, 1883}, emqx:get_config([listeners, tcp, default, bind])),
    ?assertEqual({{127, 0, 0, 1}, 8883}, emqx:get_config([listeners, ssl, default, bind])),
    ?assertEqual({{127, 0, 0, 1}, 8083}, emqx:get_config([listeners, ws, default, bind])),
    ?assertEqual({{127, 0, 0, 1}, 8084}, emqx:get_config([listeners, wss, default, bind])),

    %% Port values as bind values, loopback should still be applied in hardened profile behavior
    {ok, _} = emqx:update_config([listeners], #{
        <<"tcp">> => #{<<"default">> => #{<<"bind">> => 1883}},
        <<"ssl">> => #{<<"default">> => #{<<"bind">> => 8883}},
        <<"ws">> => #{<<"default">> => #{<<"bind">> => 8083}},
        <<"wss">> => #{<<"default">> => #{<<"bind">> => 8084}}
    }),
    ?assertEqual({{127, 0, 0, 1}, 1883}, emqx:get_config([listeners, tcp, default, bind])),
    ?assertEqual({{127, 0, 0, 1}, 8883}, emqx:get_config([listeners, ssl, default, bind])),
    ?assertEqual({{127, 0, 0, 1}, 8083}, emqx:get_config([listeners, ws, default, bind])),
    ?assertEqual({{127, 0, 0, 1}, 8084}, emqx:get_config([listeners, wss, default, bind])).

t_uppercase_profile_rejected(_) ->
    os:putenv(?PROFILE_ENV_VAR, "HARDENED"),
    emqx_security_profile:clear_profile(),
    ?assertExit({invalid_security_profile, _}, emqx_security_profile:profile()).
