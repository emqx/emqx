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

all() ->
    All = emqx_common_test_helpers:all(?MODULE),
    [
        {group, legacy},
        {group, hardened}
    ] ++ (All -- [t_profile]).

groups() ->
    [
        {legacy, [], [t_profile]},
        {hardened, [], [t_profile]}
    ].

init_per_suite(Config) ->
    emqx_common_test_helpers:clear_security_profile(),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:clear_security_profile().

init_per_group(Profile, Config) when Profile =:= legacy; Profile =:= hardened ->
    emqx_common_test_helpers:set_security_profile(Profile),
    Apps = emqx_cth_suite:start(
        [emqx],
        #{work_dir => emqx_cth_suite:work_dir(Profile, Config)}
    ),
    [{apps, Apps}, {security_profile, Profile} | Config].

end_per_group(_Profile, Config) ->
    emqx_cth_suite:stop(?config(apps, Config)),
    emqx_common_test_helpers:clear_security_profile().

t_unset_default(_) ->
    emqx_common_test_helpers:clear_security_profile(),
    ?assertEqual(legacy, emqx_security_profile:profile()).

t_profile(Config) ->
    Profile = ?config(security_profile, Config),
    {ok, _} = emqx:update_config([listeners], #{}),

    ?assertEqual(Profile, emqx_security_profile:profile()),
    assert_policies(Profile),

    assert_default_binds(Profile, full),

    {ok, _} = emqx:update_config([listeners], #{
        <<"tcp">> => #{<<"default">> => #{}},
        <<"ssl">> => #{<<"default">> => #{}},
        <<"ws">> => #{<<"default">> => #{}},
        <<"wss">> => #{<<"default">> => #{}}
    }),
    assert_default_binds(Profile, schema),

    {ok, _} = emqx:update_config([listeners], #{
        <<"tcp">> => #{<<"default">> => #{<<"bind">> => 1883}},
        <<"ssl">> => #{<<"default">> => #{<<"bind">> => 8883}},
        <<"ws">> => #{<<"default">> => #{<<"bind">> => 8083}},
        <<"wss">> => #{<<"default">> => #{<<"bind">> => 8084}}
    }),
    assert_default_binds(Profile, schema).

t_uppercase_profile_rejected(_) ->
    os:putenv(?PROFILE_ENV_VAR, "HARDENED"),
    emqx_security_profile:clear_profile(),
    try
        ?assertExit({invalid_security_profile, _}, emqx_security_profile:profile())
    after
        emqx_common_test_helpers:clear_security_profile()
    end.

assert_policies(legacy) ->
    ?assertEqual(ignore, emqx_security_profile:policy(authn_backend_failure)),
    ?assertEqual(ignore, emqx_security_profile:policy(authz_backend_failure)),
    ?assertEqual(verify_none, emqx_security_profile:policy(outbound_tls_verify)),
    ?assertEqual(ignore, emqx_security_profile:policy(authn_jwt_missing)),
    ?assertEqual(false, emqx_security_profile:policy(saml_signature_verification)),
    ?assertEqual(false, emqx_security_profile:policy(internal_subscription_checks)),
    ?assertEqual(legacy, emqx_security_profile:policy(authz_context)),
    ?assertEqual(false, emqx_security_profile:policy(delayed_publish_reauthorization)),
    ?assertEqual(
        honor_failed_action,
        emqx_security_profile:policy(exhook_server_unavailable)
    ),
    ?assertEqual(ignore, emqx_security_profile:policy(exhook_message_publish_failure)),
    ?assertEqual(optional, emqx_security_profile:policy(plugin_install_sha256_binding)),
    ?assertEqual(false, emqx_security_profile:policy(authn_builtin_default_autogenerate_password)),
    ?assertEqual(sha256, emqx_security_profile:policy(authn_builtin_default_manual_password_hash)),
    ?assertEqual(true, emqx_security_profile:policy(authn_builtin_accept_weak_password_hash));
assert_policies(hardened) ->
    ?assertEqual(deny, emqx_security_profile:policy(authn_backend_failure)),
    ?assertEqual(deny, emqx_security_profile:policy(authz_backend_failure)),
    ?assertEqual(verify_peer, emqx_security_profile:policy(outbound_tls_verify)),
    ?assertEqual(deny, emqx_security_profile:policy(authn_jwt_missing)),
    ?assertEqual(true, emqx_security_profile:policy(saml_signature_verification)),
    ?assertEqual(true, emqx_security_profile:policy(internal_subscription_checks)),
    ?assertEqual(restricted, emqx_security_profile:policy(authz_context)),
    ?assertEqual(true, emqx_security_profile:policy(delayed_publish_reauthorization)),
    ?assertEqual(deny, emqx_security_profile:policy(exhook_server_unavailable)),
    ?assertEqual(deny, emqx_security_profile:policy(exhook_message_publish_failure)),
    ?assertEqual(required, emqx_security_profile:policy(plugin_install_sha256_binding)),
    ?assertEqual(true, emqx_security_profile:policy(authn_builtin_default_autogenerate_password)),
    ?assertEqual(pbkdf2, emqx_security_profile:policy(authn_builtin_default_manual_password_hash)),
    ?assertEqual(false, emqx_security_profile:policy(authn_builtin_accept_weak_password_hash)).

assert_default_binds(Profile, Source) ->
    ?assertEqual(
        expected_bind(Profile, Source, 1883), emqx:get_config([listeners, tcp, default, bind])
    ),
    ?assertEqual(
        expected_bind(Profile, Source, 8883), emqx:get_config([listeners, ssl, default, bind])
    ),
    ?assertEqual(
        expected_bind(Profile, Source, 8083), emqx:get_config([listeners, ws, default, bind])
    ),
    ?assertEqual(
        expected_bind(Profile, Source, 8084), emqx:get_config([listeners, wss, default, bind])
    ).

expected_bind(legacy, full, Port) -> {{0, 0, 0, 0}, Port};
expected_bind(legacy, schema, Port) -> Port;
expected_bind(hardened, _Source, Port) -> {{127, 0, 0, 1}, Port}.
