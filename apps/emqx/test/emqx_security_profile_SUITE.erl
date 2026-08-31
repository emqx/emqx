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

    assert_default_binds(Profile),

    {ok, _} = emqx:update_config([listeners], #{
        <<"tcp">> => #{<<"default">> => #{}},
        <<"ssl">> => #{<<"default">> => #{}},
        <<"ws">> => #{<<"default">> => #{}},
        <<"wss">> => #{<<"default">> => #{}}
    }),
    assert_default_binds(Profile),

    {ok, _} = emqx:update_config([listeners], #{
        <<"tcp">> => #{<<"default">> => #{<<"bind">> => 1883}},
        <<"ssl">> => #{<<"default">> => #{<<"bind">> => 8883}},
        <<"ws">> => #{<<"default">> => #{<<"bind">> => 8083}},
        <<"wss">> => #{<<"default">> => #{<<"bind">> => 8084}}
    }),
    assert_default_binds(Profile).

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
    ?assertEqual(true, emqx_security_profile:policy(authn_builtin_accept_weak_password_hash)),
    ?assertEqual(
        false, emqx_security_profile:policy(authn_mnesia_mt_user_conflict_protection)
    ),
    ?assertEqual(false, emqx_security_profile:policy(authz_default_include_mountpoint)),
    ?assertMatch(
        #{authorization := #{include_mountpoint := false}},
        hocon_tconf:check_plain(
            emqx_schema,
            #{<<"authorization">> => #{}},
            #{atom_key => true, required => false},
            [authorization]
        )
    );
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
    ?assertEqual(false, emqx_security_profile:policy(authn_builtin_accept_weak_password_hash)),
    ?assertEqual(
        true, emqx_security_profile:policy(authn_mnesia_mt_user_conflict_protection)
    ),
    ?assertEqual(true, emqx_security_profile:policy(authz_default_include_mountpoint)),
    ?assertMatch(
        #{authorization := #{include_mountpoint := true}},
        hocon_tconf:check_plain(
            emqx_schema,
            #{<<"authorization">> => #{}},
            #{atom_key => true, required => false},
            [authorization]
        )
    ).

assert_default_binds(Profile) ->
    %% Schema defaults are static bare ports; the profile is applied at
    %% listener start.
    ?assertEqual(1883, emqx:get_config([listeners, tcp, default, bind])),
    ?assertEqual(8883, emqx:get_config([listeners, ssl, default, bind])),
    ?assertEqual(8083, emqx:get_config([listeners, ws, default, bind])),
    ?assertEqual(8084, emqx:get_config([listeners, wss, default, bind])),
    ?assertEqual(expected_listen_on(Profile, 1883), esockd_listen_on('tcp:default')),
    ?assertEqual(expected_listen_on(Profile, 8883), esockd_listen_on('ssl:default')),
    ?assertEqual(expected_ranch_addr(Profile, 8083), ranch:get_addr('ws:default')),
    ?assertEqual(expected_ranch_addr(Profile, 8084), ranch:get_addr('wss:default')).

expected_listen_on(legacy, Port) -> Port;
expected_listen_on(hardened, Port) -> {{127, 0, 0, 1}, Port}.

expected_ranch_addr(legacy, Port) -> {{0, 0, 0, 0}, Port};
expected_ranch_addr(hardened, Port) -> {{127, 0, 0, 1}, Port}.

esockd_listen_on(Id) ->
    [ListenOn] = [L || {{I, L}, _Pid} <- esockd:listeners(), I =:= Id],
    ListenOn.
