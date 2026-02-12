%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_nats_authn_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).
-compile(nowarn_export_all).

all() ->
    emqx_common_test_helpers:all(?MODULE).

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------

t_build_authn_ctx_and_auth_required(_Config) ->
    Disabled = emqx_nats_authn:build_authn_ctx(undefined, [], undefined, false),
    ?assertEqual(false, emqx_nats_authn:is_auth_required(#{enable_authn => false}, Disabled)),
    ?assertEqual(false, emqx_nats_authn:is_auth_required(#{enable_authn => true}, Disabled)),

    GatewayOnly = emqx_nats_authn:build_authn_ctx(undefined, [], undefined, true),
    ?assertEqual(true, emqx_nats_authn:is_auth_required(#{enable_authn => true}, GatewayOnly)),

    JWTDisabled = emqx_nats_authn:build_authn_ctx(
        undefined,
        [],
        #{enable => false, trusted_operators => [<<"OP_TEST">>]},
        false
    ),
    ?assertEqual(false, emqx_nats_authn:is_auth_required(#{enable_authn => true}, JWTDisabled)),

    Enabled = emqx_nats_authn:build_authn_ctx(
        "token",
        [nkey_pub()],
        jwt_conf(),
        false
    ),
    ?assertEqual(true, emqx_nats_authn:is_auth_required(#{enable_authn => true}, Enabled)).

t_ensure_and_publish_nkey_nonce(_Config) ->
    ConnInfo0 = #{clientid => <<"client-1">>},
    NoNKeyAuthn = emqx_nats_authn:build_authn_ctx(undefined, [], undefined, false),
    ?assertEqual(ConnInfo0, emqx_nats_authn:ensure_nkey_nonce(ConnInfo0, NoNKeyAuthn)),

    NKeyAuthn = emqx_nats_authn:build_authn_ctx(undefined, [nkey_pub()], undefined, false),
    ConnInfo1 = emqx_nats_authn:ensure_nkey_nonce(ConnInfo0, NKeyAuthn),
    Nonce = maps:get(nkey_nonce, ConnInfo1),
    ?assert(is_binary(Nonce)),
    ?assertEqual(24, byte_size(Nonce)),
    ?assertEqual(ConnInfo1, emqx_nats_authn:ensure_nkey_nonce(ConnInfo1, NKeyAuthn)),

    MsgContent = #{auth_required => true},
    ?assertEqual(MsgContent, emqx_nats_authn:maybe_add_nkey_nonce(MsgContent, ConnInfo0)),
    MsgWithNonce = emqx_nats_authn:maybe_add_nkey_nonce(MsgContent, ConnInfo1),
    ?assertEqual(Nonce, maps:get(nonce, MsgWithNonce)).

t_authenticate_token_plain_success(_Config) ->
    Authn = emqx_nats_authn:build_authn_ctx(
        <<"nats-token">>,
        [],
        jwt_conf(),
        false
    ),
    ConnParams = #{
        <<"auth_token">> => <<"nats-token">>,
        <<"jwt">> => <<"not-used">>
    },
    ClientInfo = #{clientid => <<"c1">>, username => undefined},
    {ok, Result} = emqx_nats_authn:authenticate(ConnParams, #{}, ClientInfo, Authn),
    ?assertEqual(token, maps:get(auth_method, Result)),
    ?assertEqual(plain, maps:get(token_type, Result)),
    ?assertEqual(undefined, maps:get(auth_expire_at, Result)).

t_authenticate_token_bcrypt_success(_Config) ->
    Authn = emqx_nats_authn:build_authn_ctx(token_bcrypt_hash(), [], undefined, false),
    ConnParams = #{<<"auth_token">> => token_plain()},
    ClientInfo = #{clientid => <<"c1">>},
    {ok, Result} = emqx_nats_authn:authenticate(ConnParams, #{}, ClientInfo, Authn),
    ?assertEqual(token, maps:get(auth_method, Result)),
    ?assertEqual(bcrypt, maps:get(token_type, Result)).

t_authenticate_token_priority_over_jwt(_Config) ->
    Authn = emqx_nats_authn:build_authn_ctx(
        token_plain(),
        [],
        jwt_conf(),
        false
    ),
    JWT = build_test_jwt(#{<<"sub">> => <<"jwt-user">>}),
    ConnParams = #{
        <<"auth_token">> => <<"bad-token">>,
        <<"jwt">> => JWT
    },
    ?assertEqual(
        {error, {token, invalid_token}},
        emqx_nats_authn:authenticate(ConnParams, #{}, #{}, Authn)
    ).

t_authenticate_token_fallback_to_jwt(_Config) ->
    Authn = emqx_nats_authn:build_authn_ctx(
        token_plain(),
        [],
        jwt_conf(),
        false
    ),
    JWT = build_test_jwt(#{<<"sub">> => <<"jwt-user">>}),
    ConnParams = #{<<"jwt">> => JWT},
    {ok, Result} = emqx_nats_authn:authenticate(ConnParams, #{}, #{username => undefined}, Authn),
    ?assertEqual(jwt, maps:get(auth_method, Result)),
    ?assertEqual(<<"jwt-user">>, maps:get(username, Result)).

t_authenticate_token_priority_over_nkey(_Config) ->
    Authn = emqx_nats_authn:build_authn_ctx(token_plain(), [nkey_pub()], undefined, false),
    Nonce = <<"nonce-token-priority">>,
    ConnParams = #{
        <<"auth_token">> => <<"bad-token">>,
        <<"nkey">> => nkey_pub(),
        <<"sig">> => nkey_sig(Nonce)
    },
    ?assertEqual(
        {error, {token, invalid_token}},
        emqx_nats_authn:authenticate(ConnParams, #{nkey_nonce => Nonce}, #{}, Authn)
    ).

t_authenticate_token_fallback_to_nkey(_Config) ->
    Authn = emqx_nats_authn:build_authn_ctx(token_plain(), [nkey_pub()], undefined, false),
    Nonce = <<"nonce-token-fallback">>,
    ConnParams = #{
        <<"nkey">> => nkey_pub(),
        <<"sig">> => nkey_sig(Nonce)
    },
    {ok, Result} = emqx_nats_authn:authenticate(
        ConnParams,
        #{nkey_nonce => Nonce},
        #{},
        Authn
    ),
    ?assertEqual(nkey, maps:get(auth_method, Result)).

t_authenticate_nkey_success(_Config) ->
    Authn = emqx_nats_authn:build_authn_ctx(undefined, [nkey_pub()], jwt_conf(), false),
    Nonce = <<"nonce-1">>,
    ConnParams = #{
        <<"nkey">> => nkey_pub(),
        <<"sig">> => nkey_sig(Nonce),
        <<"jwt">> => <<"not-used">>
    },
    {ok, Result} = emqx_nats_authn:authenticate(ConnParams, #{nkey_nonce => Nonce}, #{}, Authn),
    ?assertEqual(nkey, maps:get(auth_method, Result)),
    ?assertEqual(nkey_pub(), maps:get(nkey, Result)),
    ?assertEqual(undefined, maps:get(auth_expire_at, Result)).

t_authenticate_nkey_priority_over_jwt(_Config) ->
    Authn = emqx_nats_authn:build_authn_ctx(undefined, [nkey_pub()], jwt_conf(), false),
    Nonce = <<"nonce-2">>,
    JWT = build_test_jwt(#{<<"sub">> => <<"jwt-user">>}),
    ConnParams = #{
        <<"nkey">> => nkey_pub(),
        <<"sig">> => invalid_nkey_sig(Nonce),
        <<"jwt">> => JWT
    },
    ?assertEqual(
        {error, {nkey, invalid_nkey_sig}},
        emqx_nats_authn:authenticate(ConnParams, #{nkey_nonce => Nonce}, #{}, Authn)
    ).

t_authenticate_nkey_fallback_and_validation(_Config) ->
    Authn = emqx_nats_authn:build_authn_ctx(undefined, [nkey_pub()], jwt_conf(), false),
    JWT = build_test_jwt(#{<<"sub">> => <<"jwt-user">>}),
    ConnInfo = #{nkey_nonce => <<"nonce-3">>},

    {ok, Result} = emqx_nats_authn:authenticate(
        #{<<"jwt">> => JWT},
        ConnInfo,
        #{username => undefined},
        Authn
    ),
    ?assertEqual(jwt, maps:get(auth_method, Result)),

    ?assertEqual(
        {error, {nkey, nkey_sig_required}},
        emqx_nats_authn:authenticate(#{<<"nkey">> => nkey_pub()}, ConnInfo, #{}, Authn)
    ),

    ?assertEqual(
        {error, {nkey, nkey_nonce_unavailable}},
        emqx_nats_authn:authenticate(
            #{<<"nkey">> => nkey_pub(), <<"sig">> => nkey_sig(<<"nonce-x">>)},
            #{},
            #{},
            Authn
        )
    ).

t_authenticate_jwt_time_validation(_Config) ->
    ExpiredJWT = build_test_jwt(#{<<"sub">> => <<"jwt-user">>, <<"exp">> => now_seconds() - 10}),
    FutureNbfJWT = build_test_jwt(#{<<"sub">> => <<"jwt-user">>, <<"nbf">> => now_seconds() + 3600}),
    Authn = emqx_nats_authn:build_authn_ctx(undefined, [], jwt_conf(), false),

    ?assertEqual(
        {error, {jwt, jwt_expired}},
        emqx_nats_authn:authenticate(#{<<"jwt">> => ExpiredJWT}, #{}, #{}, Authn)
    ),
    ?assertEqual(
        {error, {jwt, jwt_not_before}},
        emqx_nats_authn:authenticate(#{<<"jwt">> => FutureNbfJWT}, #{}, #{}, Authn)
    ),
    ?assertEqual(
        {error, {jwt, invalid_jwt_format}},
        emqx_nats_authn:authenticate(#{<<"jwt">> => <<"invalid-jwt">>}, #{}, #{}, Authn)
    ).

t_authenticate_jwt_verify_options(_Config) ->
    ExpiredJWT = build_test_jwt(#{<<"sub">> => <<"jwt-user">>, <<"exp">> => now_seconds() - 10}),
    FutureNbfJWT = build_test_jwt(#{<<"sub">> => <<"jwt-user">>, <<"nbf">> => now_seconds() + 3600}),

    IgnoreExpAuthn = emqx_nats_authn:build_authn_ctx(
        undefined,
        [],
        jwt_conf(#{verify_exp => false}),
        false
    ),
    {ok, ExpResult} = emqx_nats_authn:authenticate(
        #{<<"jwt">> => ExpiredJWT},
        #{},
        #{},
        IgnoreExpAuthn
    ),
    ?assertEqual(jwt, maps:get(auth_method, ExpResult)),

    IgnoreNbfAuthn = emqx_nats_authn:build_authn_ctx(
        undefined,
        [],
        jwt_conf(#{verify_nbf => false}),
        false
    ),
    {ok, NbfResult} = emqx_nats_authn:authenticate(
        #{<<"jwt">> => FutureNbfJWT},
        #{},
        #{},
        IgnoreNbfAuthn
    ),
    ?assertEqual(jwt, maps:get(auth_method, NbfResult)).

t_authenticate_jwt_claim_projection(_Config) ->
    Exp = now_seconds() + 120,
    Claims = #{
        <<"sub">> => <<"jwt-user">>,
        <<"exp">> => Exp,
        <<"permissions">> => #{
            <<"pub">> => #{
                <<"allow">> => [<<"orders.*">>, 42],
                <<"deny">> => [<<"orders.secret">>]
            },
            <<"subscribe">> => #{
                <<"allow">> => [<<"events.>">>],
                <<"deny">> => [<<"events.internal">>]
            }
        }
    },
    JWT = build_test_jwt(Claims),
    Authn = emqx_nats_authn:build_authn_ctx(undefined, [], jwt_conf(), false),
    {ok, Result} = emqx_nats_authn:authenticate(
        #{<<"jwt">> => JWT},
        #{},
        #{username => undefined},
        Authn
    ),

    ?assertEqual(jwt, maps:get(auth_method, Result)),
    ?assertEqual(<<"jwt-user">>, maps:get(username, Result)),
    ?assertEqual(
        erlang:convert_time_unit(Exp, second, millisecond), maps:get(auth_expire_at, Result)
    ),
    ?assertEqual(
        #{
            publish => #{allow => [<<"orders.*">>, <<"42">>], deny => [<<"orders.secret">>]},
            subscribe => #{allow => [<<"events.>">>], deny => [<<"events.internal">>]}
        },
        maps:get(jwt_permissions, Result)
    ).

t_authenticate_jwt_missing_token_behavior(_Config) ->
    JWTOnlyAuthn = emqx_nats_authn:build_authn_ctx(undefined, [], jwt_conf(), false),
    ?assertEqual(
        {error, {jwt, jwt_required}},
        emqx_nats_authn:authenticate(#{}, #{}, #{}, JWTOnlyAuthn)
    ),

    JWTWithGatewayAuthn = emqx_nats_authn:build_authn_ctx(undefined, [], jwt_conf(), true),
    ?assertEqual(
        {continue, #{}},
        emqx_nats_authn:authenticate(#{}, #{}, #{}, JWTWithGatewayAuthn)
    ).

t_authenticate_disabled_jwt_and_empty_authn(_Config) ->
    JWTDisabledAuthn = emqx_nats_authn:build_authn_ctx(
        undefined,
        [],
        #{trusted_operators => []},
        false
    ),
    ?assertEqual(
        {continue, #{}},
        emqx_nats_authn:authenticate(#{}, #{}, #{}, JWTDisabledAuthn)
    ),

    EmptyAuthn = emqx_nats_authn:build_authn_ctx(undefined, [], undefined, false),
    ?assertEqual(
        {continue, #{}},
        emqx_nats_authn:authenticate(#{}, #{}, #{}, EmptyAuthn)
    ).

t_authenticate_required_errors_and_invalid_nkey(_Config) ->
    TokenOnlyAuthn = emqx_nats_authn:build_authn_ctx(token_plain(), [], undefined, false),
    ?assertEqual(
        {error, {token, token_required}},
        emqx_nats_authn:authenticate(#{}, #{}, #{}, TokenOnlyAuthn)
    ),

    NKeyOnlyAuthn = emqx_nats_authn:build_authn_ctx(undefined, [nkey_pub()], undefined, false),
    ?assertEqual(
        {error, {nkey, nkey_required}},
        emqx_nats_authn:authenticate(#{}, #{}, #{}, NKeyOnlyAuthn)
    ),

    ?assertEqual(
        {error, {nkey, invalid_nkey}},
        emqx_nats_authn:authenticate(
            #{
                <<"nkey">> => replace_first_char(nkey_pub(), $V),
                <<"sig">> => <<"sig">>
            },
            #{nkey_nonce => <<"nonce">>},
            #{},
            NKeyOnlyAuthn
        )
    ).

t_authenticate_jwt_decode_error_branches(_Config) ->
    Authn = emqx_nats_authn:build_authn_ctx(undefined, [], jwt_conf(), false),
    Header = base64url_encode(emqx_utils_json:encode(#{alg => <<"none">>})),
    NonMapPayload = base64url_encode(<<"1">>),
    NonMapJWT = <<Header/binary, ".", NonMapPayload/binary, ".sig">>,
    ?assertEqual(
        {error, {jwt, invalid_jwt_claims}},
        emqx_nats_authn:authenticate(#{<<"jwt">> => NonMapJWT}, #{}, #{}, Authn)
    ),

    BadB64JWT = <<Header/binary, ".*.sig">>,
    ?assertEqual(
        {error, {jwt, invalid_jwt_base64}},
        emqx_nats_authn:authenticate(#{<<"jwt">> => BadB64JWT}, #{}, #{}, Authn)
    ).

t_authn_internal_helper_branches(_Config) ->
    ?assertEqual({error, token_disabled}, emqx_nats_authn:token_authenticate(<<"t">>, #{}, #{})),

    ?assert(emqx_nats_authn:is_bcrypt_token(<<"$2a$hash">>)),
    ?assert(emqx_nats_authn:is_bcrypt_token(<<"$2y$hash">>)),

    ?assertEqual(undefined, emqx_nats_authn:normalize_jwt_config(#{<<"enable">> => false})),
    ?assertEqual(undefined, emqx_nats_authn:normalize_jwt_config(42)),
    ?assertEqual([], emqx_nats_authn:normalize_nkeys(<<"not-a-list">>)),
    ?assertEqual(undefined, emqx_nats_authn:normalize_token(<<>>)),
    ?assertEqual(#{}, emqx_nats_authn:normalize_map(not_a_map)),

    ?assertEqual(
        [<<"orders.*">>],
        emqx_nats_authn:normalize_subject_list([<<"orders.*">>, <<>>])
    ),
    ?assertEqual([], emqx_nats_authn:normalize_subject_list(<<"not-a-list">>)),

    ?assertEqual(ok, emqx_nats_authn:verify_jwt_exp(#{<<"exp">> => 10.9}, true, 9)),
    ?assertEqual(ok, emqx_nats_authn:verify_jwt_nbf(#{<<"nbf">> => 10}, true, 10)),
    ?assertEqual(ok, emqx_nats_authn:verify_jwt_nbf(#{<<"nbf">> => 10.9}, true, 10)),
    ?assertEqual(
        erlang:convert_time_unit(10, second, millisecond),
        emqx_nats_authn:jwt_claim_expire_at(#{<<"exp">> => 10.9})
    ),
    ?assertEqual(
        #{username => <<"u">>},
        emqx_nats_authn:maybe_set_clientinfo_from_jwt_claims(#{username => <<"u">>}, #{})
    ),

    ?assertEqual(true, emqx_nats_authn:to_bool(<<"true">>)),
    ?assertEqual(false, emqx_nats_authn:to_bool(<<"false">>)),
    ?assertEqual(true, emqx_nats_authn:to_bool("true")),
    ?assertEqual(false, emqx_nats_authn:to_bool("false")),
    ?assertEqual(false, emqx_nats_authn:to_bool(<<"TRUE">>)).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

jwt_conf() ->
    jwt_conf(#{}).

jwt_conf(Overrides) ->
    maps:merge(
        #{
            trusted_operators => [<<"OP_TEST">>],
            cache_ttl => <<"5m">>,
            verify_exp => true,
            verify_nbf => true
        },
        Overrides
    ).

build_test_jwt(Claims) ->
    Header = base64url_encode(emqx_utils_json:encode(#{alg => <<"none">>, typ => <<"JWT">>})),
    Payload = base64url_encode(emqx_utils_json:encode(Claims)),
    <<Header/binary, ".", Payload/binary, ".sig">>.

base64url_encode(Bin) ->
    base64:encode(Bin, #{mode => urlsafe, padding => false}).

now_seconds() ->
    erlang:system_time(second).

token_plain() ->
    <<"nats_token">>.

token_bcrypt_hash() ->
    _ = application:ensure_all_started(bcrypt),
    Salt = <<"$2b$12$wtY3h20mUjjmeaClpqZVve">>,
    emqx_passwd:hash({bcrypt, Salt}, token_plain()).

nkey_pub() ->
    <<"UB4G32YJ2GVZG3KTC3Z7BLIU3PXPJC2Y4QF6SNJUN2XIF3M3E3NDEUCZ">>.

nkey_priv() ->
    <<205, 42, 56, 73, 83, 88, 159, 152, 35, 244, 15, 34, 196, 39, 226, 60, 111, 109, 0, 79, 72,
        148, 60, 239, 181, 139, 118, 231, 215, 12, 158, 116>>.

nkey_sig(Nonce) ->
    Sig = crypto:sign(eddsa, none, Nonce, [nkey_priv(), ed25519]),
    base64:encode(Sig, #{mode => urlsafe, padding => false}).

invalid_nkey_sig(Nonce) ->
    Sig = nkey_sig(Nonce),
    SigBin = base64:decode(Sig, #{mode => urlsafe, padding => false}),
    <<First:8, Rest/binary>> = SigBin,
    BadSigBin = <<(First bxor 1), Rest/binary>>,
    base64:encode(BadSigBin, #{mode => urlsafe, padding => false}).

replace_first_char(<<_Old, Rest/binary>>, New) ->
    <<New, Rest/binary>>.
