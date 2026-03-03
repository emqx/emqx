%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_nats_authn_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-define(NKEY_ACCOUNT_PREFIX, 16#00).
-define(NKEY_OPERATOR_PREFIX, 16#70).
-define(NKEY_USER_PREFIX, 16#A0).

all() ->
    emqx_common_test_helpers:all(?MODULE).

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------

t_build_authn_ctx_and_auth_required(_Config) ->
    Disabled = mk_authn_ctx(undefined, [], undefined, false),
    ?assertEqual(false, emqx_nats_authn:is_auth_required(#{enable_authn => false}, Disabled)),
    ?assertEqual(false, emqx_nats_authn:is_auth_required(#{enable_authn => true}, Disabled)),

    GatewayOnly = mk_authn_ctx(undefined, [], undefined, true),
    ?assertEqual(true, emqx_nats_authn:is_auth_required(#{enable_authn => true}, GatewayOnly)),

    JWTDisabled = mk_authn_ctx(
        undefined,
        [],
        #{enable => false, trusted_operators => [<<"OP_TEST">>]},
        false
    ),
    ?assertEqual(false, emqx_nats_authn:is_auth_required(#{enable_authn => true}, JWTDisabled)),

    Enabled = mk_authn_ctx(
        "token",
        [nkey_pub()],
        jwt_conf(),
        false
    ),
    ?assertEqual(true, emqx_nats_authn:is_auth_required(#{enable_authn => true}, Enabled)).

t_ensure_and_publish_nkey_nonce(_Config) ->
    ConnInfo0 = #{clientid => <<"client-1">>},
    NoNKeyAuthn = mk_authn_ctx(undefined, [], undefined, false),
    ?assertEqual(ConnInfo0, emqx_nats_authn:ensure_nkey_nonce(ConnInfo0, NoNKeyAuthn)),

    NKeyAuthn = mk_authn_ctx(undefined, [nkey_pub()], undefined, false),
    ConnInfo1 = emqx_nats_authn:ensure_nkey_nonce(ConnInfo0, NKeyAuthn),
    Nonce = maps:get(nkey_nonce, ConnInfo1),
    ?assert(is_binary(Nonce)),
    ?assertEqual(24, byte_size(Nonce)),
    ?assertEqual(ConnInfo1, emqx_nats_authn:ensure_nkey_nonce(ConnInfo1, NKeyAuthn)),

    MsgContent = #{auth_required => true},
    ?assertEqual(MsgContent, emqx_nats_authn:maybe_add_nkey_nonce(MsgContent, ConnInfo0)),
    MsgWithNonce = emqx_nats_authn:maybe_add_nkey_nonce(MsgContent, ConnInfo1),
    ?assertEqual(Nonce, maps:get(nonce, MsgWithNonce)),

    JWTAuthn = mk_authn_ctx(undefined, [], jwt_conf(), false),
    ConnInfo2 = emqx_nats_authn:ensure_nkey_nonce(ConnInfo0, JWTAuthn),
    ?assert(is_binary(maps:get(nkey_nonce, ConnInfo2))).

t_authenticate_token_plain_success(_Config) ->
    Authn = mk_authn_ctx(
        <<"nats-token">>,
        [],
        jwt_conf(),
        false
    ),
    ConnParams = #{
        <<"auth_token">> => <<"nats-token">>,
        <<"jwt">> => <<"not-used">>
    },
    ClientInfo = #{clientid => <<"c1">>, username => <<"spoofed-user">>},
    {ok, Result} = emqx_nats_authn:authenticate(ConnParams, #{}, ClientInfo, Authn),
    ?assertEqual(token, maps:get(auth_method, Result)),
    ?assertEqual(<<"token">>, maps:get(username, Result)),
    ?assertEqual(plain, maps:get(token_type, Result)),
    ?assertEqual(undefined, maps:get(auth_expire_at, Result)).

t_authenticate_token_bcrypt_success(_Config) ->
    Authn = mk_authn_ctx(token_bcrypt_hash(), [], undefined, false),
    ConnParams = #{<<"auth_token">> => token_plain()},
    ClientInfo = #{clientid => <<"c1">>},
    {ok, Result} = emqx_nats_authn:authenticate(ConnParams, #{}, ClientInfo, Authn),
    ?assertEqual(token, maps:get(auth_method, Result)),
    ?assertEqual(bcrypt, maps:get(token_type, Result)).

t_authenticate_token_priority_over_jwt(_Config) ->
    Authn = mk_authn_ctx(
        token_plain(),
        [],
        jwt_conf(),
        false
    ),
    JWT = build_test_jwt(#{}),
    ConnParams = #{
        <<"auth_token">> => <<"bad-token">>,
        <<"jwt">> => JWT
    },
    ?assertEqual(
        {error, {token, invalid_token}},
        emqx_nats_authn:authenticate(ConnParams, #{}, #{}, Authn)
    ).

t_authenticate_token_fallback_to_jwt(_Config) ->
    Authn = mk_authn_ctx(
        token_plain(),
        [],
        jwt_conf(),
        false
    ),
    Nonce = <<"nonce-token-jwt-fallback">>,
    JWT = build_test_jwt(#{}),
    ConnParams = #{
        <<"jwt">> => JWT,
        <<"sig">> => nkey_sig(Nonce)
    },
    {ok, Result} = emqx_nats_authn:authenticate(
        ConnParams,
        #{nkey_nonce => Nonce},
        #{username => undefined},
        Authn
    ),
    ?assertEqual(jwt, maps:get(auth_method, Result)),
    ?assertEqual(nkey_pub(), maps:get(username, Result)).

t_authenticate_order_from_internal_authn(_Config) ->
    Nonce = <<"nonce-order-authn">>,
    JWT = build_test_jwt(#{}),
    ConnParams = #{
        <<"auth_token">> => <<"bad-token">>,
        <<"jwt">> => JWT,
        <<"sig">> => nkey_sig(Nonce)
    },
    TokenFirst = emqx_nats_authn:build_authn_ctx(
        [token_method(token_plain()), jwt_method(jwt_conf())],
        false
    ),
    ?assertEqual(
        {error, {token, invalid_token}},
        emqx_nats_authn:authenticate(ConnParams, #{nkey_nonce => Nonce}, #{}, TokenFirst)
    ),
    JWTFirst = emqx_nats_authn:build_authn_ctx(
        [jwt_method(jwt_conf()), token_method(token_plain())],
        false
    ),
    {ok, Result} = emqx_nats_authn:authenticate(ConnParams, #{nkey_nonce => Nonce}, #{}, JWTFirst),
    ?assertEqual(jwt, maps:get(auth_method, Result)),
    ?assertEqual(nkey_pub(), maps:get(username, Result)).

t_authenticate_token_priority_over_nkey(_Config) ->
    Authn = mk_authn_ctx(token_plain(), [nkey_pub()], undefined, false),
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
    Authn = mk_authn_ctx(token_plain(), [nkey_pub()], undefined, false),
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
    Authn = mk_authn_ctx(undefined, [nkey_pub()], jwt_conf(), false),
    Nonce = <<"nonce-1">>,
    ConnParams = #{
        <<"nkey">> => nkey_pub(),
        <<"sig">> => nkey_sig(Nonce),
        <<"jwt">> => <<"not-used">>
    },
    {ok, Result} = emqx_nats_authn:authenticate(
        ConnParams,
        #{nkey_nonce => Nonce},
        #{username => <<"spoofed-user">>},
        Authn
    ),
    ?assertEqual(nkey, maps:get(auth_method, Result)),
    ?assertEqual(nkey_pub(), maps:get(username, Result)),
    ?assertEqual(nkey_pub(), maps:get(nkey, Result)),
    ?assertEqual(undefined, maps:get(auth_expire_at, Result)).

t_authenticate_nkey_priority_over_jwt(_Config) ->
    Authn = mk_authn_ctx(undefined, [nkey_pub()], jwt_conf(), false),
    Nonce = <<"nonce-2">>,
    JWT = build_test_jwt(#{}),
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
    Authn = mk_authn_ctx(undefined, [nkey_pub()], jwt_conf(), false),
    JWT = build_test_jwt(#{}),
    ConnInfo = #{nkey_nonce => <<"nonce-3">>},

    {ok, Result} = emqx_nats_authn:authenticate(
        #{
            <<"jwt">> => JWT,
            <<"sig">> => nkey_sig(<<"nonce-3">>)
        },
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
    ExpiredJWT = build_test_jwt(#{<<"exp">> => now_seconds() - 10}),
    FutureNbfJWT = build_test_jwt(#{<<"nbf">> => now_seconds() + 3600}),
    Authn = mk_authn_ctx(undefined, [], jwt_conf(), false),

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

t_authenticate_jwt_signature_and_trust_chain_validation(_Config) ->
    Authn = mk_authn_ctx(undefined, [], jwt_conf(), false),
    ValidJWT = build_test_jwt(#{}),
    BadSigJWT = mutate_jwt_signature(ValidJWT),
    NoneAlgJWT = build_none_jwt(#{}),

    ?assertEqual(
        {error, {jwt, invalid_jwt_signature}},
        emqx_nats_authn:authenticate(#{<<"jwt">> => BadSigJWT}, #{}, #{}, Authn)
    ),
    ?assertEqual(
        {error, {jwt, invalid_jwt_alg}},
        emqx_nats_authn:authenticate(#{<<"jwt">> => NoneAlgJWT}, #{}, #{}, Authn)
    ),

    MissingAccountAuthn = mk_authn_ctx(
        undefined,
        [],
        jwt_conf(#{resolver => #{type => memory, resolver_preload => []}}),
        false
    ),
    ?assertEqual(
        {error, {jwt, jwt_account_not_found}},
        emqx_nats_authn:authenticate(#{<<"jwt">> => ValidJWT}, #{}, #{}, MissingAccountAuthn)
    ),

    UntrustedOperatorAuthn = mk_authn_ctx(
        undefined,
        [],
        jwt_conf(#{trusted_operators => [<<"OP_TEST">>]}),
        false
    ),
    ?assertEqual(
        {error, {jwt, jwt_untrusted_operator}},
        emqx_nats_authn:authenticate(#{<<"jwt">> => ValidJWT}, #{}, #{}, UntrustedOperatorAuthn)
    ).

t_authenticate_jwt_claim_projection(_Config) ->
    Exp = now_seconds() + 120,
    Nonce = <<"nonce-jwt-claim-projection">>,
    Claims = #{
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
    Authn = mk_authn_ctx(undefined, [], jwt_conf(), false),
    {ok, Result} = emqx_nats_authn:authenticate(
        #{
            <<"jwt">> => JWT,
            <<"sig">> => nkey_sig(Nonce)
        },
        #{nkey_nonce => Nonce},
        #{username => undefined},
        Authn
    ),

    ?assertEqual(jwt, maps:get(auth_method, Result)),
    ?assertEqual(nkey_pub(), maps:get(username, Result)),
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

t_authenticate_jwt_claim_projection_prefers_nats_permissions(_Config) ->
    Nonce = <<"nonce-jwt-perm">>,
    Claims = #{
        <<"nats">> => #{
            <<"pub">> => #{
                <<"allow">> => [<<"nats.orders.*">>],
                <<"deny">> => [<<"nats.orders.secret">>]
            },
            <<"sub">> => #{
                <<"allow">> => [<<"nats.events.>">>],
                <<"deny">> => [<<"nats.events.internal">>]
            },
            <<"type">> => <<"user">>,
            <<"version">> => 2
        },
        <<"permissions">> => #{
            <<"pub">> => #{
                <<"allow">> => [<<"legacy.orders.*">>],
                <<"deny">> => [<<"legacy.orders.secret">>]
            },
            <<"sub">> => #{
                <<"allow">> => [<<"legacy.events.>">>],
                <<"deny">> => [<<"legacy.events.internal">>]
            }
        }
    },
    JWT = build_test_jwt(Claims),
    Authn = mk_authn_ctx(undefined, [], jwt_conf(), false),
    {ok, Result} = emqx_nats_authn:authenticate(
        #{
            <<"jwt">> => JWT,
            <<"sig">> => nkey_sig(Nonce)
        },
        #{nkey_nonce => Nonce},
        #{username => undefined},
        Authn
    ),
    ?assertEqual(
        #{
            publish =>
                #{
                    allow => [<<"nats.orders.*">>],
                    deny => [<<"nats.orders.secret">>]
                },
            subscribe =>
                #{
                    allow => [<<"nats.events.>">>],
                    deny => [<<"nats.events.internal">>]
                }
        },
        maps:get(jwt_permissions, Result)
    ).

t_authenticate_jwt_missing_token_behavior(_Config) ->
    JWTOnlyAuthn = mk_authn_ctx(undefined, [], jwt_conf(), false),
    ?assertEqual(
        {error, {jwt, jwt_required}},
        emqx_nats_authn:authenticate(#{}, #{}, #{}, JWTOnlyAuthn)
    ),

    JWTWithGatewayAuthn = mk_authn_ctx(undefined, [], jwt_conf(), true),
    ?assertEqual(
        {continue, #{}},
        emqx_nats_authn:authenticate(#{}, #{}, #{}, JWTWithGatewayAuthn)
    ).

t_authenticate_jwt_nonce_signature_validation(_Config) ->
    Authn = mk_authn_ctx(undefined, [], jwt_conf(), false),
    Nonce = <<"nonce-jwt-proof">>,
    JWT = build_test_jwt(#{}),
    ?assertEqual(
        {error, {jwt, jwt_sig_required}},
        emqx_nats_authn:authenticate(#{<<"jwt">> => JWT}, #{nkey_nonce => Nonce}, #{}, Authn)
    ),
    ?assertEqual(
        {error, {jwt, nkey_nonce_unavailable}},
        emqx_nats_authn:authenticate(
            #{
                <<"jwt">> => JWT,
                <<"sig">> => nkey_sig(Nonce)
            },
            #{},
            #{},
            Authn
        )
    ),
    ?assertEqual(
        {error, {jwt, jwt_nkey_mismatch}},
        emqx_nats_authn:authenticate(
            #{
                <<"jwt">> => JWT,
                <<"nkey">> => other_user_nkey(),
                <<"sig">> => nkey_sig(Nonce)
            },
            #{nkey_nonce => Nonce},
            #{},
            Authn
        )
    ).

t_authenticate_disabled_jwt_and_empty_authn(_Config) ->
    JWTDisabledAuthn = mk_authn_ctx(
        undefined,
        [],
        jwt_conf(#{trusted_operators => []}),
        false
    ),
    JWT = build_test_jwt(#{}),
    Nonce = <<"nonce-jwt-disabled">>,
    ?assertEqual(
        {error, {jwt, jwt_required}},
        emqx_nats_authn:authenticate(#{}, #{}, #{}, JWTDisabledAuthn)
    ),
    ?assertEqual(
        {error, {jwt, jwt_trusted_operators_required}},
        emqx_nats_authn:authenticate(
            #{
                <<"jwt">> => JWT,
                <<"sig">> => nkey_sig(Nonce)
            },
            #{nkey_nonce => Nonce},
            #{},
            JWTDisabledAuthn
        )
    ),

    EmptyAuthn = mk_authn_ctx(undefined, [], undefined, false),
    ?assertEqual(
        {continue, #{}},
        emqx_nats_authn:authenticate(#{}, #{}, #{}, EmptyAuthn)
    ).

t_authenticate_required_errors_and_invalid_nkey(_Config) ->
    TokenOnlyAuthn = mk_authn_ctx(token_plain(), [], undefined, false),
    ?assertEqual(
        {error, {token, token_required}},
        emqx_nats_authn:authenticate(#{}, #{}, #{}, TokenOnlyAuthn)
    ),

    NKeyOnlyAuthn = mk_authn_ctx(undefined, [nkey_pub()], undefined, false),
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
    Authn = mk_authn_ctx(undefined, [], jwt_conf(), false),
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

    DisabledTokenAuthn = emqx_nats_authn:build_authn_ctx([#{type => token}], false),
    ?assertEqual(
        {continue, #{}},
        emqx_nats_authn:authenticate(#{}, #{}, #{}, DisabledTokenAuthn)
    ),
    DisabledNKeyAuthn = emqx_nats_authn:build_authn_ctx([#{type => nkey, nkeys => []}], false),
    ?assertEqual(
        {continue, #{}},
        emqx_nats_authn:authenticate(#{}, #{}, #{}, DisabledNKeyAuthn)
    ),
    DisabledJWTAuthn = emqx_nats_authn:build_authn_ctx([#{type => jwt, enable => false}], false),
    ?assertEqual(
        {continue, #{}},
        emqx_nats_authn:authenticate(#{}, #{}, #{}, DisabledJWTAuthn)
    ),

    ?assertEqual({error, invalid_jwt_account}, emqx_nats_authn:decode_jwt_account(<<"bad-jwt">>)),
    ?assertEqual(
        {error, invalid_jwt_account},
        emqx_nats_authn:verify_jwt_account_token_alg(#{header => #{alg => <<"none">>}})
    ),
    ?assertEqual(
        {error, invalid_jwt_account},
        emqx_nats_authn:verify_jwt_account_subject(#{<<"sub">> => nkey_pub()}, other_user_nkey())
    ),
    ?assertEqual(
        {error, invalid_jwt_account},
        emqx_nats_authn:verify_jwt_account_subject(#{}, nkey_pub())
    ),
    ?assertEqual(
        {error, invalid_jwt_account},
        emqx_nats_authn:jwt_claim_account_issuer(#{})
    ),

    ?assertEqual(
        {error, invalid_jwt_signature},
        emqx_nats_authn:verify_jwt_token_signature(
            #{
                signing_input => <<"x">>,
                signature => bad_signature
            },
            account_nkey()
        )
    ),
    ?assertEqual(
        {error, invalid_jwt_issuer},
        emqx_nats_authn:verify_jwt_token_signature(
            #{
                signing_input => <<"x">>,
                signature => <<"x">>
            },
            <<"BAD_NKEY">>
        )
    ),

    ?assertEqual(
        ok,
        emqx_nats_authn:verify_jwt_user_issuer_allowed(
            account_nkey(),
            nkey_pub(),
            #{
                <<"nats">> => #{
                    <<"signing_keys">> => [account_nkey()]
                }
            }
        )
    ),
    ?assertEqual(
        {error, jwt_untrusted_signing_key},
        emqx_nats_authn:verify_jwt_user_issuer_allowed(
            account_nkey(),
            nkey_pub(),
            #{
                <<"nats">> => #{
                    <<"signing_keys">> => []
                }
            }
        )
    ),

    ?assertEqual({error, invalid_jwt_issuer}, emqx_nats_authn:jwt_claim_issuer(#{})),
    ?assertEqual(
        account_nkey(),
        emqx_nats_authn:jwt_claim_issuer_account(
            #{
                <<"issuer_account">> => account_nkey()
            },
            nkey_pub()
        )
    ),
    ?assertEqual({error, invalid_jwt_subject}, emqx_nats_authn:jwt_claim_sub(#{})),
    ?assertMatch(
        {error, _},
        emqx_nats_authn:verify_jwt_connect_nkey(nkey_pub(), <<"BAD_NKEY">>)
    ),

    AccountsFromTopLevel = emqx_nats_authn:jwt_resolver_accounts(#{
        resolver_preload => [
            #{
                pubkey => account_nkey(),
                jwt => <<"jwt-account">>
            }
        ]
    }),
    ?assertEqual(<<"jwt-account">>, maps:get(account_nkey(), AccountsFromTopLevel)),
    ?assertEqual(#{}, emqx_nats_authn:resolver_preload_entries([#{}], #{})),
    ?assertEqual(
        #{foo => bar},
        emqx_nats_authn:resolver_preload_entries(invalid_preload, #{foo => bar})
    ),
    ?assertEqual(
        error,
        emqx_nats_authn:normalize_resolver_preload_entry(
            #{
                jwt => <<"jwt-account">>
            }
        )
    ),
    ?assertEqual(
        error,
        emqx_nats_authn:normalize_resolver_preload_entry(
            #{
                pubkey => account_nkey()
            }
        )
    ),

    ?assertEqual(undefined, emqx_nats_authn:normalize_jwt_config(#{enable => false})),
    ?assertEqual(undefined, emqx_nats_authn:normalize_jwt_config(#{<<"enable">> => false})),
    ?assertEqual(#{enable => true}, emqx_nats_authn:normalize_jwt_config(#{enable => true})),
    ?assertEqual([], emqx_nats_authn:normalize_authn_methods(not_a_list)),
    ?assertEqual(false, emqx_nats_authn:normalize_authn_method(#{type => unsupported})),
    ?assertEqual(token, emqx_nats_authn:normalize_method_type(<<"token">>)),
    ?assertEqual(nkey, emqx_nats_authn:normalize_method_type(<<"nkey">>)),
    ?assertEqual(jwt, emqx_nats_authn:normalize_method_type(<<"jwt">>)),
    ?assertEqual(undefined, emqx_nats_authn:normalize_method_type(invalid)),
    ?assertEqual([], emqx_nats_authn:normalize_nkeys(<<"not-a-list">>)),
    ?assertEqual([], emqx_nats_authn:normalize_nkey_list([<<>>])),
    ?assertEqual([], emqx_nats_authn:normalize_nkey_list(<<"not-a-list">>)),
    ?assertEqual(undefined, emqx_nats_authn:normalize_token(<<>>)),
    ?assertEqual(#{}, emqx_nats_authn:normalize_map(not_a_map)),

    ?assertEqual(
        [<<"orders.*">>],
        emqx_nats_authn:normalize_subject_list([<<"orders.*">>, <<>>])
    ),
    ?assertEqual([], emqx_nats_authn:normalize_subject_list(<<"not-a-list">>)),

    ?assertEqual(ok, emqx_nats_authn:verify_jwt_exp(#{<<"exp">> => 10.9}, 9)),
    ?assertEqual(ok, emqx_nats_authn:verify_jwt_nbf(#{<<"nbf">> => 10}, 10)),
    ?assertEqual(ok, emqx_nats_authn:verify_jwt_nbf(#{<<"nbf">> => 10.9}, 10)),
    ?assertEqual(
        erlang:convert_time_unit(10, second, millisecond),
        emqx_nats_authn:jwt_claim_expire_at(#{<<"exp">> => 10.9})
    ),
    ?assertEqual(
        {ok, nkey_pub()},
        emqx_nats_authn:resolve_jwt_connect_nkey(nkey_pub(), undefined)
    ),
    ?assertMatch(
        {error, _},
        emqx_nats_authn:resolve_jwt_connect_nkey(nkey_pub(), <<"BAD_NKEY">>)
    ).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

mk_authn_ctx(Token, NKeys, JWT, GatewayAuthEnabled) ->
    Methods =
        maybe_token_method(Token) ++
            maybe_nkey_method(NKeys) ++
            maybe_jwt_method(JWT),
    emqx_nats_authn:build_authn_ctx(Methods, GatewayAuthEnabled).

maybe_token_method(Token0) ->
    case normalize_test_token(Token0) of
        undefined ->
            [];
        Token ->
            [token_method(Token)]
    end.

maybe_nkey_method(NKeys) when is_list(NKeys), NKeys =/= [] ->
    [#{type => nkey, nkeys => NKeys}];
maybe_nkey_method(_NKeys) ->
    [].

maybe_jwt_method(undefined) ->
    [];
maybe_jwt_method(#{enable := false}) ->
    [];
maybe_jwt_method(#{<<"enable">> := false}) ->
    [];
maybe_jwt_method(JWTConf) when is_map(JWTConf) ->
    [jwt_method(JWTConf)];
maybe_jwt_method(_JWTConf) ->
    [].

token_method(Token) ->
    #{type => token, token => normalize_test_token(Token)}.

jwt_method(JWTConf) ->
    maps:merge(#{type => jwt}, JWTConf).

normalize_test_token(undefined) ->
    undefined;
normalize_test_token(<<>>) ->
    undefined;
normalize_test_token(Token) when is_binary(Token) ->
    Token;
normalize_test_token(Token) ->
    emqx_utils_conv:bin(Token).

jwt_conf() ->
    jwt_conf(#{}).

jwt_conf(Overrides) ->
    Fixture = jwt_fixture(),
    maps:merge(
        #{
            trusted_operators => [maps:get(operator_nkey, Fixture)],
            resolver => #{
                type => memory,
                resolver_preload => [
                    #{
                        pubkey => maps:get(account_nkey, Fixture),
                        jwt => maps:get(account_jwt, Fixture)
                    }
                ]
            }
        },
        Overrides
    ).

build_test_jwt(Claims) ->
    Fixture = jwt_fixture(),
    DefaultClaims = #{
        <<"iss">> => maps:get(account_nkey, Fixture),
        <<"sub">> => nkey_pub(),
        <<"iat">> => now_seconds(),
        <<"nats">> => #{
            <<"type">> => <<"user">>,
            <<"version">> => 2
        }
    },
    sign_jwt(maps:merge(DefaultClaims, Claims), jwt_account_priv()).

build_none_jwt(Claims) ->
    Fixture = jwt_fixture(),
    DefaultClaims = #{
        <<"iss">> => maps:get(account_nkey, Fixture),
        <<"sub">> => nkey_pub()
    },
    Header = base64url_encode(emqx_utils_json:encode(#{alg => <<"none">>, typ => <<"JWT">>})),
    Payload = base64url_encode(emqx_utils_json:encode(maps:merge(DefaultClaims, Claims))),
    <<Header/binary, ".", Payload/binary, ".sig">>.

sign_jwt(Claims, PrivateKey) ->
    Header = base64url_encode(
        emqx_utils_json:encode(#{alg => <<"ed25519-nkey">>, typ => <<"JWT">>})
    ),
    Payload = base64url_encode(emqx_utils_json:encode(Claims)),
    SigningInput = <<Header/binary, ".", Payload/binary>>,
    Sig = crypto:sign(eddsa, none, SigningInput, [PrivateKey, ed25519]),
    Signature = base64url_encode(Sig),
    <<SigningInput/binary, ".", Signature/binary>>.

base64url_encode(Bin) ->
    base64:encode(Bin, #{mode => urlsafe, padding => false}).

base64url_decode(Bin) ->
    base64:decode(Bin, #{mode => urlsafe, padding => false}).

now_seconds() ->
    erlang:system_time(second).

jwt_fixture() ->
    OperatorNKey = operator_nkey(),
    AccountNKey = account_nkey(),
    #{
        operator_nkey => OperatorNKey,
        account_nkey => AccountNKey,
        account_jwt => build_account_jwt(OperatorNKey, AccountNKey)
    }.

build_account_jwt(OperatorNKey, AccountNKey) ->
    Claims = #{
        <<"iss">> => OperatorNKey,
        <<"sub">> => AccountNKey,
        <<"iat">> => now_seconds(),
        <<"nats">> => #{
            <<"type">> => <<"account">>,
            <<"version">> => 2
        }
    },
    sign_jwt(Claims, jwt_operator_priv()).

operator_nkey() ->
    public_nkey(?NKEY_OPERATOR_PREFIX, jwt_operator_priv()).

account_nkey() ->
    public_nkey(?NKEY_ACCOUNT_PREFIX, jwt_account_priv()).

other_user_nkey() ->
    public_nkey(?NKEY_USER_PREFIX, jwt_account_priv()).

public_nkey(Prefix, PrivateKey) ->
    {PubKey, _} = crypto:generate_key(eddsa, ed25519, PrivateKey),
    encode_nkey(Prefix, PubKey).

encode_nkey(Prefix, PubKey) ->
    Payload = <<Prefix:8, PubKey/binary>>,
    Crc = emqx_nats_nkey:crc16_xmodem(Payload),
    emqx_nats_nkey:base32_encode(<<Payload/binary, Crc:16/little-unsigned>>).

jwt_operator_priv() ->
    <<1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
        27, 28, 29, 30, 31, 32>>.

jwt_account_priv() ->
    <<33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55,
        56, 57, 58, 59, 60, 61, 62, 63, 64>>.

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

mutate_jwt_signature(JWT) ->
    [Header, Payload, Signature] = binary:split(JWT, <<".">>, [global]),
    SignatureBin = base64url_decode(Signature),
    <<First:8, Rest/binary>> = SignatureBin,
    BadSignature = base64url_encode(<<(First bxor 1), Rest/binary>>),
    <<Header/binary, ".", Payload/binary, ".", BadSignature/binary>>.

replace_first_char(<<_Old, Rest/binary>>, New) ->
    <<New, Rest/binary>>.
