%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_scram_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

t_rfc7677_sha256_vector(_) ->
    Username = <<"user">>,
    Password = <<"pencil">>,
    ClientNonce = <<"rOprNGfwEbeRWgbNEkqO">>,
    ServerNonce = <<"%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0">>,
    Salt = base64:decode(<<"W22ZaJ0SNY7soEsUEjb6gQ==">>),
    Iterations = 4096,
    ClientProof = emqx_dashboard_scram:client_proof(
        Username,
        ClientNonce,
        ServerNonce,
        Salt,
        Iterations,
        Password
    ),
    ?assertEqual(
        <<"dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=">>,
        base64:encode(ClientProof)
    ),
    %% The RFC server-final verifier is independently specified as a literal.
    SaltedPassword = crypto:pbkdf2_hmac(sha256, Password, Salt, Iterations, 32),
    PwdHash = iolist_to_binary([
        <<"$1$4096$">>,
        base64:encode(Salt, #{padding => false}),
        <<"$">>,
        base64:encode(SaltedPassword, #{padding => false})
    ]),
    {ok, Verifier} = emqx_dashboard_scram:from_pwdhash(PwdHash),
    {ok, ServerSignature} = emqx_dashboard_scram:verify(
        Verifier,
        Username,
        ClientNonce,
        ServerNonce,
        ClientProof
    ),
    ?assertEqual(
        <<"6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=">>,
        base64:encode(ServerSignature)
    ).

t_username_escaping_vector(_) ->
    Username = <<"user,name=example">>,
    Password = <<"pencil">>,
    ClientNonce = <<"rOprNGfwEbeRWgbNEkqO">>,
    ServerNonce = <<"%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0">>,
    Salt = base64:decode(<<"W22ZaJ0SNY7soEsUEjb6gQ==">>),
    Iterations = 4096,
    ClientProof = emqx_dashboard_scram:client_proof(
        Username,
        ClientNonce,
        ServerNonce,
        Salt,
        Iterations,
        Password
    ),
    ?assertEqual(
        <<"jA+s4YTor9rmIHiMCNwLGh3OxJzt5J7AGGIFll+Mlzk=">>,
        base64:encode(ClientProof)
    ),
    SaltedPassword = crypto:pbkdf2_hmac(sha256, Password, Salt, Iterations, 32),
    PwdHash = iolist_to_binary([
        <<"$1$4096$">>,
        base64:encode(Salt, #{padding => false}),
        <<"$">>,
        base64:encode(SaltedPassword, #{padding => false})
    ]),
    {ok, Verifier} = emqx_dashboard_scram:from_pwdhash(PwdHash),
    {ok, ServerSignature} = emqx_dashboard_scram:verify(
        Verifier,
        Username,
        ClientNonce,
        ServerNonce,
        ClientProof
    ),
    ?assertEqual(
        <<"eYccYlA2rAWZTp6yG/400lb9GthcHhtUB9ffXbzpH8o=">>,
        base64:encode(ServerSignature)
    ).

t_v1_pwdhash_roundtrip(_) ->
    Password = <<"correctP@ss1">>,
    Salt = crypto:strong_rand_bytes(16),
    SaltedPassword = crypto:pbkdf2_hmac(sha256, Password, Salt, 600000, 32),
    PwdHash = iolist_to_binary([
        <<"$1$600000$">>,
        base64:encode(Salt, #{padding => false}),
        <<"$">>,
        base64:encode(SaltedPassword, #{padding => false})
    ]),
    {ok, Verifier} = emqx_dashboard_scram:from_pwdhash(PwdHash),
    Proof = emqx_dashboard_scram:client_proof(
        <<"admin">>, <<"client">>, <<"server">>, Salt, 600000, Password
    ),
    ?assertMatch(
        {ok, _},
        emqx_dashboard_scram:verify(Verifier, <<"admin">>, <<"client">>, <<"server">>, Proof)
    ).

t_wrong_password_rejected(_) ->
    Password = <<"correctP@ss1">>,
    Salt = crypto:strong_rand_bytes(16),
    SaltedPassword = crypto:pbkdf2_hmac(sha256, Password, Salt, 4096, 32),
    PwdHash = iolist_to_binary([
        <<"$1$4096$">>,
        base64:encode(Salt, #{padding => false}),
        <<"$">>,
        base64:encode(SaltedPassword, #{padding => false})
    ]),
    {ok, Verifier} = emqx_dashboard_scram:from_pwdhash(PwdHash),
    WrongProof = emqx_dashboard_scram:client_proof(
        <<"admin">>, <<"client">>, <<"server">>, Salt, 4096, <<"wrongP@ss1">>
    ),
    ?assertEqual(
        {error, invalid_proof},
        emqx_dashboard_scram:verify(
            Verifier, <<"admin">>, <<"client">>, <<"server">>, WrongProof
        )
    ).

t_legacy_hash_rejected(_) ->
    ?assertEqual(
        {error, unsupported},
        emqx_dashboard_scram:from_pwdhash(
            <<"abcd", 0:256>>
        )
    ).

t_malformed_v1_hash_rejected(_) ->
    ?assertEqual(
        {error, malformed},
        emqx_dashboard_scram:from_pwdhash(<<"$1$600000$not-valid$not-valid">>)
    ).
