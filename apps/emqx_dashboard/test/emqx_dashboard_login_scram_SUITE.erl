%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_login_scram_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_dashboard.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(USERNAME, <<"scram_admin">>).
-define(PASSWORD, <<"scramP@ss1">>).
-define(GOOD_TOTP, <<"123456">>).

-record(login_attempts, {key, extra}).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_Case, Config) ->
    mnesia:clear_table(?ADMIN),
    mnesia:clear_table(?ADMIN_JWT),
    mnesia:clear_table(emqx_dashboard_scram_challenge),
    emqx_dashboard_login_lock:cleanup_all(),
    emqx_config:put([dashboard, password_login], both),
    {ok, _} = emqx_dashboard_admin:add_user(
        ?USERNAME, ?PASSWORD, ?ROLE_SUPERUSER, <<"SCRAM test user">>
    ),
    Config.

t_scram_login_roundtrip(_) ->
    ClientNonce = <<"clientNonce1234567890">>,
    {ok, Challenge} = emqx_dashboard_login:scram_challenge(?USERNAME, ClientNonce),
    Salt = base64:decode(maps:get(salt, Challenge)),
    Iterations = maps:get(iterations, Challenge),
    ServerNonce = maps:get(server_nonce, Challenge),
    [#?ADMIN{pwdhash = PwdHash}] = emqx_dashboard_admin:lookup_user(?USERNAME),
    {ok, _Verifier} = emqx_dashboard_scram:from_pwdhash(PwdHash),
    Proof = emqx_dashboard_scram:client_proof(
        ?USERNAME,
        ClientNonce,
        ServerNonce,
        Salt,
        Iterations,
        ?PASSWORD
    ),
    CombinedNonce = <<ClientNonce/binary, ServerNonce/binary>>,
    {ok, Result} = emqx_dashboard_login:scram_verify(
        maps:get(challenge_id, Challenge),
        CombinedNonce,
        Proof,
        ?NO_MFA_TOKEN
    ),
    ?assert(is_binary(maps:get(token, Result))),
    ?assertEqual(32, byte_size(base64:decode(maps:get(server_signature, Result)))),
    %% the SCRAM completion carries the same MFA status the password login does
    ?assertEqual(pending_voluntary, maps:get(mfa_status, Result)),
    ?assertEqual(
        {error, password_error},
        emqx_dashboard_login:scram_verify(
            maps:get(challenge_id, Challenge),
            CombinedNonce,
            Proof,
            ?NO_MFA_TOKEN
        )
    ).

t_long_client_nonce_roundtrip(_) ->
    ClientNonce = binary:copy(<<"a">>, 128),
    {ok, Challenge} = emqx_dashboard_login:scram_challenge(?USERNAME, ClientNonce),
    Salt = base64:decode(maps:get(salt, Challenge)),
    ServerNonce = maps:get(server_nonce, Challenge),
    Proof = emqx_dashboard_scram:client_proof(
        ?USERNAME,
        ClientNonce,
        ServerNonce,
        Salt,
        maps:get(iterations, Challenge),
        ?PASSWORD
    ),
    ?assertMatch(
        {ok, _},
        emqx_dashboard_login:scram_verify(
            maps:get(challenge_id, Challenge),
            <<ClientNonce/binary, ServerNonce/binary>>,
            Proof,
            ?NO_MFA_TOKEN
        )
    ).

t_scram_password_changed_invalidates_challenge(_) ->
    ClientNonce = <<"clientNonce1234567890">>,
    {ok, Challenge} = emqx_dashboard_login:scram_challenge(?USERNAME, ClientNonce),
    Salt = base64:decode(maps:get(salt, Challenge)),
    ServerNonce = maps:get(server_nonce, Challenge),
    Proof = emqx_dashboard_scram:client_proof(
        ?USERNAME,
        ClientNonce,
        ServerNonce,
        Salt,
        maps:get(iterations, Challenge),
        ?PASSWORD
    ),
    {ok, _} = emqx_dashboard_admin:change_password_trusted(?USERNAME, <<"newP@ss2">>),
    ?assertEqual(
        {error, password_error},
        emqx_dashboard_login:scram_verify(
            maps:get(challenge_id, Challenge),
            <<ClientNonce/binary, ServerNonce/binary>>,
            Proof,
            ?NO_MFA_TOKEN
        )
    ).

t_expired_scram_challenge_rejected(_) ->
    ClientNonce = <<"clientNonce1234567890">>,
    {ok, Challenge} = emqx_dashboard_login:scram_challenge(?USERNAME, ClientNonce),
    ChallengeId = maps:get(challenge_id, Challenge),
    {atomic, ok} = mria:sync_transaction(?DASHBOARD_SHARD, fun() ->
        [Stored] = mnesia:read(emqx_dashboard_scram_challenge, ChallengeId, write),
        mnesia:write(Stored#emqx_dashboard_scram_challenge{
            expires_at = erlang:system_time(millisecond) - 1
        })
    end),
    ?assertEqual(
        {error, invalid_challenge},
        emqx_dashboard_login:scram_verify(
            ChallengeId,
            <<ClientNonce/binary, (maps:get(server_nonce, Challenge))/binary>>,
            crypto:strong_rand_bytes(32),
            ?NO_MFA_TOKEN
        )
    ).

t_wrong_proof_counts_as_password_failure(_) ->
    ClientNonce = <<"clientNonce1234567890">>,
    {ok, Challenge} = emqx_dashboard_login:scram_challenge(?USERNAME, ClientNonce),
    Salt = base64:decode(maps:get(salt, Challenge)),
    ServerNonce = maps:get(server_nonce, Challenge),
    WrongProof = emqx_dashboard_scram:client_proof(
        ?USERNAME,
        ClientNonce,
        ServerNonce,
        Salt,
        maps:get(iterations, Challenge),
        <<"wrongP@ss1">>
    ),
    CombinedNonce = <<ClientNonce/binary, ServerNonce/binary>>,
    ?assertEqual(
        {error, password_error},
        emqx_dashboard_login:scram_verify(
            maps:get(challenge_id, Challenge),
            CombinedNonce,
            WrongProof,
            ?NO_MFA_TOKEN
        )
    ),
    ?assertEqual(ok, emqx_dashboard_login_lock:verify(?USERNAME)).

t_scram_challenge_limit_per_username(_) ->
    ClientNonce = <<"clientNonce1234567890">>,
    Challenges = [
        begin
            {ok, Challenge} = emqx_dashboard_login:scram_challenge(?USERNAME, ClientNonce),
            Challenge
        end
     || _ <- lists:seq(1, 16)
    ],
    ?assertEqual(
        {error, capacity},
        emqx_dashboard_login:scram_challenge(?USERNAME, ClientNonce)
    ),
    ?assertEqual(
        16,
        length(
            mnesia:dirty_match_object(
                emqx_dashboard_scram_challenge,
                #emqx_dashboard_scram_challenge{username = ?USERNAME, _ = '_'}
            )
        )
    ),
    _ = Challenges,
    First = hd(Challenges),
    Salt = base64:decode(maps:get(salt, First)),
    ServerNonce = maps:get(server_nonce, First),
    Proof = emqx_dashboard_scram:client_proof(
        ?USERNAME,
        ClientNonce,
        ServerNonce,
        Salt,
        maps:get(iterations, First),
        ?PASSWORD
    ),
    {ok, _} = emqx_dashboard_login:scram_verify(
        maps:get(challenge_id, First),
        <<ClientNonce/binary, ServerNonce/binary>>,
        Proof,
        ?NO_MFA_TOKEN
    ),
    ?assertMatch({ok, _}, emqx_dashboard_login:scram_challenge(?USERNAME, ClientNonce)).

t_scram_login_locked_after_challenge(_) ->
    ClientNonce = <<"clientNonce1234567890">>,
    {ok, Challenge} = emqx_dashboard_login:scram_challenge(?USERNAME, ClientNonce),
    Salt = base64:decode(maps:get(salt, Challenge)),
    ServerNonce = maps:get(server_nonce, Challenge),
    Proof = emqx_dashboard_scram:client_proof(
        ?USERNAME,
        ClientNonce,
        ServerNonce,
        Salt,
        maps:get(iterations, Challenge),
        ?PASSWORD
    ),
    {ok, _} = emqx_dashboard_admin:set_login_lock(
        ?USERNAME,
        erlang:system_time(second) + 60
    ),
    ?assertEqual(
        {error, login_locked},
        emqx_dashboard_login:scram_verify(
            maps:get(challenge_id, Challenge),
            <<ClientNonce/binary, ServerNonce/binary>>,
            Proof,
            ?NO_MFA_TOKEN
        )
    ).

t_http_password_login_disabled(_) ->
    emqx_config:put([dashboard, password_login], scram_only),
    ?assertMatch(
        {ok, 403, #{
            <<"code">> := <<"PASSWORD_LOGIN_DISABLED">>,
            <<"message">> :=
                <<"Plaintext password login is disabled.">>
        }},
        request_api(
            post,
            api_path(["login"]),
            no_auth_header,
            #{username => ?USERNAME, password => ?PASSWORD}
        )
    ).

t_http_scram_login_locked(_) ->
    ClientNonce = <<"clientNonce1234567890">>,
    {ok, _} = emqx_dashboard_admin:set_login_lock(
        ?USERNAME,
        erlang:system_time(second) + 60
    ),
    {ok, 200, Challenge} = request_api(
        post,
        api_path(["login", "challenge"]),
        no_auth_header,
        #{username => ?USERNAME, client_nonce => ClientNonce}
    ),
    Salt = base64:decode(maps:get(<<"salt">>, Challenge)),
    ServerNonce = maps:get(<<"server_nonce">>, Challenge),
    Proof = emqx_dashboard_scram:client_proof(
        ?USERNAME,
        ClientNonce,
        ServerNonce,
        Salt,
        maps:get(<<"iterations">>, Challenge),
        ?PASSWORD
    ),
    ?assertMatch(
        {ok, 401, #{<<"code">> := <<"LOGIN_LOCKED">>}},
        request_api(
            post,
            api_path(["login", "verify"]),
            no_auth_header,
            #{
                challenge_id => maps:get(<<"challenge_id">>, Challenge),
                combined_nonce => <<ClientNonce/binary, ServerNonce/binary>>,
                client_proof => base64:encode(Proof)
            }
        )
    ).

t_http_scram_login_storage_unavailable(_) ->
    ClientNonce = <<"clientNonce1234567890">>,
    {ok, 200, Challenge} = request_api(
        post,
        api_path(["login", "challenge"]),
        no_auth_header,
        #{username => ?USERNAME, client_nonce => ClientNonce}
    ),
    ok = meck:new(mria, [passthrough, no_history]),
    ok = meck:expect(mria, sync_transaction, fun(_, _) ->
        {timeout, {error, shard_not_ready}}
    end),
    try
        ?assertMatch(
            {ok, 503, #{<<"code">> := <<"SERVICE_UNAVAILABLE">>}},
            request_api(
                post,
                api_path(["login", "verify"]),
                no_auth_header,
                #{
                    challenge_id => maps:get(<<"challenge_id">>, Challenge),
                    combined_nonce =>
                        <<ClientNonce/binary, (maps:get(<<"server_nonce">>, Challenge))/binary>>,
                    client_proof => base64:encode(crypto:strong_rand_bytes(32))
                }
            )
        )
    after
        ok = meck:unload(mria)
    end.

t_http_scram_login_mfa(_) ->
    ok = meck:new(pot, [passthrough, no_history]),
    ok = meck:expect(pot, valid_totp, fun(Token, _) -> Token =:= ?GOOD_TOTP end),
    {ok, ok} = emqx_dashboard_admin:set_mfa_state(
        ?USERNAME,
        #{mechanism => totp, secret => <<"JBSWY3DPEHPK3PXP">>}
    ),
    try
        ClientNonce = <<"clientNonce1234567890">>,
        {ok, 200, Challenge1} = request_api(
            post,
            api_path(["login", "challenge"]),
            no_auth_header,
            #{username => ?USERNAME, client_nonce => ClientNonce}
        ),
        Proof1 = http_proof(ClientNonce, Challenge1),
        ?assertMatch(
            {ok, 401, #{<<"code">> := <<"BAD_MFA_TOKEN">>}},
            request_api(
                post,
                api_path(["login", "verify"]),
                no_auth_header,
                http_verify_body(ClientNonce, Challenge1, Proof1, #{})
            )
        ),
        {ok, 200, Challenge2} = request_api(
            post,
            api_path(["login", "challenge"]),
            no_auth_header,
            #{username => ?USERNAME, client_nonce => ClientNonce}
        ),
        Proof2 = http_proof(ClientNonce, Challenge2),
        ?assertMatch(
            {ok, 200, #{<<"token">> := _}},
            request_api(
                post,
                api_path(["login", "verify"]),
                no_auth_header,
                http_verify_body(
                    ClientNonce,
                    Challenge2,
                    Proof2,
                    #{mfa_token => ?GOOD_TOTP}
                )
            )
        )
    after
        _ = emqx_dashboard_admin:clear_mfa_state(?USERNAME),
        ok = meck:unload(pot)
    end.

t_scram_only_hides_legacy_password_migration(_) ->
    ok = emqx_config:put([dashboard, password_login], scram_only),
    {atomic, ok} = mria:sync_transaction(?DASHBOARD_SHARD, fun() ->
        [User] = mnesia:wread({?ADMIN, ?USERNAME}),
        mnesia:write(User#?ADMIN{pwdhash = <<"abcd", 0:256>>})
    end),
    try
        Reports =
            emqx_cth_log_capture:capture(error, fun() ->
                {ok, _} = emqx_dashboard_login:scram_challenge(
                    ?USERNAME, <<"clientNonce1234567890">>
                )
            end),
        {ok, 200, LegacyChallenge} =
            request_api(
                post,
                api_path(["login", "challenge"]),
                no_auth_header,
                #{username => ?USERNAME, client_nonce => <<"clientNonce1234567890">>}
            ),
        {ok, 200, UnknownChallenge} =
            request_api(
                post,
                api_path(["login", "challenge"]),
                no_auth_header,
                #{username => <<"unknown_scram_user">>, client_nonce => <<"clientNonce1234567890">>}
            ),
        ?assertEqual(
            lists:sort(maps:keys(LegacyChallenge)),
            lists:sort(maps:keys(UnknownChallenge))
        ),
        ?assertEqual(
            nomatch,
            binary:match(emqx_utils_json:encode(LegacyChallenge), <<"migration">>)
        ),
        ?assertMatch(
            {ok, 401, #{
                <<"code">> := <<"BAD_USERNAME_OR_PWD">>,
                <<"message">> := <<"Auth failed">>
            }},
            request_api(
                post,
                api_path(["login", "verify"]),
                no_auth_header,
                #{
                    challenge_id => maps:get(<<"challenge_id">>, LegacyChallenge),
                    combined_nonce => <<
                        "clientNonce1234567890",
                        (maps:get(<<"server_nonce">>, LegacyChallenge))/binary
                    >>,
                    client_proof => base64:encode(crypto:strong_rand_bytes(32))
                }
            )
        ),
        ?assert(
            lists:any(
                fun
                    (
                        #{
                            msg := "dashboard_scram_password_migration_required",
                            username := ?USERNAME,
                            migration_command := Command
                        }
                    ) ->
                        Command =:=
                            <<"emqx ctl admins passwd scram_admin <new-password>">>;
                    (_) ->
                        false
                end,
                Reports
            )
        )
    after
        ok = emqx_config:put([dashboard, password_login], both)
    end.

t_fake_challenge_does_not_record_password_failure(_) ->
    Username = <<"unknown_scram_user">>,
    ClientNonce = <<"clientNonce1234567890">>,
    {ok, Challenge} = emqx_dashboard_login:scram_challenge(Username, ClientNonce),
    CombinedNonce = <<ClientNonce/binary, (maps:get(server_nonce, Challenge))/binary>>,
    {error, password_error} = emqx_dashboard_login:scram_verify(
        maps:get(challenge_id, Challenge),
        CombinedNonce,
        crypto:strong_rand_bytes(32),
        ?NO_MFA_TOKEN
    ),
    ?assertEqual(
        [],
        mnesia:dirty_read(emqx_dashboard_scram_challenge, maps:get(challenge_id, Challenge))
    ),
    ?assertEqual(
        [],
        mnesia:dirty_select(login_attempts, [
            {
                #login_attempts{key = {Username, '_'}, extra = '_'},
                [],
                ['$_']
            }
        ])
    ).

t_fake_challenge_is_stable_per_username(_) ->
    ClientNonce = <<"clientNonce1234567890">>,
    {ok, Challenge1} = emqx_dashboard_login:scram_challenge(
        <<"unknown_scram_user">>, ClientNonce
    ),
    {ok, Challenge2} = emqx_dashboard_login:scram_challenge(
        <<"unknown_scram_user">>, ClientNonce
    ),
    {ok, OtherChallenge} = emqx_dashboard_login:scram_challenge(
        <<"another_unknown_scram_user">>, ClientNonce
    ),
    ?assertEqual(maps:get(salt, Challenge1), maps:get(salt, Challenge2)),
    ?assertNotEqual(maps:get(salt, Challenge1), maps:get(salt, OtherChallenge)).

api_path(Parts) ->
    binary_to_list(iolist_to_binary(emqx_mgmt_api_test_util:api_path(Parts))).

request_api(Method, Url, Auth, Body) ->
    case emqx_common_test_http:request_api(Method, Url, [], Auth, Body) of
        {ok, Code, ResponseBody} ->
            {ok, Code, emqx_utils_json:decode(ResponseBody)};
        Error ->
            Error
    end.

http_proof(ClientNonce, Challenge) ->
    Salt = base64:decode(maps:get(<<"salt">>, Challenge)),
    ServerNonce = maps:get(<<"server_nonce">>, Challenge),
    emqx_dashboard_scram:client_proof(
        ?USERNAME,
        ClientNonce,
        ServerNonce,
        Salt,
        maps:get(<<"iterations">>, Challenge),
        ?PASSWORD
    ).

http_verify_body(ClientNonce, Challenge, Proof, Extra) ->
    ServerNonce = maps:get(<<"server_nonce">>, Challenge),
    maps:merge(
        #{
            challenge_id => maps:get(<<"challenge_id">>, Challenge),
            combined_nonce => <<ClientNonce/binary, ServerNonce/binary>>,
            client_proof => base64:encode(Proof)
        },
        Extra
    ).
