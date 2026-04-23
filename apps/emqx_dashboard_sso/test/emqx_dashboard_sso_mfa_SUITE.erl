%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_sso_mfa_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("../../emqx_dashboard/include/emqx_dashboard.hrl").

-define(HOST, "http://127.0.0.1:18083").
-define(BASE_PATH, "/api/v5").
-define(GOOD_TOTP, <<"123456">>).

-define(SSO_BACKEND, ldap).
-define(SSO_USER, <<"sso_testuser">>).
-define(SSO_USER2, <<"sso_testuser2">>).

-define(EE_ONLY(EXPR, NON_EE),
    case emqx_release:edition() of
        ee -> EXPR;
        _ -> NON_EE
    end
).

%%--------------------------------------------------------------------
%% CT callbacks
%%--------------------------------------------------------------------

all() ->
    ?EE_ONLY(emqx_common_test_helpers:all(?MODULE), []).

init_per_suite(Config) ->
    ?EE_ONLY(
        begin
            Apps = emqx_cth_suite:start(
                [
                    emqx,
                    emqx_conf,
                    emqx_management,
                    emqx_mgmt_api_test_util:emqx_dashboard(),
                    emqx_dashboard_sso
                ],
                #{work_dir => emqx_cth_suite:work_dir(Config)}
            ),
            ok = init_users(),
            [{apps, Apps} | Config]
        end,
        Config
    ).

end_per_suite(Config) ->
    ?EE_ONLY(
        begin
            mnesia:clear_table(?ADMIN),
            mnesia:clear_table(?ADMIN_JWT),
            emqx_cth_suite:stop(?config(apps, Config))
        end,
        ok
    ).

init_per_testcase(Case, Config) ->
    ?MODULE:Case({init, Config}).

end_per_testcase(Case, Config) ->
    ?MODULE:Case({'end', Config}).

init_users() ->
    mnesia:clear_table(?ADMIN),
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"admin1">>, <<"admin1pass">>, ?ROLE_SUPERUSER, "admin"
    ),
    %% Create SSO users directly via add_sso_user
    {ok, _} = emqx_dashboard_admin:add_sso_user(?SSO_BACKEND, ?SSO_USER, ?ROLE_VIEWER, <<>>),
    {ok, _} = emqx_dashboard_admin:add_sso_user(?SSO_BACKEND, ?SSO_USER2, ?ROLE_VIEWER, <<>>),
    ok.

%%====================================================================
%% Unit tests for emqx_dashboard_sso_mfa:get_force_mfa/1
%%====================================================================

t_get_force_mfa_not_configured({init, Config}) ->
    Config;
t_get_force_mfa_not_configured({'end', _Config}) ->
    ok;
t_get_force_mfa_not_configured(_Config) ->
    %% When no SSO backend config exists, get_force_mfa returns false
    ?assertEqual(false, emqx_dashboard_sso_mfa:get_force_mfa(nonexistent_backend)),
    ok.

t_get_force_mfa_enabled({init, Config}) ->
    mock_force_mfa(?SSO_BACKEND, true),
    Config;
t_get_force_mfa_enabled({'end', _Config}) ->
    clear_force_mfa(?SSO_BACKEND),
    ok;
t_get_force_mfa_enabled(_Config) ->
    ?assertEqual(true, emqx_dashboard_sso_mfa:get_force_mfa(?SSO_BACKEND)),
    ok.

t_get_force_mfa_disabled({init, Config}) ->
    mock_force_mfa(?SSO_BACKEND, false),
    Config;
t_get_force_mfa_disabled({'end', _Config}) ->
    clear_force_mfa(?SSO_BACKEND),
    ok;
t_get_force_mfa_disabled(_Config) ->
    ?assertEqual(false, emqx_dashboard_sso_mfa:get_force_mfa(?SSO_BACKEND)),
    ok.

%%====================================================================
%% Unit tests for emqx_dashboard_mfa temp token functions (Mnesia-backed)
%%====================================================================

t_create_and_verify_setup_token({init, Config}) ->
    %% Create a temporary user for this test
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"test_setup_user">>, <<"TestPass1!">>, ?ROLE_VIEWER, <<>>
    ),
    Config;
t_create_and_verify_setup_token({'end', _Config}) ->
    _ = emqx_dashboard_admin:remove_user(<<"test_setup_user">>),
    ok;
t_create_and_verify_setup_token(_Config) ->
    Username = <<"test_setup_user">>,
    Secret = <<"JBSWY3DPEHPK3PXP">>,
    Token = emqx_dashboard_mfa:create_setup_token(Username, Secret),
    ?assert(is_binary(Token)),
    ?assert(byte_size(Token) > 0),
    Result = emqx_dashboard_mfa:verify_temp_token(Username, Token),
    ?assertMatch({ok, Username, {setup, Secret}}, Result),
    ok.

t_create_and_verify_verify_token({init, Config}) ->
    %% Create a MFA record first since create_verify_token updates existing record
    SsoUsername = ?SSO_USERNAME(?SSO_BACKEND, ?SSO_USER),
    MfaState = #{mechanism => totp, secret => <<"VERIFYSECRET">>, first_verify_ts => 1000},
    {ok, ok} = emqx_dashboard_admin:set_mfa_state(SsoUsername, MfaState),
    Config;
t_create_and_verify_verify_token({'end', _Config}) ->
    _ = emqx_dashboard_admin:clear_mfa_state(?SSO_USERNAME(?SSO_BACKEND, ?SSO_USER)),
    ok;
t_create_and_verify_verify_token(_Config) ->
    SsoUsername = ?SSO_USERNAME(?SSO_BACKEND, ?SSO_USER),
    Token = emqx_dashboard_mfa:create_verify_token(SsoUsername),
    ?assert(is_binary(Token)),
    ?assert(byte_size(Token) > 0),
    Result = emqx_dashboard_mfa:verify_temp_token(SsoUsername, Token),
    ?assertMatch({ok, SsoUsername, verify}, Result),
    ok.

t_verify_token_expired({init, Config}) ->
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"test_expired_user">>, <<"TestPass1!">>, ?ROLE_VIEWER, <<>>
    ),
    Config;
t_verify_token_expired({'end', _Config}) ->
    _ = emqx_dashboard_admin:remove_user(<<"test_expired_user">>),
    ok;
t_verify_token_expired(_Config) ->
    Username = <<"test_expired_user">>,
    Token = emqx_dashboard_mfa:create_setup_token(Username, <<"SECRET">>),
    ?assert(is_binary(Token)),
    %% Manually update the pending record with an expired timestamp
    ExpiredTs = erlang:system_time(second) - 301,
    {ok, ok} = emqx_dashboard_admin:set_mfa_pending(Username, #{
        type => setup,
        token => Token,
        secret => <<"SECRET">>,
        timestamp => ExpiredTs
    }),
    Result = emqx_dashboard_mfa:verify_temp_token(Username, Token),
    ?assertMatch({error, token_expired}, Result),
    ok.

t_verify_token_invalid({init, Config}) ->
    Config;
t_verify_token_invalid({'end', _Config}) ->
    ok;
t_verify_token_invalid(_Config) ->
    %% Random bytes without HMAC signature → rejected
    DummyUser = <<"nonexistent_user">>,
    RandomToken = binary:encode_hex(crypto:strong_rand_bytes(32)),
    ?assertMatch(
        {error, invalid_token}, emqx_dashboard_mfa:verify_temp_token(DummyUser, RandomToken)
    ),
    ?assertMatch(
        {error, invalid_token}, emqx_dashboard_mfa:peek_temp_token(DummyUser, RandomToken)
    ),
    ok.

t_verify_token_consumed({init, Config}) ->
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"test_consumed_user">>, <<"TestPass1!">>, ?ROLE_VIEWER, <<>>
    ),
    Config;
t_verify_token_consumed({'end', _Config}) ->
    _ = emqx_dashboard_admin:remove_user(<<"test_consumed_user">>),
    ok;
t_verify_token_consumed(_Config) ->
    Username = <<"test_consumed_user">>,
    Token = emqx_dashboard_mfa:create_setup_token(Username, <<"SECRET">>),
    %% First verify should succeed
    ?assertMatch(
        {ok, Username, {setup, <<"SECRET">>}}, emqx_dashboard_mfa:verify_temp_token(Username, Token)
    ),
    %% Second verify should fail — token is consumed (one-time use)
    ?assertMatch({error, invalid_token}, emqx_dashboard_mfa:verify_temp_token(Username, Token)),
    ok.

%%====================================================================
%% Integration tests for check_sso_mfa flow
%%====================================================================

t_check_sso_mfa_no_force({init, Config}) ->
    mock_force_mfa(?SSO_BACKEND, false),
    Config;
t_check_sso_mfa_no_force({'end', _Config}) ->
    clear_force_mfa(?SSO_BACKEND),
    ok;
t_check_sso_mfa_no_force(_Config) ->
    [User] = emqx_dashboard_admin:lookup_user(?SSO_BACKEND, ?SSO_USER),
    Result = emqx_dashboard_sso_mfa:check_sso_mfa(User, ?SSO_BACKEND),
    %% force_mfa=false => no MFA, JWT signed at exchange time
    ?assertEqual({ok, login}, Result),
    ok.

t_check_sso_mfa_not_configured({init, Config}) ->
    mock_force_mfa(?SSO_BACKEND, true),
    ok = mock_totp(),
    %% Ensure user has no MFA state
    _ = emqx_dashboard_admin:clear_mfa_state(?SSO_USERNAME(?SSO_BACKEND, ?SSO_USER)),
    Config;
t_check_sso_mfa_not_configured({'end', _Config}) ->
    clear_force_mfa(?SSO_BACKEND),
    ok = unmock_totp(),
    ok;
t_check_sso_mfa_not_configured(_Config) ->
    [User] = emqx_dashboard_admin:lookup_user(?SSO_BACKEND, ?SSO_USER),
    Result = emqx_dashboard_sso_mfa:check_sso_mfa(User, ?SSO_BACKEND),
    %% force_mfa=true, no MFA state => mfa_setup
    ?assertMatch({mfa_setup, _SetupToken, _QRInfo}, Result),
    {mfa_setup, SetupToken, QRInfo} = Result,
    ?assert(is_binary(SetupToken)),
    ?assertMatch(#{secret := _, mechanism := totp}, QRInfo),
    ok.

t_check_sso_mfa_enabled({init, Config}) ->
    mock_force_mfa(?SSO_BACKEND, true),
    ok = mock_totp(),
    %% Set MFA enabled for SSO_USER2
    SsoUsername = ?SSO_USERNAME(?SSO_BACKEND, ?SSO_USER2),
    MfaState = #{mechanism => totp, secret => <<"TESTSECRET">>, first_verify_ts => 1000},
    {ok, ok} = emqx_dashboard_admin:set_mfa_state(SsoUsername, MfaState),
    Config;
t_check_sso_mfa_enabled({'end', _Config}) ->
    clear_force_mfa(?SSO_BACKEND),
    ok = unmock_totp(),
    _ = emqx_dashboard_admin:clear_mfa_state(?SSO_USERNAME(?SSO_BACKEND, ?SSO_USER2)),
    ok;
t_check_sso_mfa_enabled(_Config) ->
    [User] = emqx_dashboard_admin:lookup_user(?SSO_BACKEND, ?SSO_USER2),
    Result = emqx_dashboard_sso_mfa:check_sso_mfa(User, ?SSO_BACKEND),
    %% force_mfa=true, MFA enabled => mfa_verify
    ?assertMatch({mfa_verify, _VerifyToken}, Result),
    {mfa_verify, VerifyToken} = Result,
    ?assert(is_binary(VerifyToken)),
    ok.

t_check_sso_mfa_admin_disabled({init, Config}) ->
    mock_force_mfa(?SSO_BACKEND, true),
    %% Set MFA to disabled for SSO_USER (admin exemption)
    SsoUsername = ?SSO_USERNAME(?SSO_BACKEND, ?SSO_USER),
    %% First set MFA enabled so disable_mfa can work
    MfaState = #{mechanism => totp, secret => <<"TESTSECRET">>, first_verify_ts => 1000},
    {ok, ok} = emqx_dashboard_admin:set_mfa_state(SsoUsername, MfaState),
    ok = emqx_dashboard_admin:disable_mfa(SsoUsername),
    Config;
t_check_sso_mfa_admin_disabled({'end', _Config}) ->
    clear_force_mfa(?SSO_BACKEND),
    _ = emqx_dashboard_admin:clear_mfa_state(?SSO_USERNAME(?SSO_BACKEND, ?SSO_USER)),
    ok;
t_check_sso_mfa_admin_disabled(_Config) ->
    [User] = emqx_dashboard_admin:lookup_user(?SSO_BACKEND, ?SSO_USER),
    Result = emqx_dashboard_sso_mfa:check_sso_mfa(User, ?SSO_BACKEND),
    %% force_mfa=true, admin_disabled => skip MFA, JWT signed at exchange time
    ?assertEqual({ok, login}, Result),
    ok.

%%====================================================================
%% Admin MFA control tests
%%====================================================================

t_admin_disable_mfa({init, Config}) ->
    Config;
t_admin_disable_mfa({'end', _Config}) ->
    _ = emqx_dashboard_admin:clear_mfa_state(?SSO_USERNAME(?SSO_BACKEND, ?SSO_USER)),
    ok;
t_admin_disable_mfa(_Config) ->
    SsoUsername = ?SSO_USERNAME(?SSO_BACKEND, ?SSO_USER),
    OrigState = #{mechanism => totp, secret => <<"MYSECRET">>, first_verify_ts => 12345},
    {ok, ok} = emqx_dashboard_admin:set_mfa_state(SsoUsername, OrigState),
    %% Admin disables MFA
    ok = emqx_dashboard_admin:disable_mfa(SsoUsername),
    %% Verify state is disabled (secret cleared, record preserved as admin_disabled)
    ?assertEqual({ok, disabled}, emqx_dashboard_admin:get_mfa_state(SsoUsername)),
    ok.

%%====================================================================
%% SSO MFA API endpoint tests
%%====================================================================

t_mfa_setup_api({init, Config}) ->
    mock_force_mfa(?SSO_BACKEND, true),
    ok = mock_totp(),
    %% Ensure clean MFA state
    _ = emqx_dashboard_admin:clear_mfa_state(?SSO_USERNAME(?SSO_BACKEND, ?SSO_USER)),
    Config;
t_mfa_setup_api({'end', _Config}) ->
    clear_force_mfa(?SSO_BACKEND),
    ok = unmock_totp(),
    _ = emqx_dashboard_admin:clear_mfa_state(?SSO_USERNAME(?SSO_BACKEND, ?SSO_USER)),
    ok;
t_mfa_setup_api(_Config) ->
    %% Trigger check_sso_mfa to get a setup token
    [User] = emqx_dashboard_admin:lookup_user(?SSO_BACKEND, ?SSO_USER),
    {mfa_setup, SetupToken, _QRInfo} = emqx_dashboard_sso_mfa:check_sso_mfa(User, ?SSO_BACKEND),

    %% POST /sso/mfa/setup with valid setup_token and totp_code
    Body = #{
        <<"setup_token">> => SetupToken,
        <<"totp_code">> => ?GOOD_TOTP,
        <<"username">> => ?SSO_USER,
        <<"backend">> => atom_to_binary(?SSO_BACKEND)
    },
    {ok, 200, RspBody} = request_api(post, api_path(["sso", "mfa", "setup"]), no_auth_header, Body),
    Rsp = json_map(RspBody),
    ?assertMatch(#{<<"token">> := _, <<"license">> := _}, Rsp),
    ok.

t_mfa_verify_api({init, Config}) ->
    mock_force_mfa(?SSO_BACKEND, true),
    ok = mock_totp(),
    %% Ensure user has MFA enabled (so check_sso_mfa returns mfa_verify)
    SsoUsername = ?SSO_USERNAME(?SSO_BACKEND, ?SSO_USER),
    MfaState = #{mechanism => totp, secret => <<"VERIFYSECRET">>, first_verify_ts => 1000},
    {ok, ok} = emqx_dashboard_admin:set_mfa_state(SsoUsername, MfaState),
    Config;
t_mfa_verify_api({'end', _Config}) ->
    clear_force_mfa(?SSO_BACKEND),
    ok = unmock_totp(),
    _ = emqx_dashboard_admin:clear_mfa_state(?SSO_USERNAME(?SSO_BACKEND, ?SSO_USER)),
    ok;
t_mfa_verify_api(_Config) ->
    %% Trigger check_sso_mfa to get a verify token
    [User] = emqx_dashboard_admin:lookup_user(?SSO_BACKEND, ?SSO_USER),
    {mfa_verify, VerifyToken} = emqx_dashboard_sso_mfa:check_sso_mfa(User, ?SSO_BACKEND),

    %% POST /sso/mfa/verify with valid verify_token and totp_code
    Body = #{
        <<"verify_token">> => VerifyToken,
        <<"totp_code">> => ?GOOD_TOTP,
        <<"username">> => ?SSO_USER,
        <<"backend">> => atom_to_binary(?SSO_BACKEND)
    },
    {ok, 200, RspBody} = request_api(
        post, api_path(["sso", "mfa", "verify"]), no_auth_header, Body
    ),
    Rsp = json_map(RspBody),
    ?assertMatch(#{<<"token">> := _, <<"license">> := _}, Rsp),
    ok.

t_mfa_setup_invalid_token({init, Config}) ->
    ok = mock_totp(),
    Config;
t_mfa_setup_invalid_token({'end', _Config}) ->
    ok = unmock_totp(),
    ok;
t_mfa_setup_invalid_token(_Config) ->
    %% POST /sso/mfa/setup with bad token
    Body = #{
        <<"setup_token">> => <<"totally_invalid_token">>,
        <<"totp_code">> => ?GOOD_TOTP,
        <<"username">> => ?SSO_USER,
        <<"backend">> => atom_to_binary(?SSO_BACKEND)
    },
    {ok, 401, RspBody} = request_api(post, api_path(["sso", "mfa", "setup"]), no_auth_header, Body),
    Rsp = json_map(RspBody),
    ?assertMatch(#{<<"code">> := <<"UNAUTHORIZED">>}, Rsp),
    ok.

t_mfa_verify_bad_totp({init, Config}) ->
    mock_force_mfa(?SSO_BACKEND, true),
    ok = mock_totp(),
    %% Ensure user has MFA enabled
    SsoUsername = ?SSO_USERNAME(?SSO_BACKEND, ?SSO_USER),
    MfaState = #{mechanism => totp, secret => <<"VERIFYSECRET">>, first_verify_ts => 1000},
    {ok, ok} = emqx_dashboard_admin:set_mfa_state(SsoUsername, MfaState),
    Config;
t_mfa_verify_bad_totp({'end', _Config}) ->
    clear_force_mfa(?SSO_BACKEND),
    ok = unmock_totp(),
    _ = emqx_dashboard_admin:clear_mfa_state(?SSO_USERNAME(?SSO_BACKEND, ?SSO_USER)),
    ok;
t_mfa_verify_bad_totp(_Config) ->
    %% Get a verify token
    [User] = emqx_dashboard_admin:lookup_user(?SSO_BACKEND, ?SSO_USER),
    {mfa_verify, VerifyToken} = emqx_dashboard_sso_mfa:check_sso_mfa(User, ?SSO_BACKEND),

    %% POST /sso/mfa/verify with wrong TOTP code
    Body = #{
        <<"verify_token">> => VerifyToken,
        <<"totp_code">> => <<"999999">>,
        <<"username">> => ?SSO_USER,
        <<"backend">> => atom_to_binary(?SSO_BACKEND)
    },
    {ok, 401, RspBody} = request_api(
        post, api_path(["sso", "mfa", "verify"]), no_auth_header, Body
    ),
    Rsp = json_map(RspBody),
    ?assertMatch(#{<<"code">> := <<"UNAUTHORIZED">>}, Rsp),
    ok.

%%====================================================================
%% Unit tests for emqx_dashboard_sso_code (one-time code exchange)
%%====================================================================

t_sso_code_create_and_exchange({init, Config}) ->
    Config;
t_sso_code_create_and_exchange({'end', _Config}) ->
    _ = emqx_dashboard_admin:clear_sso_code(?SSO_USERNAME(?SSO_BACKEND, ?SSO_USER)),
    ok;
t_sso_code_create_and_exchange(_Config) ->
    SsoUsername = ?SSO_USERNAME(?SSO_BACKEND, ?SSO_USER),
    Payload = #{action => <<"login">>, username => <<"alice">>},
    Code = emqx_dashboard_sso_code:create_code(SsoUsername, Payload),
    ?assert(is_binary(Code)),
    ?assertMatch(<<"sso_", _/binary>>, Code),
    {ok, Returned} = emqx_dashboard_sso_code:exchange_code(?SSO_BACKEND, ?SSO_USER, Code),
    ?assertEqual(Payload, Returned),
    ?assert(maps:is_key(action, Returned)),
    ok.

t_sso_code_one_time_consumption({init, Config}) ->
    Config;
t_sso_code_one_time_consumption({'end', _Config}) ->
    _ = emqx_dashboard_admin:clear_sso_code(?SSO_USERNAME(?SSO_BACKEND, ?SSO_USER)),
    ok;
t_sso_code_one_time_consumption(_Config) ->
    SsoUsername = ?SSO_USERNAME(?SSO_BACKEND, ?SSO_USER),
    Payload = #{action => <<"login">>, username => <<"one_time_user">>},
    Code = emqx_dashboard_sso_code:create_code(SsoUsername, Payload),
    ?assertMatch({ok, _}, emqx_dashboard_sso_code:exchange_code(?SSO_BACKEND, ?SSO_USER, Code)),
    ?assertEqual(
        {error, not_found}, emqx_dashboard_sso_code:exchange_code(?SSO_BACKEND, ?SSO_USER, Code)
    ),
    ok.

t_sso_code_expired({init, Config}) ->
    Config;
t_sso_code_expired({'end', _Config}) ->
    _ = emqx_dashboard_admin:clear_sso_code(?SSO_USERNAME(?SSO_BACKEND, ?SSO_USER)),
    ok;
t_sso_code_expired(_Config) ->
    SsoUsername = ?SSO_USERNAME(?SSO_BACKEND, ?SSO_USER),
    Payload = #{action => <<"login">>, username => <<"expired_user">>},
    Code = emqx_dashboard_sso_code:create_code(SsoUsername, Payload),
    %% Manually set exptime to the past via set_sso_code
    ExpiredTime = erlang:system_time(second) - 10,
    {ok, ok} = emqx_dashboard_admin:set_sso_code(
        SsoUsername, #{code => Code, payload => Payload, exptime => ExpiredTime}
    ),
    ?assertEqual(
        {error, expired}, emqx_dashboard_sso_code:exchange_code(?SSO_BACKEND, ?SSO_USER, Code)
    ),
    ok.

t_sso_code_invalid({init, Config}) ->
    Config;
t_sso_code_invalid({'end', _Config}) ->
    ok;
t_sso_code_invalid(_Config) ->
    ?assertEqual(
        {error, not_found},
        emqx_dashboard_sso_code:exchange_code(?SSO_BACKEND, ?SSO_USER, <<"sso_nonexistent">>)
    ),
    ok.

t_sso_code_expired_rejected({init, Config}) ->
    Config;
t_sso_code_expired_rejected({'end', _Config}) ->
    _ = emqx_dashboard_admin:clear_sso_code(?SSO_USERNAME(?SSO_BACKEND, ?SSO_USER2)),
    ok;
t_sso_code_expired_rejected(_Config) ->
    %% Expired codes are properly rejected on exchange
    SsoUsername = ?SSO_USERNAME(?SSO_BACKEND, ?SSO_USER2),
    Payload = #{action => <<"login">>, username => <<"cleanup_user">>},
    %% Create a valid signed code, then manually expire it
    Code = emqx_dashboard_sso_code:create_code(SsoUsername, Payload),
    ExpiredTime = erlang:system_time(second) - 10,
    {ok, ok} = emqx_dashboard_admin:set_sso_code(
        SsoUsername, #{code => Code, payload => Payload, exptime => ExpiredTime}
    ),
    ?assertEqual(
        {error, expired}, emqx_dashboard_sso_code:exchange_code(?SSO_BACKEND, ?SSO_USER2, Code)
    ),
    ok.

%% --- Review fix tests ---

%% Test: peek_temp_token returns secret without consuming token
t_peek_temp_token({init, Config}) ->
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"test_peek_user">>, <<"TestPass1!">>, ?ROLE_VIEWER, <<>>
    ),
    Config;
t_peek_temp_token({'end', _Config}) ->
    _ = emqx_dashboard_admin:remove_user(<<"test_peek_user">>),
    ok;
t_peek_temp_token(_Config) ->
    Username = <<"test_peek_user">>,
    Secret = <<"PEEKSECRET">>,
    Token = emqx_dashboard_mfa:create_setup_token(Username, Secret),
    %% Peek should return the secret
    ?assertMatch(
        {ok, Username, {setup, Secret}}, emqx_dashboard_mfa:peek_temp_token(Username, Token)
    ),
    %% Token should NOT be consumed — peek again should work
    ?assertMatch(
        {ok, Username, {setup, Secret}}, emqx_dashboard_mfa:peek_temp_token(Username, Token)
    ),
    %% verify should still work (token still exists)
    ?assertMatch(
        {ok, Username, {setup, Secret}}, emqx_dashboard_mfa:verify_temp_token(Username, Token)
    ),
    %% Now token is consumed
    ?assertMatch({error, invalid_token}, emqx_dashboard_mfa:peek_temp_token(Username, Token)),
    ok.

%% Test: POST /sso/mfa/setup_info returns secret without consuming token
t_mfa_setup_info_api({init, Config}) ->
    mock_force_mfa(?SSO_BACKEND, true),
    ok = mock_totp(),
    _ = emqx_dashboard_admin:clear_mfa_state(?SSO_USERNAME(?SSO_BACKEND, ?SSO_USER)),
    Config;
t_mfa_setup_info_api({'end', _Config}) ->
    clear_force_mfa(?SSO_BACKEND),
    ok = unmock_totp(),
    _ = emqx_dashboard_admin:clear_mfa_state(?SSO_USERNAME(?SSO_BACKEND, ?SSO_USER)),
    ok;
t_mfa_setup_info_api(_Config) ->
    %% Get a setup token
    [User] = emqx_dashboard_admin:lookup_user(?SSO_BACKEND, ?SSO_USER),
    {mfa_setup, SetupToken, _QRInfo} = emqx_dashboard_sso_mfa:check_sso_mfa(User, ?SSO_BACKEND),

    %% POST /sso/mfa/setup_info — should return secret
    InfoBody = #{
        <<"setup_token">> => SetupToken,
        <<"username">> => ?SSO_USER,
        <<"backend">> => atom_to_binary(?SSO_BACKEND)
    },
    {ok, 200, InfoRspBody} = request_api(
        post, api_path(["sso", "mfa", "setup_info"]), no_auth_header, InfoBody
    ),
    InfoRsp = json_map(InfoRspBody),
    ?assertMatch(#{<<"secret">> := _, <<"mechanism">> := <<"totp">>}, InfoRsp),

    %% Token should NOT be consumed — setup should still work
    SetupBody = #{
        <<"setup_token">> => SetupToken,
        <<"totp_code">> => ?GOOD_TOTP,
        <<"username">> => ?SSO_USER,
        <<"backend">> => atom_to_binary(?SSO_BACKEND)
    },
    {ok, 200, SetupRspBody} = request_api(
        post, api_path(["sso", "mfa", "setup"]), no_auth_header, SetupBody
    ),
    SetupRsp = json_map(SetupRspBody),
    ?assertMatch(#{<<"token">> := _, <<"license">> := _}, SetupRsp),
    ok.

%%====================================================================
%% New tests for redundancy fixes
%%====================================================================

%% --- JWT deferred signing (token_exchange login flow) ---

%% Verify that check_sso_mfa returns {ok, login} (not JWT) when no MFA needed
t_check_sso_mfa_returns_login_intent({init, Config}) ->
    mock_force_mfa(?SSO_BACKEND, false),
    Config;
t_check_sso_mfa_returns_login_intent({'end', _Config}) ->
    clear_force_mfa(?SSO_BACKEND),
    ok;
t_check_sso_mfa_returns_login_intent(_Config) ->
    [User] = emqx_dashboard_admin:lookup_user(?SSO_BACKEND, ?SSO_USER),
    Result = emqx_dashboard_sso_mfa:check_sso_mfa(User, ?SSO_BACKEND),
    %% Should return {ok, login} not {ok, Role, Token}
    ?assertEqual({ok, login}, Result),
    ok.

%% Verify that sso_code payload for login has action => <<"login">> with no token
t_sso_code_login_payload_no_jwt({init, Config}) ->
    Config;
t_sso_code_login_payload_no_jwt({'end', _Config}) ->
    _ = emqx_dashboard_admin:clear_sso_code(?SSO_USERNAME(?SSO_BACKEND, ?SSO_USER)),
    ok;
t_sso_code_login_payload_no_jwt(_Config) ->
    SsoUsername = ?SSO_USERNAME(?SSO_BACKEND, ?SSO_USER),
    Payload = #{action => <<"login">>, username => ?SSO_USER, backend => ?SSO_BACKEND},
    Code = emqx_dashboard_sso_code:create_code(SsoUsername, Payload),
    {ok, Returned} = emqx_dashboard_sso_code:exchange_code(?SSO_BACKEND, ?SSO_USER, Code),
    %% Payload should have action but NO token (JWT signed later at exchange time)
    ?assertEqual(<<"login">>, maps:get(action, Returned)),
    ?assertNot(maps:is_key(token, Returned)),
    ok.

%% --- parse_backend protection ---

%% Verify that unknown backend in token_exchange returns 400
t_token_exchange_unknown_backend({init, Config}) ->
    Config;
t_token_exchange_unknown_backend({'end', _Config}) ->
    ok;
t_token_exchange_unknown_backend(_Config) ->
    Body = #{
        <<"code">> => <<"sso_fakecode.fakemac">>,
        <<"username">> => <<"alice">>,
        <<"backend">> => <<"nonexistent_backend_xyz">>
    },
    {ok, 400, RspBody} = request_api(
        post, api_path(["sso", "token_exchange"]), no_auth_header, Body
    ),
    Rsp = json_map(RspBody),
    ?assertMatch(#{<<"message">> := <<"Unknown SSO backend">>}, Rsp),
    ok.

%% --- disable_mfa merged behavior ---

%% Verify that disable_mfa on user with no MFA returns error
t_disable_mfa_not_configured({init, Config}) ->
    Config;
t_disable_mfa_not_configured({'end', _Config}) ->
    ok;
t_disable_mfa_not_configured(_Config) ->
    SsoUsername = ?SSO_USERNAME(?SSO_BACKEND, ?SSO_USER),
    _ = emqx_dashboard_admin:clear_mfa_state(SsoUsername),
    %% Disabling a user with no MFA configured sets mfa_state => disabled (ok, not error)
    Result = emqx_dashboard_admin:disable_mfa(SsoUsername),
    ?assertEqual(ok, Result),
    ?assertEqual({ok, disabled}, emqx_dashboard_admin:get_mfa_state(SsoUsername)),
    ok.

%% Verify that disable_mfa on already disabled user returns error
t_disable_mfa_already_disabled({init, Config}) ->
    Config;
t_disable_mfa_already_disabled({'end', _Config}) ->
    _ = emqx_dashboard_admin:clear_mfa_state(?SSO_USERNAME(?SSO_BACKEND, ?SSO_USER)),
    ok;
t_disable_mfa_already_disabled(_Config) ->
    SsoUsername = ?SSO_USERNAME(?SSO_BACKEND, ?SSO_USER),
    %% Enable MFA first
    MfaState = #{mechanism => totp, secret => <<"SECRET">>, first_verify_ts => 1000},
    {ok, ok} = emqx_dashboard_admin:set_mfa_state(SsoUsername, MfaState),
    %% First disable should succeed
    ok = emqx_dashboard_admin:disable_mfa(SsoUsername),
    %% Second disable should fail
    ?assertMatch(
        {error, <<"MFA is already disabled">>}, emqx_dashboard_admin:disable_mfa(SsoUsername)
    ),
    ok.

%% --- MFA endpoints with unknown backend ---

t_mfa_setup_unknown_backend({init, Config}) ->
    Config;
t_mfa_setup_unknown_backend({'end', _Config}) ->
    ok;
t_mfa_setup_unknown_backend(_Config) ->
    Body = #{
        <<"setup_token">> => <<"fake.token">>,
        <<"totp_code">> => <<"123456">>,
        <<"username">> => <<"alice">>,
        <<"backend">> => <<"nonexistent_xyz">>
    },
    {ok, 401, _} = request_api(
        post, api_path(["sso", "mfa", "setup"]), no_auth_header, Body
    ),
    ok.

mock_force_mfa(Backend, Value) ->
    %% Set force_mfa config for a specific SSO backend.
    %% We set it at the config path [dashboard, sso, Backend].
    %% get_force_mfa reads from emqx:get_config([dashboard, sso, Backend], undefined)
    %% and looks for #{force_mfa := ForceMfa} in the result.
    CurrentConf = emqx:get_config([dashboard, sso, Backend], #{}),
    NewConf = CurrentConf#{force_mfa => Value, backend => Backend},
    emqx_config:put([dashboard, sso, Backend], NewConf).

clear_force_mfa(Backend) ->
    CurrentConf = emqx:get_config([dashboard, sso, Backend], #{}),
    NewConf = maps:remove(force_mfa, CurrentConf),
    emqx_config:put([dashboard, sso, Backend], NewConf).

mock_totp() ->
    meck:new(pot, [passthrough, no_history]),
    meck:expect(pot, valid_totp, fun(Token, _) -> Token =:= ?GOOD_TOTP end),
    ok.

unmock_totp() ->
    meck:unload(pot).

json_map(X) when is_map(X) -> X;
json_map(X) when is_binary(X) -> emqx_utils_json:decode(X).

api_path(Parts) ->
    ?HOST ++ filename:join([?BASE_PATH | Parts]).

request_api(Method, Url, Auth) ->
    emqx_common_test_http:request_api(Method, Url, _QueryParams = [], Auth).

request_api(Method, Url, Auth, Body) ->
    emqx_common_test_http:request_api(Method, Url, _QueryParams = [], Auth, Body).
