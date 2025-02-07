%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_dashboard_mfa_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include("emqx_dashboard.hrl").

-define(HOST, "http://127.0.0.1:18083").

-define(BASE_PATH, "/api/v5").
-define(GOOD_TOTP, <<"123456">>).

-define(EE_ONLY(EXPR, NON_EE),
    case emqx_release:edition() of
        ee -> EXPR;
        _ -> NON_EE
    end
).

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
                    emqx_mgmt_api_test_util:emqx_dashboard()
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

%% Users for login testing, an administrator role and a viewer role.
init_users() ->
    mnesia:clear_table(?ADMIN),
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"admin1">>, <<"admin1pass">>, ?ROLE_SUPERUSER, "admin"
    ),
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"viewer1">>, <<"viewer1pass">>, ?ROLE_VIEWER, "viewer"
    ),
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"viewer2">>, <<"viewer2pass">>, ?ROLE_VIEWER, "viewer"
    ),
    ok.

%% login when there is no MFA state
%% expect only password check
t_login_no_mfa_setting_no_mfa_token({init, Config}) ->
    Config;
t_login_no_mfa_setting_no_mfa_token({'end', _Config}) ->
    ok;
t_login_no_mfa_setting_no_mfa_token(_Config) ->
    Body1 =
        #{
            <<"username">> => <<"admin1">>,
            <<"password">> => <<"admin1pass">>
        },
    Body2 = Body1#{<<"mfa_token">> => <<"123456">>},
    BadBody = Body2#{<<"password">> => <<"wrongpassword1">>},
    ?assertMatch({ok, 200, _}, login(Body1)),
    ?assertMatch({ok, 200, _}, login(Body2)),
    {ok, 401, BadPassRsp} = login(BadBody),
    ok = assert_return_code("BAD_USERNAME_OR_PWD", BadPassRsp),
    ok.

%% login when there is MFA state initialized
%% expect to fail with a TOTP secret prompt
t_login_with_mfa_setting({init, Config}) ->
    ok = mock_totp(),
    Config;
t_login_with_mfa_setting({'end', _Config}) ->
    ok = unmock_totp();
t_login_with_mfa_setting(_Config) ->
    LoginBody =
        #{
            <<"username">> => <<"viewer1">>,
            <<"password">> => <<"viewer1pass">>
        },
    BadPwd = LoginBody#{<<"password">> => <<"wrongpassword1">>},
    BadTotp = LoginBody#{<<"mfa_token">> => <<"233333">>},
    BadPwdWithTotp = BadPwd#{<<"mfa_token">> => ?GOOD_TOTP},
    LoginWithTotp = LoginBody#{<<"mfa_token">> => ?GOOD_TOTP},
    ?assertMatch({ok, 204, _}, enable_mfa(<<"viewer1">>)),
    {ok, 401, BadPass} = login(BadPwd),
    ok = assert_return_code("BAD_USERNAME_OR_PWD", BadPass),
    %% expect to get a hint about missing MFA token
    ExpectTotpMissingFn = fun(IsWithSecret) ->
        {ok, 401, Rsp} = login(LoginBody),
        #{
            <<"code">> := <<"BAD_MFA_TOKEN">>,
            <<"message">> :=
                #{
                    <<"error">> := <<"missing_mfa_token">>,
                    <<"mechanism">> := <<"totp">>
                } = Msg
        } = json_map(Rsp),
        %% assert
        true = (IsWithSecret =:= maps:is_key(<<"secret">>, Msg)),
        ok
    end,
    %% expect to get a hint about bad MFA token
    ExpectBadTotpFn = fun(Body) ->
        {ok, 401, Rsp} = login(Body),
        #{
            <<"code">> := <<"BAD_MFA_TOKEN">>,
            <<"message">> :=
                #{
                    <<"error">> := <<"bad_mfa_token">>,
                    <<"mechanism">> := <<"totp">>
                } = Msg
        } = json_map(Rsp),
        %% assert
        false = maps:is_key(<<"secret">>, Msg),
        ok
    end,
    ok = ExpectTotpMissingFn(true),
    %% now EMQX is expecting viewer1 to provide a token for the first time
    %% but login with a bad password should continue to fail
    {ok, 401, BadPass1} = login(BadPwd),
    ok = assert_return_code("BAD_USERNAME_OR_PWD", BadPass1),
    ok = ExpectTotpMissingFn(true),
    ok = ExpectBadTotpFn(BadTotp),
    %% secret is still presented after a failed login with bad token
    ok = ExpectTotpMissingFn(true),
    %% try to login with good TOTP but bad password
    %% this should return bad password error
    {ok, 401, BadPass2} = login(BadPwdWithTotp),
    ok = assert_return_code("BAD_USERNAME_OR_PWD", BadPass2),
    ok = ExpectTotpMissingFn(true),
    %% login with good totp and password
    ?assertMatch({ok, 200, _}, login(LoginWithTotp)),
    %% once MFA is setup, it should no longer send back TOTP secret.
    ok = ExpectTotpMissingFn(false),
    %% login again with bad password bad token should result in 401 (not 401) and 'bad_mfa_token' in message
    ok = ExpectBadTotpFn(BadPwd#{<<"mfa_token">> => <<"badtoken2">>}),
    ok.

%% Enable then delete MFA.
t_disable_mfa({init, Config}) ->
    ok = mock_totp(),
    Config;
t_disable_mfa({'end', _Config}) ->
    ok = unmock_totp();
t_disable_mfa(_Config) ->
    LoginBody =
        #{
            <<"username">> => <<"viewer1">>,
            <<"password">> => <<"viewer1pass">>,
            <<"mfa_token">> => <<"123456">>
        },
    AdminJwtToken = admin_jwt_token(),
    %% enable by admin1 for viewer1
    ?assertMatch({ok, 204, _}, enable_mfa(<<"viewer1">>), AdminJwtToken),
    ?assertMatch({ok, 204, _}, enable_mfa(<<"viewer2">>), AdminJwtToken),
    {ok, 200, RspBody} = login(LoginBody),
    #{<<"token">> := JwtToken} = json_map(RspBody),
    ?assertMatch({ok, 204, _}, disable_mfa(<<"viewer1">>, JwtToken)),
    %% viewer is not allow to enable other user's MFA
    ?assertMatch({ok, 403, _}, enable_mfa(<<"viewer2">>, JwtToken)),
    %% viewer is not allow to disable other user's MFA
    ?assertMatch({ok, 403, _}, disable_mfa(<<"viewer2">>, JwtToken)),
    %% admin can disable other user's MFA
    ?assertMatch({ok, 204, _}, disable_mfa(<<"viewer2">>, AdminJwtToken)),
    %% disable for viewer1 again should return success
    ?assertMatch({ok, 204, _}, disable_mfa(<<"viewer1">>, AdminJwtToken)),
    ok.

%% Enable MFA from config.
%% There is no need to call users/{username}/mfa API to enable MFA
t_enable_by_config({init, Config}) ->
    %% configure default MFA to use totp mechanism
    emqx_config:put([dashboard, default_mfa], #{mechanism => totp}),
    %% clear state so it can init a new state according to config
    {ok, ok} = emqx_dashboard_admin:clear_mfa_state(<<"viewer1">>),
    ok = mock_totp(),
    Config;
t_enable_by_config({'end', _Config}) ->
    emqx_config:put([dashboard, default_mfa], none),
    ok = unmock_totp();
t_enable_by_config(_Config) ->
    ?assertMatch(#{<<"mfa">> := <<"none">>}, get_user(<<"viewer1">>)),
    LoginBody =
        #{
            <<"username">> => <<"viewer1">>,
            <<"password">> => <<"viewer1pass">>
        },
    %% cannot login without TOTP because default MFA is configured
    {ok, 401, Rsp1} = login(LoginBody),
    ?assertMatch(#{<<"mfa">> := <<"totp">>}, get_user(<<"viewer1">>)),
    ?assertMatch(
        #{
            <<"code">> := <<"BAD_MFA_TOKEN">>,
            <<"message">> :=
                #{
                    <<"error">> := <<"missing_mfa_token">>,
                    <<"mechanism">> := <<"totp">>,
                    <<"secret">> := _
                }
        },
        json_map(Rsp1)
    ),
    {ok, 401, Rsp2} = login(LoginBody#{<<"mfa_token">> => <<"badtotp">>}),
    ?assertMatch(
        #{
            <<"code">> := <<"BAD_MFA_TOKEN">>,
            <<"message">> :=
                #{
                    <<"error">> := <<"bad_mfa_token">>,
                    <<"mechanism">> := <<"totp">>
                }
        },
        json_map(Rsp2)
    ),
    {ok, 200, LoginRsp} = login(LoginBody#{<<"mfa_token">> => ?GOOD_TOTP}),
    #{<<"token">> := JwtToken} = json_map(LoginRsp),
    %% mark MFA disalbed for this user
    ?assertMatch({ok, 204, _}, disable_mfa(<<"viewer1">>, JwtToken)),
    ?assertMatch(#{<<"mfa">> := <<"disabled">>}, get_user(<<"viewer1">>)),
    %% should be able to login without TOTP even though default MFA is configured
    {ok, 200, _} = login(LoginBody),
    ok.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

bin(X) -> iolist_to_binary(X).

json_map(X) when is_map(X) -> X;
json_map(X) when is_binary(X) -> emqx_utils_json:decode(X).

assert_return_message_error(Code, Body) ->
    Exp = bin(Code),
    Got = maps:get(<<"error">>, maps:get(<<"message">>, json_map(Body))),
    ?assertEqual(Exp, Got).

assert_return_code(Code, Body) ->
    Exp = bin(Code),
    Got = maps:get(<<"code">>, json_map(Body)),
    ?assertEqual(Exp, Got).

login(Body) ->
    request_api(post, api_path(["login"]), no_auth_header, Body).

enable_mfa(User) ->
    enable_mfa(User, admin_jwt_token()).

enable_mfa(User, JwtToken) ->
    Body = #{mechanism => totp},
    request_api(post, api_path(["users", User, "mfa"]), auth_header(JwtToken), Body).

disable_mfa(User, JwtToken) ->
    request_api(delete, api_path(["users", User, mfa]), auth_header(JwtToken), #{}).

get_user(Name) ->
    Users = list_users(),
    [User] = lists:filter(fun(#{<<"username">> := N}) -> Name =:= N end, Users),
    User.

list_users() ->
    {ok, 200, List} = request_api(get, api_path(["users"]), auth_header()),
    emqx_utils_json:decode(List).

admin_jwt_token() ->
    {ok, #{token := JwtToken}} = emqx_dashboard_admin:sign_token(
        <<"admin1">>, <<"admin1pass">>, ?TRUSTED_MFA_TOKEN
    ),
    JwtToken.

auth_header() ->
    auth_header(admin_jwt_token()).

auth_header(JwtToken) ->
    {"Authorization", "Bearer " ++ binary_to_list(JwtToken)}.

api_path(Parts) ->
    ?HOST ++ filename:join([?BASE_PATH | Parts]).

json(Data) ->
    {ok, Jsx} = emqx_utils_json:safe_decode(Data, [return_maps]),
    Jsx.

request_api(Method, Url, Auth) ->
    emqx_common_test_http:request_api(Method, Url, _QueryParams = [], Auth).

request_api(Method, Url, Auth, Body) ->
    emqx_common_test_http:request_api(Method, Url, _QueryParams = [], Auth, Body).

%% TOTP is not very friendly for tests due to its
%% time-based nature, here we mock it for determinstic
mock_totp() ->
    meck:new(pot, [passthrough, no_history]),
    meck:expect(pot, valid_totp, fun(Token, _) -> Token =:= ?GOOD_TOTP end),
    ok.

unmock_totp() ->
    meck:unload(pot).
