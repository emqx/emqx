%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_auth_jwt_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() -> emqx_ct:all(?MODULE).

init_per_testcase(TestCase, Config) ->
    ?MODULE:TestCase(init, Config),
    emqx_ct_helpers:start_apps([emqx_auth_jwt], fun set_special_configs/1),
    Config.

end_per_testcase(_Case, _Config) ->
    emqx_ct_helpers:stop_apps([emqx_auth_jwt]).

set_special_configs(emqx) ->
    application:set_env(emqx, allow_anonymous, false),
    application:set_env(emqx, acl_nomatch, deny),
    application:set_env(emqx, enable_acl_cache, false),
    LoadedPluginPath = filename:join(["test", "emqx_SUITE_data", "loaded_plugins"]),
    AclFilePath = filename:join(["test", "emqx_SUITE_data", "acl.conf"]),
    application:set_env(emqx, plugins_loaded_file,
                        emqx_ct_helpers:deps_path(emqx, LoadedPluginPath)),
    application:set_env(emqx, acl_file,
                        emqx_ct_helpers:deps_path(emqx, AclFilePath));

set_special_configs(emqx_auth_jwt) ->
    application:set_env(emqx_auth_jwt, secret, "emqxsecret"),
    application:set_env(emqx_auth_jwt, from, password);

set_special_configs(_) ->
    ok.

sign(Payload, Header, Key) when is_map(Header) ->
    Jwk = jose_jwk:from_oct(Key),
    Jwt = emqx_json:encode(Payload),
    {_, Token} = jose_jws:compact(jose_jwt:sign(Jwk, Header, Jwt)),
    Token;

sign(Payload, Alg, Key) ->
    Jwk = jose_jwk:from_oct(Key),
    Jwt = emqx_json:encode(Payload),
    {_, Token} = jose_jws:compact(jose_jwt:sign(Jwk, #{<<"alg">> => Alg}, Jwt)),
    Token.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_check_auth(init, _Config) ->
    application:unset_env(emqx_auth_jwt, verify_claims).
t_check_auth(_Config) ->
    Plain = #{clientid => <<"client1">>, username => <<"plain">>, zone => external},
    Jwt = sign([{clientid, <<"client1">>},
                {username, <<"plain">>},
                {exp, os:system_time(seconds) + 3}], <<"HS256">>, <<"emqxsecret">>),
    ct:pal("Jwt: ~p~n", [Jwt]),

    Result0 = emqx_access_control:authenticate(Plain#{password => Jwt}),
    ct:pal("Auth result: ~p~n", [Result0]),
    ?assertMatch({ok, #{auth_result := success, jwt_claims := #{<<"clientid">> := <<"client1">>}}}, Result0),

    ct:sleep(3100),
    Result1 = emqx_access_control:authenticate(Plain#{password => Jwt}),
    ct:pal("Auth result after 1000ms: ~p~n", [Result1]),
    ?assertMatch({error, _}, Result1),

    Jwt_Error = sign([{client_id, <<"client1">>},
                      {username, <<"plain">>}], <<"HS256">>, <<"secret">>),
    ct:pal("invalid jwt: ~p~n", [Jwt_Error]),
    Result2 = emqx_access_control:authenticate(Plain#{password => Jwt_Error}),
    ct:pal("Auth result for the invalid jwt: ~p~n", [Result2]),
    ?assertEqual({error, invalid_signature}, Result2),
    ?assertMatch({error, _}, emqx_access_control:authenticate(Plain#{password => <<"asd">>})).

t_check_claims(init, _Config) ->
    application:set_env(emqx_auth_jwt, verify_claims, [{sub, <<"value">>}]).
t_check_claims(_Config) ->
    Plain = #{clientid => <<"client1">>, username => <<"plain">>, zone => external},
    Jwt = sign([{client_id, <<"client1">>},
                {username, <<"plain">>},
                {sub, value},
                {exp, os:system_time(seconds) + 3}], <<"HS256">>, <<"emqxsecret">>),
    Result0 = emqx_access_control:authenticate(Plain#{password => Jwt}),
    ct:pal("Auth result: ~p~n", [Result0]),
    ?assertMatch({ok, #{auth_result := success, jwt_claims := _}}, Result0),
    Jwt_Error = sign([{clientid, <<"client1">>},
                       {username, <<"plain">>}], <<"HS256">>, <<"secret">>),
    Result2 = emqx_access_control:authenticate(Plain#{password => Jwt_Error}),
    ct:pal("Auth result for the invalid jwt: ~p~n", [Result2]),
    ?assertEqual({error, invalid_signature}, Result2).

t_check_claims_clientid(init, _Config) ->
    application:set_env(emqx_auth_jwt, verify_claims, [{clientid, <<"%c">>}]).
t_check_claims_clientid(_Config) ->
    Plain = #{clientid => <<"client23">>, username => <<"plain">>, zone => external},
    Jwt = sign([{clientid, <<"client23">>},
                {username, <<"plain">>},
                {exp, os:system_time(seconds) + 3}], <<"HS256">>, <<"emqxsecret">>),
    Result0 = emqx_access_control:authenticate(Plain#{password => Jwt}),
    ct:pal("Auth result: ~p~n", [Result0]),
    ?assertMatch({ok, #{auth_result := success, jwt_claims := _}}, Result0),
    Jwt_Error = sign([{clientid, <<"client1">>},
                      {username, <<"plain">>}], <<"HS256">>, <<"secret">>),
    Result2 = emqx_access_control:authenticate(Plain#{password => Jwt_Error}),
    ct:pal("Auth result for the invalid jwt: ~p~n", [Result2]),
    ?assertEqual({error, invalid_signature}, Result2).

t_check_claims_username(init, _Config) ->
    application:set_env(emqx_auth_jwt, verify_claims, [{username, <<"%u">>}]).
t_check_claims_username(_Config) ->
    Plain = #{clientid => <<"client23">>, username => <<"plain">>, zone => external},
    Jwt = sign([{client_id, <<"client23">>},
                {username, <<"plain">>},
                {exp, os:system_time(seconds) + 3}], <<"HS256">>, <<"emqxsecret">>),
    Result0 = emqx_access_control:authenticate(Plain#{password => Jwt}),
    ct:pal("Auth result: ~p~n", [Result0]),
    ?assertMatch({ok, #{auth_result := success, jwt_claims := _}}, Result0),
    Jwt_Error = sign([{clientid, <<"client1">>},
                      {username, <<"plain">>}], <<"HS256">>, <<"secret">>),
    Result3 = emqx_access_control:authenticate(Plain#{password => Jwt_Error}),
    ct:pal("Auth result for the invalid jwt: ~p~n", [Result3]),
    ?assertEqual({error, invalid_signature}, Result3).

t_check_claims_kid_in_header(init, _Config) ->
    application:set_env(emqx_auth_jwt, verify_claims, []).
t_check_claims_kid_in_header(_Config) ->
    Plain = #{clientid => <<"client23">>, username => <<"plain">>, zone => external},
    Jwt = sign([{clientid, <<"client23">>},
                {username, <<"plain">>},
                {exp, os:system_time(seconds) + 3}],
               #{<<"alg">> => <<"HS256">>,
                 <<"kid">> => <<"a_kid_str">>}, <<"emqxsecret">>),
    Result0 = emqx_access_control:authenticate(Plain#{password => Jwt}),
    ct:pal("Auth result: ~p~n", [Result0]),
    ?assertMatch({ok, #{auth_result := success, jwt_claims := _}}, Result0).

t_check_jwt_acl(init, _Config) ->
    application:set_env(emqx_auth_jwt, verify_claims, [{sub, <<"value">>}]).
t_check_jwt_acl(_Config) ->
    Jwt = sign([{client_id, <<"client1">>},
                {username, <<"plain">>},
                {sub, value},
                {acl, [{sub, [<<"a/b">>]},
                       {pub, [<<"c/d">>]}]},
                {exp, os:system_time(seconds) + 10}],
               <<"HS256">>,
               <<"emqxsecret">>),

    {ok, C} = emqtt:start_link(
                [{clean_start, true},
                 {proto_ver, v5},
                 {client_id, <<"client1">>},
                 {password, Jwt}]),
    {ok, _} = emqtt:connect(C),

    ?assertMatch(
       {ok, #{}, [0]},
       emqtt:subscribe(C, <<"a/b">>, 0)),

    ?assertMatch(
       ok,
       emqtt:publish(C, <<"c/d">>, <<"hi">>, 0)),

    ?assertMatch(
       {ok, #{}, [?RC_NOT_AUTHORIZED]},
       emqtt:subscribe(C, <<"c/d">>, 0)),

    ok = emqtt:publish(C, <<"a/b">>, <<"hi">>, 0),

    receive
        {publish, #{topic := <<"a/b">>}} ->
            ?assert(false, "Publish to `a/b` should not be allowed")
    after 100 -> ok
    end,

    ok = emqtt:disconnect(C).

t_check_jwt_acl_no_recs(init, _Config) ->
    application:set_env(emqx_auth_jwt, verify_claims, [{sub, <<"value">>}]).
t_check_jwt_acl_no_recs(_Config) ->
    Jwt = sign([{client_id, <<"client1">>},
                {username, <<"plain">>},
                {sub, value},
                {acl, []},
                {exp, os:system_time(seconds) + 10}],
               <<"HS256">>,
               <<"emqxsecret">>),

    {ok, C} = emqtt:start_link(
                [{clean_start, true},
                 {proto_ver, v5},
                 {client_id, <<"client1">>},
                 {password, Jwt}]),
    {ok, _} = emqtt:connect(C),

    ?assertMatch(
       {ok, #{}, [?RC_NOT_AUTHORIZED]},
       emqtt:subscribe(C, <<"a/b">>, 0)),

    ok = emqtt:disconnect(C).

t_check_jwt_acl_no_acl_claim(init, _Config) ->
    application:set_env(emqx_auth_jwt, verify_claims, [{sub, <<"value">>}]).
t_check_jwt_acl_no_acl_claim(_Config) ->
    Jwt = sign([{client_id, <<"client1">>},
                {username, <<"plain">>},
                {sub, value},
                {exp, os:system_time(seconds) + 10}],
               <<"HS256">>,
               <<"emqxsecret">>),

    {ok, C} = emqtt:start_link(
                [{clean_start, true},
                 {proto_ver, v5},
                 {client_id, <<"client1">>},
                 {password, Jwt}]),
    {ok, _} = emqtt:connect(C),

    ?assertMatch(
       {ok, #{}, [?RC_NOT_AUTHORIZED]},
       emqtt:subscribe(C, <<"a/b">>, 0)),

    ok = emqtt:disconnect(C).

t_check_jwt_acl_expire(init, _Config) ->
    application:set_env(emqx_auth_jwt, verify_claims, [{sub, <<"value">>}]).
t_check_jwt_acl_expire(_Config) ->
    Jwt = sign([{client_id, <<"client1">>},
                {username, <<"plain">>},
                {sub, value},
                {acl, [{sub, [<<"a/b">>]}]},
                {exp, os:system_time(seconds) + 1}],
               <<"HS256">>,
               <<"emqxsecret">>),

    {ok, C} = emqtt:start_link(
                [{clean_start, true},
                 {proto_ver, v5},
                 {client_id, <<"client1">>},
                 {password, Jwt}]),
    {ok, _} = emqtt:connect(C),

    ?assertMatch(
       {ok, #{}, [0]},
       emqtt:subscribe(C, <<"a/b">>, 0)),

    ?assertMatch(
       {ok, #{}, [0]},
       emqtt:unsubscribe(C, <<"a/b">>)),

    timer:sleep(2000),

    ?assertMatch(
       {ok, #{}, [?RC_NOT_AUTHORIZED]},
       emqtt:subscribe(C, <<"a/b">>, 0)),

    ok = emqtt:disconnect(C).
