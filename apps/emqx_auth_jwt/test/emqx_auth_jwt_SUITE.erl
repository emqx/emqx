%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

all() -> emqx_ct:all(?MODULE).

init_per_testcase(TestCase, Config) ->
    ?MODULE:TestCase(init, Config),
    emqx_ct_helpers:start_apps([emqx_auth_jwt], fun set_special_configs/1),
    Config.

end_per_testcase(TestCase, Config) ->
    try ?MODULE:TestCase('end', Config) catch _:_ -> ok end,
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
                {exp, erlang:system_time(seconds) + 2}], <<"HS256">>, <<"emqxsecret">>),
    ct:pal("Jwt: ~p~n", [Jwt]),

    Result0 = emqx_access_control:authenticate(Plain#{password => Jwt}),
    ct:pal("Auth result: ~p~n", [Result0]),
    ?assertMatch({ok, #{auth_result := success, jwt_claims := #{<<"clientid">> := <<"client1">>}}}, Result0),

    ct:sleep(3100),
    Result1 = emqx_access_control:authenticate(Plain#{password => Jwt}),
    ct:pal("Auth result after 3100ms: ~p~n", [Result1]),
    ?assertMatch({error, _}, Result1),

    Jwt_Error = sign([{client_id, <<"client1">>},
                      {username, <<"plain">>}], <<"HS256">>, <<"secret">>),
    ct:pal("invalid jwt: ~p~n", [Jwt_Error]),
    Result2 = emqx_access_control:authenticate(Plain#{password => Jwt_Error}),
    ct:pal("Auth result for the invalid jwt: ~p~n", [Result2]),
    ?assertEqual({error, invalid_signature}, Result2),
    ?assertMatch({error, _}, emqx_access_control:authenticate(Plain#{password => <<"asd">>})).

t_check_nbf(init, _Config) ->
    application:unset_env(emqx_auth_jwt, verify_claims).
t_check_nbf(_Config) ->
    Plain = #{clientid => <<"client1">>, username => <<"plain">>, zone => external},
    Jwt = sign([{clientid, <<"client1">>},
                {username, <<"plain">>},
                {nbf, erlang:system_time(seconds) + 3}], <<"HS256">>, <<"emqxsecret">>),
    ct:pal("Jwt: ~p~n", [Jwt]),

    Result0 = emqx_access_control:authenticate(Plain#{password => Jwt}),
    ct:pal("Auth result: ~p~n", [Result0]),
    ?assertEqual({error, {invalid_signature, not_valid_yet}}, Result0).

t_check_iat(init, _Config) ->
    application:unset_env(emqx_auth_jwt, verify_claims).
t_check_iat(_Config) ->
    Plain = #{clientid => <<"client1">>, username => <<"plain">>, zone => external},
    Jwt = sign([{clientid, <<"client1">>},
                {username, <<"plain">>},
                {iat, erlang:system_time(seconds) + 3}], <<"HS256">>, <<"emqxsecret">>),
    ct:pal("Jwt: ~p~n", [Jwt]),

    Result0 = emqx_access_control:authenticate(Plain#{password => Jwt}),
    ct:pal("Auth result: ~p~n", [Result0]),
    ?assertEqual({error, {invalid_signature, issued_in_future}}, Result0).

t_check_auth_invalid_exp(init, _Config) ->
    application:unset_env(emqx_auth_jwt, verify_claims).
t_check_auth_invalid_exp(_Config) ->
    Plain = #{clientid => <<"client1">>, username => <<"plain">>, zone => external},
    Jwt0 = sign([{clientid, <<"client1">>},
                 {username, <<"plain">>},
                 {exp, [{foo, bar}]}], <<"HS256">>, <<"emqxsecret">>),
    ct:pal("Jwt: ~p~n", [Jwt0]),

    Result0 = emqx_access_control:authenticate(Plain#{password => Jwt0}),
    ct:pal("Auth result: ~p~n", [Result0]),
    ?assertMatch({error, _}, Result0),

    Jwt1 = sign([{clientid, <<"client1">>},
                 {username, <<"plain">>},
                 {exp, <<"foobar">>}], <<"HS256">>, <<"emqxsecret">>),
    ct:pal("Jwt: ~p~n", [Jwt1]),

    Result1 = emqx_access_control:authenticate(Plain#{password => Jwt1}),
    ct:pal("Auth result: ~p~n", [Result1]),
    ?assertMatch({error, _}, Result1).

t_check_auth_str_exp(init, _Config) ->
    application:unset_env(emqx_auth_jwt, verify_claims).
t_check_auth_str_exp(_Config) ->
    Plain = #{clientid => <<"client1">>, username => <<"plain">>, zone => external},
    Exp = integer_to_binary(erlang:system_time(seconds) + 3),

    Jwt0 = sign([{clientid, <<"client1">>},
                 {username, <<"plain">>},
                 {exp, Exp}], <<"HS256">>, <<"emqxsecret">>),
    ct:pal("Jwt: ~p~n", [Jwt0]),

    Result0 = emqx_access_control:authenticate(Plain#{password => Jwt0}),
    ct:pal("Auth result: ~p~n", [Result0]),
    ?assertMatch({ok, #{auth_result := success, jwt_claims := _}}, Result0),

    Jwt1 = sign([{clientid, <<"client1">>},
                 {username, <<"plain">>},
                 {exp, <<"0">>}], <<"HS256">>, <<"emqxsecret">>),
    ct:pal("Jwt: ~p~n", [Jwt1]),

    Result1 = emqx_access_control:authenticate(Plain#{password => Jwt1}),
    ct:pal("Auth result: ~p~n", [Result1]),
    ?assertMatch({error, _}, Result1),

    Exp2 = float_to_binary(erlang:system_time(seconds) + 3.5),

    Jwt2 = sign([{clientid, <<"client1">>},
                 {username, <<"plain">>},
                 {exp, Exp2}], <<"HS256">>, <<"emqxsecret">>),
    ct:pal("Jwt: ~p~n", [Jwt2]),

    Result2 = emqx_access_control:authenticate(Plain#{password => Jwt2}),
    ct:pal("Auth result: ~p~n", [Result2]),
    ?assertMatch({ok, #{auth_result := success, jwt_claims := _}}, Result2).

t_check_auth_float_exp(init, _Config) ->
    application:unset_env(emqx_auth_jwt, verify_claims).
t_check_auth_float_exp(_Config) ->
    Plain = #{clientid => <<"client1">>, username => <<"plain">>, zone => external},
    Exp = erlang:system_time(seconds) + 3.5,

    Jwt0 = sign([{clientid, <<"client1">>},
                 {username, <<"plain">>},
                 {exp, Exp}], <<"HS256">>, <<"emqxsecret">>),
    ct:pal("Jwt: ~p~n", [Jwt0]),

    Result0 = emqx_access_control:authenticate(Plain#{password => Jwt0}),
    ct:pal("Auth result: ~p~n", [Result0]),
    ?assertMatch({ok, #{auth_result := success, jwt_claims := _}}, Result0),

    Jwt1 = sign([{clientid, <<"client1">>},
                 {username, <<"plain">>},
                 {exp, 1.5}], <<"HS256">>, <<"emqxsecret">>),
    ct:pal("Jwt: ~p~n", [Jwt1]),

    Result1 = emqx_access_control:authenticate(Plain#{password => Jwt1}),
    ct:pal("Auth result: ~p~n", [Result1]),
    ?assertMatch({error, _}, Result1).

t_check_claims(init, _Config) ->
    application:set_env(emqx_auth_jwt, verify_claims, [{sub, <<"value">>}]).
t_check_claims(_Config) ->
    Plain = #{clientid => <<"client1">>, username => <<"plain">>, zone => external},
    Jwt = sign([{client_id, <<"client1">>},
                {username, <<"plain">>},
                {sub, value},
                {exp, erlang:system_time(seconds) + 3}], <<"HS256">>, <<"emqxsecret">>),
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
                {exp, erlang:system_time(seconds) + 3}], <<"HS256">>, <<"emqxsecret">>),
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
                {exp, erlang:system_time(seconds) + 3}], <<"HS256">>, <<"emqxsecret">>),
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
                {exp, erlang:system_time(seconds) + 3}],
               #{<<"alg">> => <<"HS256">>,
                 <<"kid">> => <<"a_kid_str">>}, <<"emqxsecret">>),
    Result0 = emqx_access_control:authenticate(Plain#{password => Jwt}),
    ct:pal("Auth result: ~p~n", [Result0]),
    ?assertMatch({ok, #{auth_result := success, jwt_claims := _}}, Result0).

t_keys_update(init, _Config) ->
    ok = meck:new(httpc, [passthrough, no_history]),
    ok = meck:expect(
           httpc,
           request,
           fun(get, _, _, _) ->
                   {ok,
                    {200,
                     [],
                     jiffy:encode(#{<<"keys">> => []})}}
           end),

    application:set_env(emqx_auth_jwt, verify_claims, []),
    application:set_env(emqx_auth_jwt, refresh_interval, 100),
    application:set_env(emqx_auth_jwt, jwks, "http://localhost:4001/keys.json").
t_keys_update(_Config) ->
    ?check_trace(
       snabbkaffe:block_until(
         ?match_n_events(2, #{?snk_kind := emqx_auth_jwt_svr_jwks_updated}),
         _Timeout    = infinity,
         _BackInTIme = 0),
       fun(_, Trace) ->
               ?assertMatch([#{pid := Pid}, #{pid := Pid} |  _],
                            ?of_kind(emqx_auth_jwt_svr_jwks_updated, Trace))
       end).

t_check_jwt_acl(init, _Config) ->
    application:set_env(emqx_auth_jwt, verify_claims, [{sub, <<"value">>}]).
t_check_jwt_acl(_Config) ->
    Jwt = sign([{client_id, <<"client1">>},
                {username, <<"plain">>},
                {sub, value},
                {acl, [{sub, [<<"a/b">>]},
                       {pub, [<<"c/d">>]},
                       {all, [<<"all">>]}]},
                {exp, erlang:system_time(seconds) + 10}],
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

    %% can pub/sub to all rules
    ?assertMatch(
      {ok, #{}, [0]},
      emqtt:subscribe(C, <<"all">>, 0)),

    ?assertMatch(
      ok,
      emqtt:publish(C, <<"all">>, <<"hi">>, 0)),
    receive
        {publish, #{topic := <<"all">>}} -> ok
    after 2000 ->
              ?assert(false, "Publish to `all` should be allowed")
    end,
    ok = emqtt:disconnect(C).

t_check_jwt_acl_no_recs(init, _Config) ->
    application:set_env(emqx_auth_jwt, verify_claims, [{sub, <<"value">>}]).
t_check_jwt_acl_no_recs(_Config) ->
    Jwt = sign([{client_id, <<"client1">>},
                {username, <<"plain">>},
                {sub, value},
                {acl, []},
                {exp, erlang:system_time(seconds) + 10}],
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
                {exp, erlang:system_time(seconds) + 10}],
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

t_check_jwt_acl_no_jwt_claims_helper(_ClientInfo, _LastAuthResult) ->
    {stop, #{auth_result => success, anonymous => false}}.
t_check_jwt_acl_no_jwt_claims(init, _Config) ->
    ok;
t_check_jwt_acl_no_jwt_claims('end', _Config) ->
    ok = emqx_hooks:del(
            'client.authenticate',
            {?MODULE, t_check_jwt_acl_no_jwt_claims_helper, []}
          ).
t_check_jwt_acl_no_jwt_claims(_Config) ->
    %% bypass the jwt authentication checking
    ok = emqx_hooks:add(
            'client.authenticate',
            {?MODULE, t_check_jwt_acl_no_jwt_claims_helper, []},
            _Priority = 99999
          ),

    {ok, C} = emqtt:start_link(
                [{clean_start, true},
                 {proto_ver, v5},
                 {client_id, <<"client1">>},
                 {username, <<"client1">>},
                 {password, <<"password">>}]),
    {ok, _} = emqtt:connect(C),

    ok = snabbkaffe:start_trace(),

    ?assertMatch(
       {ok, #{}, [?RC_NOT_AUTHORIZED]},
       emqtt:subscribe(C, <<"a/b">>, 0)),

    {ok, _} = ?block_until(#{?snk_kind := no_jwt_claim}, 1000),
    Trace = snabbkaffe:collect_trace(),
    ?assertEqual(1, length(?of_kind(no_jwt_claim, Trace))),

    snabbkaffe:stop(),
    ok = emqtt:disconnect(C).

t_check_jwt_acl_expire(init, _Config) ->
    application:set_env(emqx_auth_jwt, verify_claims, [{sub, <<"value">>}]).
t_check_jwt_acl_expire(_Config) ->
    Jwt = sign([{client_id, <<"client1">>},
                {username, <<"plain">>},
                {sub, value},
                {acl, [{sub, [<<"a/b">>]}]},
                {exp, erlang:system_time(seconds) + 1}],
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

    Default = emqx_zone:get_env(external, acl_nomatch, deny),
    emqx_zone:set_env(external, acl_nomatch, allow),
    try
        ?assertMatch(
           {ok, #{}, [?RC_NOT_AUTHORIZED]},
           emqtt:subscribe(C, <<"a/b">>, 0))
    after
        emqx_zone:set_env(external, acl_nomatch, Default)
    end,

    ok = emqtt:disconnect(C).

t_check_jwt_acl_no_exp(init, _Config) ->
    application:set_env(emqx_auth_jwt, verify_claims, [{sub, <<"value">>}]).
t_check_jwt_acl_no_exp(_Config) ->
    Jwt = sign([{client_id, <<"client1">>},
                {username, <<"plain">>},
                {sub, value},
                {acl, [{sub, [<<"a/b">>]}]}],
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

    ok = emqtt:disconnect(C).

t_check_compatibility(init, _Config) -> ok.
t_check_compatibility(_Config) ->

    %% We literary want emqx_auth_jwt:check call emqx_auth_jwt:check_auth, so check with meck

    ok = meck:new(emqx_auth_jwt, [passthrough, no_history]),
    ok = meck:expect(emqx_auth_jwt, check_auth, fun(a, b, c) -> ok end),

    ?assertEqual(
       ok,
       emqx_auth_jwt:check(a, b, c)
      ),

    meck:validate(emqx_auth_jwt),
    meck:unload(emqx_auth_jwt).
