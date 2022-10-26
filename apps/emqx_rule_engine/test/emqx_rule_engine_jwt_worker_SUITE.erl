%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_rule_engine_jwt_worker_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx_rule_engine/include/rule_engine.hrl").
-include_lib("jose/include/jose_jwt.hrl").
-include_lib("jose/include/jose_jws.hrl").

-compile([export_all, nowarn_export_all]).

%%-----------------------------------------------------------------------------
%% CT boilerplate
%%-----------------------------------------------------------------------------

all() ->
    emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

%%-----------------------------------------------------------------------------
%% Helper fns
%%-----------------------------------------------------------------------------

generate_private_key_pem() ->
    PublicExponent = 65537,
    Size = 2048,
    Key = public_key:generate_key({rsa, Size, PublicExponent}),
    DERKey = public_key:der_encode('PrivateKeyInfo', Key),
    public_key:pem_encode([{'PrivateKeyInfo', DERKey, not_encrypted}]).

generate_config() ->
    PrivateKeyPEM = generate_private_key_pem(),
    ResourceID = emqx_guid:gen(),
    #{ private_key => PrivateKeyPEM
     , expiration => timer:hours(1)
     , resource_id => ResourceID
     , table => ets:new(test_jwt_table, [ordered_set, public])
     , iss => <<"issuer">>
     , sub => <<"subject">>
     , aud => <<"audience">>
     , kid => <<"key id">>
     , alg => <<"RS256">>
     }.

is_expired(JWT) ->
    #jose_jwt{fields = #{<<"exp">> := Exp}} = jose_jwt:peek(JWT),
    Now = erlang:system_time(seconds),
    Now >= Exp.

%%-----------------------------------------------------------------------------
%% Test cases
%%-----------------------------------------------------------------------------

t_create_success(_Config) ->
    Ref = alias([reply]),
    Config = generate_config(),
    ?assertMatch({ok, _}, emqx_rule_engine_jwt_worker:start_link(Config, Ref)),
    receive
        {Ref, token_created} ->
            ok
    after
        1_000 ->
            ct:fail("should have confirmed token creation; msgs: ~0p",
                    [process_info(self(), messages)])
    end,
    ok.

t_empty_key(_Config) ->
    Ref = alias([reply]),
    Config0 = generate_config(),
    Config = Config0#{private_key := <<>>},
    process_flag(trap_exit, true),
    ?assertMatch({ok, _}, emqx_rule_engine_jwt_worker:start_link(Config, Ref)),
    receive
        {Ref, {error, {invalid_private_key, empty_key}}} ->
            ok
    after
        1_000 ->
            ct:fail("should have errored; msgs: ~0p",
                    [process_info(self(), messages)])
    end,
    ok.

t_invalid_pem(_Config) ->
    Ref = alias([reply]),
    Config0 = generate_config(),
    InvalidPEM = public_key:pem_encode([{'PrivateKeyInfo', <<"xxxxxx">>, not_encrypted},
                                        {'PrivateKeyInfo', <<"xxxxxx">>, not_encrypted}]),
    Config = Config0#{private_key := InvalidPEM},
    process_flag(trap_exit, true),
    ?assertMatch({ok, _}, emqx_rule_engine_jwt_worker:start_link(Config, Ref)),
    receive
        {Ref, {error, {invalid_private_key, _}}} ->
            ok
    after
        1_000 ->
            ct:fail("should have errored; msgs: ~0p",
                    [process_info(self(), messages)])
    end,
    ok.

t_refresh(_Config) ->
    Ref = alias([reply]),
    Config0 = #{ table := Table
               , resource_id := ResourceId
               } = generate_config(),
    Config = Config0#{expiration => 5_000},
    ?check_trace(
       begin
         {{ok, _Pid}, {ok, _Event}} =
           ?wait_async_action(
              emqx_rule_engine_jwt_worker:start_link(Config, Ref),
              #{?snk_kind := jwt_worker_token_stored},
              5_000),
         {ok, FirstJWT} = emqx_rule_engine_jwt_worker:lookup_jwt(Table, ResourceId),
         ?block_until(#{?snk_kind := rule_engine_jwt_worker_refresh}, 15_000),
         {ok, SecondJWT} = emqx_rule_engine_jwt_worker:lookup_jwt(Table, ResourceId),
         ?assertNot(is_expired(SecondJWT)),
         ?assert(is_expired(FirstJWT)),
         {FirstJWT, SecondJWT}
       end,
       fun({FirstJWT, SecondJWT}, Trace) ->
         ?assertMatch([_, _ | _],
                      ?of_kind(jwt_worker_token_stored, Trace)),
         ?assertNotEqual(FirstJWT, SecondJWT),
         ok
       end),
    ok.

t_format_status(_Config) ->
    Ref = alias([reply]),
    Config = generate_config(),
    {ok, Pid} = emqx_rule_engine_jwt_worker:start_link(Config, Ref),
    {status, _, _, Props} = sys:get_status(Pid),
    [State] = [State
               || Info = [_ | _] <- Props,
                  {data, Data = [_ | _]} <- Info,
                  {"State", State} <- Data],
    ?assertMatch(
      #{ jwt := "******"
       , jwk := "******"
       },
       State),
    ok.

t_lookup_ok(_Config) ->
    Ref = alias([reply]),
    Config = #{ table := Table
              , resource_id := ResourceId
              , private_key := PrivateKeyPEM
              , aud := Aud
              , iss := Iss
              , sub := Sub
              , kid := KId
              } = generate_config(),
    {ok, _} = emqx_rule_engine_jwt_worker:start_link(Config, Ref),
    receive
        {Ref, token_created} ->
            ok
    after
        500 ->
            error(timeout)
    end,
    Res = emqx_rule_engine_jwt_worker:lookup_jwt(Table, ResourceId),
    ?assertMatch({ok, _}, Res),
    {ok, JWT} = Res,
    ?assert(is_binary(JWT)),
    JWK = jose_jwk:from_pem(PrivateKeyPEM),
    {IsValid, ParsedJWT, JWS} = jose_jwt:verify_strict(JWK, [<<"RS256">>], JWT),
    ?assertMatch(
      #jose_jwt{
         fields = #{ <<"aud">> := Aud
                   , <<"iss">> := Iss
                   , <<"sub">> := Sub
                   , <<"exp">> := _
                   , <<"iat">> := _
                   }},
      ParsedJWT),
    ?assertNot(is_expired(JWT)),
    ?assertMatch(
      #jose_jws{
         alg = {_, 'RS256'},
         fields = #{ <<"kid">> := KId
                   , <<"typ">> := <<"JWT">>
                   }},
      JWS),
    ?assert(IsValid),
    ok.

t_lookup_not_found(_Config) ->
    Table = ets:new(test_jwt_table, [ordered_set, public]),
    InexistentResource = <<"xxx">>,
    ?assertEqual({error, not_found},
                 emqx_rule_engine_jwt_worker:lookup_jwt(Table, InexistentResource)),
    ok.

t_lookup_badarg(_Config) ->
    InexistentTable = i_dont_exist,
    InexistentResource = <<"xxx">>,
    ?assertEqual({error, not_found},
                 emqx_rule_engine_jwt_worker:lookup_jwt(InexistentTable, InexistentResource)),
    ok.

t_start_supervised_worker(_Config) ->
    {ok, _} = emqx_rule_engine_jwt_sup:start_link(),
    Config = #{resource_id := ResourceId} = generate_config(),
    {Ref, Pid} = emqx_rule_engine_jwt_sup:start_worker(ResourceId, Config),
    receive
        {Ref, token_created} ->
            ok
    after
        1_000 ->
            ct:fail("timeout")
    end,
    MRef = monitor(process, Pid),
    ?assert(is_process_alive(Pid)),
    ok = emqx_rule_engine_jwt_sup:stop_worker(ResourceId),
    receive
        {'DOWN', MRef, process, Pid, _} ->
            ok
    after
        1_000 ->
            ct:fail("timeout")
    end,
    ok.
