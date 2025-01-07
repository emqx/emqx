%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_connector_jwt_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("jose/include/jose_jwt.hrl").
-include_lib("jose/include/jose_jws.hrl").
-include("emqx_connector_jwt_tables.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-compile([export_all, nowarn_export_all]).

%%-----------------------------------------------------------------------------
%% CT boilerplate
%%-----------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx_connector_jwt], #{work_dir => emqx_cth_suite:work_dir(Config)}),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ets:delete_all_objects(?JWT_TABLE),
    ok.

%%-----------------------------------------------------------------------------
%% Helper fns
%%-----------------------------------------------------------------------------

insert_jwt(TId, ResourceId, JWT) ->
    ets:insert(TId, {{ResourceId, jwt}, JWT}).

generate_private_key_pem() ->
    PublicExponent = 65537,
    Size = 2048,
    Key = public_key:generate_key({rsa, Size, PublicExponent}),
    DERKey = public_key:der_encode('PrivateKeyInfo', Key),
    public_key:pem_encode([{'PrivateKeyInfo', DERKey, not_encrypted}]).

generate_config() ->
    PrivateKeyPEM = generate_private_key_pem(),
    ResourceID = emqx_guid:gen(),
    #{
        private_key => PrivateKeyPEM,
        expiration => timer:hours(1),
        resource_id => ResourceID,
        table => ets:new(test_jwt_table, [ordered_set, public]),
        iss => <<"issuer">>,
        sub => <<"subject">>,
        aud => <<"audience">>,
        kid => <<"key id">>,
        alg => <<"RS256">>
    }.

is_expired(JWT) ->
    #jose_jwt{fields = #{<<"exp">> := Exp}} = jose_jwt:peek(JWT),
    Now = erlang:system_time(seconds),
    Now >= Exp.

%%-----------------------------------------------------------------------------
%% Test cases
%%-----------------------------------------------------------------------------

t_lookup_jwt_ok(_Config) ->
    TId = ?JWT_TABLE,
    JWT = <<"some jwt">>,
    ResourceId = <<"resource id">>,
    true = insert_jwt(TId, ResourceId, JWT),
    ?assertEqual({ok, JWT}, emqx_connector_jwt:lookup_jwt(ResourceId)),
    ok.

t_lookup_jwt_missing(_Config) ->
    ResourceId = <<"resource id">>,
    ?assertEqual({error, not_found}, emqx_connector_jwt:lookup_jwt(ResourceId)),
    ok.

t_delete_jwt(_Config) ->
    TId = ?JWT_TABLE,
    JWT = <<"some jwt">>,
    ResourceId = <<"resource id">>,
    true = insert_jwt(TId, ResourceId, JWT),
    {ok, _} = emqx_connector_jwt:lookup_jwt(ResourceId),
    ?assertEqual(ok, emqx_connector_jwt:delete_jwt(TId, ResourceId)),
    ?assertEqual({error, not_found}, emqx_connector_jwt:lookup_jwt(TId, ResourceId)),
    ok.

t_ensure_jwt(_Config) ->
    Config0 =
        #{
            table := Table,
            resource_id := ResourceId,
            private_key := PrivateKeyPEM
        } = generate_config(),
    JWK = jose_jwk:from_pem(PrivateKeyPEM),
    Config1 = maps:without([private_key], Config0),
    Expiration = timer:seconds(10),
    JWTConfig = Config1#{jwk => JWK, expiration := Expiration},
    ?assertEqual({error, not_found}, emqx_connector_jwt:lookup_jwt(Table, ResourceId)),
    ?check_trace(
        begin
            JWT0 = emqx_connector_jwt:ensure_jwt(JWTConfig),
            ?assertNot(is_expired(JWT0)),
            %% should refresh 5 s before expiration
            ct:sleep(Expiration - 3000),
            JWT1 = emqx_connector_jwt:ensure_jwt(JWTConfig),
            ?assertNot(is_expired(JWT1)),
            %% fully expired
            ct:sleep(2 * Expiration),
            JWT2 = emqx_connector_jwt:ensure_jwt(JWTConfig),
            ?assertNot(is_expired(JWT2)),
            {JWT0, JWT1, JWT2}
        end,
        fun({JWT0, JWT1, JWT2}, Trace) ->
            ?assertNotEqual(JWT0, JWT1),
            ?assertNotEqual(JWT1, JWT2),
            ?assertNotEqual(JWT2, JWT0),
            ?assertMatch([_, _, _], ?of_kind(emqx_connector_jwt_token_stored, Trace)),
            ok
        end
    ),
    ok.
