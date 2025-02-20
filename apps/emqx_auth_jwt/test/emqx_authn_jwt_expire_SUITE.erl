%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authn_jwt_expire_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx_auth/include/emqx_authn.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(PATH, [authentication]).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_testcase(_, Config) ->
    _ = emqx_authn_test_lib:delete_authenticators(?PATH, ?GLOBAL),
    Config.

end_per_testcase(_, _Config) ->
    _ = emqx_authn_test_lib:delete_authenticators(?PATH, ?GLOBAL),
    ok.

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            {emqx_conf, "authorization.no_match = deny, authorization.cache.enable = false"},
            emqx_auth,
            emqx_auth_jwt
        ],
        #{
            work_dir => ?config(priv_dir, Config)
        }
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_authn_test_lib:delete_authenticators(?PATH, ?GLOBAL),
    ok = emqx_cth_suite:stop(?config(apps, Config)),
    ok.

%%--------------------------------------------------------------------
%% CT cases
%%--------------------------------------------------------------------

t_jwt_expire(_Config) ->
    _ = process_flag(trap_exit, true),

    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, auth_config()}
    ),

    {ok, [#{provider := emqx_authn_jwt}]} = emqx_authn_chains:list_authenticators(?GLOBAL),

    Expire = erlang:system_time(second) + 3,

    Payload = #{
        <<"username">> => <<"myuser">>,
        <<"exp">> => Expire
    },
    JWS = emqx_authn_jwt_SUITE:generate_jws('hmac-based', Payload, <<"secret">>),

    {ok, C} = emqtt:start_link([{username, <<"myuser">>}, {password, JWS}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(C),

    receive
        {disconnected, ?RC_NOT_AUTHORIZED, #{}} ->
            ?assert(erlang:system_time(second) >= Expire)
    after 5000 ->
        ct:fail("Client should be disconnected by timeout")
    end.

t_will_message_on_auth_expire(_Config) ->
    _ = process_flag(trap_exit, true),

    %% Set up the authenticator
    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, auth_config()}
    ),
    {ok, [#{provider := emqx_authn_jwt}]} = emqx_authn_chains:list_authenticators(?GLOBAL),

    %% Set up the subscriber, it will receive the will message
    Now = erlang:system_time(second),
    WillTopic = <<"t/will">>,
    PayloadSub = #{
        <<"username">> => <<"subuser">>,
        <<"exp">> => Now + 100
    },
    JWSSub = emqx_authn_jwt_SUITE:generate_jws('hmac-based', PayloadSub, <<"secret">>),
    {ok, SubClient} = emqtt:start_link([
        {username, <<"subuser">>}, {password, JWSSub}, {proto_ver, v5}
    ]),
    {ok, _} = emqtt:connect(SubClient),
    {ok, _, _} = emqtt:subscribe(SubClient, WillTopic, 1),

    %% Set up the publisher, it will publish the will message on auth expire
    WillPayload = <<"will">>,
    PayloadPub = #{
        <<"username">> => <<"pubuser">>,
        <<"exp">> => Now + 3,
        <<"acl">> => [
            #{
                <<"permission">> => <<"allow">>,
                <<"action">> => <<"pub">>,
                <<"topics">> => [WillTopic]
            }
        ]
    },
    JWSPub = emqx_authn_jwt_SUITE:generate_jws('hmac-based', PayloadPub, <<"secret">>),
    {ok, PubClient} = emqtt:start_link([
        {username, <<"pubuser">>},
        {password, JWSPub},
        {proto_ver, v5},
        {will_flag, true},
        {will_topic, WillTopic},
        {will_payload, WillPayload},
        {will_retain, false}
    ]),
    {ok, _} = emqtt:connect(PubClient),

    %% Check that the will message is received
    receive
        {publish, #{payload := Payload}} when Payload =:= WillPayload -> ok
    after 5000 ->
        ct:fail("Will message not received")
    end,

    %% Clean up
    ok = emqtt:disconnect(SubClient).

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

auth_config() ->
    #{
        <<"use_jwks">> => false,
        <<"algorithm">> => <<"hmac-based">>,
        <<"acl_claim_name">> => <<"acl">>,
        <<"secret">> => <<"secret">>,
        <<"mechanism">> => <<"jwt">>,
        <<"verify_claims">> => #{<<"username">> => <<"${username}">>}
        %% Should be enabled by default
        %% <<"disconnect_after_expire">> => true
    }.
