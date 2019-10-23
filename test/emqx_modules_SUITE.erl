%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_modules_SUITE).

%% API
-compile(export_all).
-compile(nowarn_export_all).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").

%%-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%-define(PROPTEST(M,F), true = proper:quickcheck(M:F())).

-define(RULES, [{rewrite,<<"x/#">>,<<"^x/y/(.+)$">>,<<"z/y/$1">>},
                {rewrite,<<"y/+/z/#">>,<<"^y/(.+)/z/(.+)$">>,<<"y/z/$2">>}
               ]).

all() -> emqx_ct:all(?MODULE).

suite() ->
    [{ct_hooks,[cth_surefire]}, {timetrap, {seconds, 30}}].

init_per_suite(Config) ->
    emqx_ct_helpers:boot_modules(all),
    emqx_ct_helpers:start_apps([emqx]),
    %% Ensure all the modules unloaded.
    ok = emqx_modules:unload(),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([emqx]).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_unload(_) ->
    error('TODO').

t_load(_) ->
    error('TODO').


%% Test case for emqx_mod_presence
t_mod_presence(_) ->
    ok = emqx_mod_presence:load([{qos, ?QOS_1}]),
    {ok, C1} = emqtt:start_link([{clientid, <<"monsys">>}]),
    {ok, _} = emqtt:connect(C1),
    {ok, _Props, [?QOS_1]} = emqtt:subscribe(C1, <<"$SYS/brokers/+/clients/#">>, qos1),
    %% Connected Presence
    {ok, C2} = emqtt:start_link([{clientid, <<"clientid">>},
                                 {username, <<"username">>}]),
    {ok, _} = emqtt:connect(C2),
    ok = recv_and_check_presence(<<"clientid">>, <<"connected">>),
    %% Disconnected Presence
    ok = emqtt:disconnect(C2),
    ok = recv_and_check_presence(<<"clientid">>, <<"disconnected">>),
    ok = emqtt:disconnect(C1),
    ok = emqx_mod_presence:unload([{qos, ?QOS_1}]).

t_mod_presence_reason(_) ->
    ?assertEqual(normal, emqx_mod_presence:reason(normal)),
    ?assertEqual(discarded, emqx_mod_presence:reason({shutdown, discarded})),
    ?assertEqual(tcp_error, emqx_mod_presence:reason({tcp_error, einval})),
    ?assertEqual(internal_error, emqx_mod_presence:reason(<<"unknown error">>)).

recv_and_check_presence(ClientId, Presence) ->
    {ok, #{qos := ?QOS_1, topic := Topic, payload := Payload}} = receive_publish(100),
    ?assertMatch([<<"$SYS">>, <<"brokers">>, _Node, <<"clients">>, ClientId, Presence],
                 binary:split(Topic, <<"/">>, [global])),
    case Presence of
        <<"connected">> ->
            ?assertMatch(#{clientid := <<"clientid">>,
                           username := <<"username">>,
                           ipaddress := <<"127.0.0.1">>,
                           proto_name := <<"MQTT">>,
                           proto_ver := ?MQTT_PROTO_V4,
                           connack := ?RC_SUCCESS,
                           clean_start := true}, emqx_json:decode(Payload, [{labels, atom}, return_maps]));
        <<"disconnected">> ->
            ?assertMatch(#{clientid := <<"clientid">>,
                           username := <<"username">>,
                           reason := <<"normal">>}, emqx_json:decode(Payload, [{labels, atom}, return_maps]))
    end.

%% Test case for emqx_mod_subscription
t_mod_subscription(_) ->
    emqx_mod_subscription:load([{<<"connected/%c/%u">>, ?QOS_0}]),
    {ok, C} = emqtt:start_link([{host, "localhost"},
                                {clientid, "myclient"},
                                {username, "admin"}]),
    {ok, _} = emqtt:connect(C),
    emqtt:publish(C, <<"connected/myclient/admin">>, <<"Hello world">>, ?QOS_0),
    {ok, #{topic := Topic, payload := Payload}} = receive_publish(100),
    ?assertEqual(<<"connected/myclient/admin">>, Topic),
    ?assertEqual(<<"Hello world">>, Payload),
    ok = emqtt:disconnect(C),
    emqx_mod_subscription:unload([]).

%% Test case for emqx_mod_write
t_mod_rewrite(_Config) ->
    ok = emqx_mod_rewrite:load(?RULES),
    {ok, C} = emqtt:start_link([{clientid, <<"rewrite_client">>}]),
    {ok, _} = emqtt:connect(C),
    OrigTopics = [<<"x/y/2">>, <<"x/1/2">>, <<"y/a/z/b">>, <<"y/def">>],
    DestTopics = [<<"z/y/2">>, <<"x/1/2">>, <<"y/z/b">>, <<"y/def">>],
    %% Subscribe
    {ok, _Props, _} = emqtt:subscribe(C, [{Topic, ?QOS_1} || Topic <- OrigTopics]),
    timer:sleep(100),
    Subscriptions = emqx_broker:subscriptions(<<"rewrite_client">>),
    ?assertEqual(DestTopics, [Topic || {Topic, _SubOpts} <- Subscriptions]),
    %% Publish
    RecvTopics = [begin
                      ok = emqtt:publish(C, Topic, <<"payload">>),
                      {ok, #{topic := RecvTopic}} = receive_publish(100),
                      RecvTopic
                  end || Topic <- OrigTopics],
    ?assertEqual(DestTopics, RecvTopics),
    %% Unsubscribe
    {ok, _, _} = emqtt:unsubscribe(C, OrigTopics),
    timer:sleep(100),
    ?assertEqual([], emqx_broker:subscriptions(<<"rewrite_client">>)),
    ok = emqtt:disconnect(C),
    ok = emqx_mod_rewrite:unload(?RULES).

t_rewrite_rule(_Config) ->
    Rules = emqx_mod_rewrite:compile(?RULES),
    ?assertEqual(<<"z/y/2">>, emqx_mod_rewrite:match_and_rewrite(<<"x/y/2">>, Rules)),
    ?assertEqual(<<"x/1/2">>, emqx_mod_rewrite:match_and_rewrite(<<"x/1/2">>, Rules)),
    ?assertEqual(<<"y/z/b">>, emqx_mod_rewrite:match_and_rewrite(<<"y/a/z/b">>, Rules)),
    ?assertEqual(<<"y/def">>, emqx_mod_rewrite:match_and_rewrite(<<"y/def">>, Rules)).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

receive_publish(Timeout) ->
    receive
        {publish, Publish} -> {ok, Publish}
    after
        Timeout -> {error, timeout}
    end.

