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

-module(mqtt_protocol_v5_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(lists, [nth/2]).

-define(TOPICS, [<<"TopicA">>, <<"TopicA/B">>, <<"Topic/C">>, <<"TopicA/C">>,
                 <<"/TopicA">>]).

-define(WILD_TOPICS, [<<"TopicA/+">>, <<"+/C">>, <<"#">>, <<"/#">>, <<"/+">>,
                      <<"+/+">>, <<"TopicA/#">>]).

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    %% Meck emqtt
    ok = meck:new(emqtt, [non_strict, passthrough, no_history, no_link]),
    %% Start Apps
    emqx_ct_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    ok = meck:unload(emqtt),
    emqx_ct_helpers:stop_apps([]).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

receive_messages(Count) ->
    receive_messages(Count, []).

receive_messages(0, Msgs) ->
    Msgs;
receive_messages(Count, Msgs) ->
    receive
        {publish, Msg} ->
            receive_messages(Count-1, [Msg|Msgs]);
        _Other ->
            receive_messages(Count, Msgs)
    after 100 ->
        Msgs
    end.

clean_retained(Topic) ->
    {ok, Clean} = emqtt:start_link([{clean_start, true}]),
    {ok, _} = emqtt:connect(Clean),
    {ok, _} = emqtt:publish(Clean, Topic, #{}, <<"">>, [{qos, ?QOS_1}, {retain, true}]),
    ok = emqtt:disconnect(Clean).

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------

t_basic_test(_) ->
    Topic = nth(1, ?TOPICS),
    ct:print("Basic test starting"),
    {ok, C} = emqtt:start_link([{proto_ver, v5}]),
    {ok, _} = emqtt:connect(C),
    {ok, _, [1]} = emqtt:subscribe(C, Topic, qos1),
    {ok, _, [2]} = emqtt:subscribe(C, Topic, qos2),
    {ok, _} = emqtt:publish(C, Topic, <<"qos 2">>, 2),
    {ok, _} = emqtt:publish(C, Topic, <<"qos 2">>, 2),
    {ok, _} = emqtt:publish(C, Topic, <<"qos 2">>, 2),
    ?assertEqual(3, length(receive_messages(3))),
    ok = emqtt:disconnect(C).

%%--------------------------------------------------------------------
%% Connection
%%--------------------------------------------------------------------

t_connect_idle_timeout(_) ->
    IdleTimeout = 2,
    emqx_zone:set_env(external, idle_timeout, IdleTimeout),

    {ok, Sock} = emqtt_sock:connect({127,0,0,1}, 1883, [], 60000),
    timer:sleep(timer:seconds(IdleTimeout)),
    ?assertMatch({error, closed}, emqtt_sock:recv(Sock,1024)).

t_connect_limit_timeout(_) ->
    ok = meck:new(proplists, [non_strict, passthrough, no_history, no_link, unstick]),
    meck:expect(proplists, get_value, fun(active_n, _Options, _Default) -> 1;
                                        (Arg1, ARg2, Arg3) -> meck:passthrough([Arg1, ARg2, Arg3])
                                                            end),

    Topic = nth(1, ?TOPICS),
    emqx_zone:set_env(external, publish_limit, {2.0, 3}),

    {ok, Client} = emqtt:start_link([{clientid, <<"t_connect_limit_timeout">>},{proto_ver, v5},{keepalive, 60}]),
    {ok, _} = emqtt:connect(Client),
    [ClientPid] = emqx_cm:lookup_channels(<<"t_connect_limit_timeout">>),

    ?assertEqual(undefined, emqx_connection:info(limit_timer, sys:get_state(ClientPid))),
    ok = emqtt:publish(Client, Topic, <<"t_shared_subscriptions_client_terminates_when_qos_eq_2">>, 0),
    ok = emqtt:publish(Client, Topic, <<"t_shared_subscriptions_client_terminates_when_qos_eq_2">>, 0),
    ok = emqtt:publish(Client, Topic, <<"t_shared_subscriptions_client_terminates_when_qos_eq_2">>, 0),
    ok = emqtt:publish(Client, Topic, <<"t_shared_subscriptions_client_terminates_when_qos_eq_2">>, 0),
    timer:sleep(200),
    ?assert(is_reference(emqx_connection:info(limit_timer, sys:get_state(ClientPid)))),

    emqtt:disconnect(Client),
    meck:unload(proplists).

t_connect_emit_stats_timeout(_) ->
    IdleTimeout = 10,
    emqx_zone:set_env(external, idle_timeout, IdleTimeout),

    {ok, Client} = emqtt:start_link([{clientid, <<"t_connect_emit_stats_timeout">>},{proto_ver, v5},{keepalive, 60}]),
    {ok, _} = emqtt:connect(Client),
    [ClientPid] = emqx_cm:lookup_channels(<<"t_connect_emit_stats_timeout">>),
    
    ?assertEqual(undefined, emqx_connection:info(stats_timer, sys:get_state(ClientPid))),
    pong = emqtt:ping(Client),
    ?assert(is_reference(emqx_connection:info(stats_timer, sys:get_state(ClientPid)))),
    timer:sleep(timer:seconds(IdleTimeout)),
    ?assertEqual(undefined, emqx_connection:info(stats_timer, sys:get_state(ClientPid))),
    emqtt:disconnect(Client).

%% [MQTT-3.1.2-22]
t_connect_keepalive_timeout(_) ->
    Keepalive = 2,

    {ok, Client} = emqtt:start_link([{proto_ver, v5},
                                    {keepalive, Keepalive}]),
    {ok, _} = emqtt:connect(Client),
    emqtt:pause(Client),
    receive
        Msg -> 
            ReasonCode = 141,
            ?assertMatch({disconnected, ReasonCode, _Channel}, Msg)
    after 
        round(timer:seconds(Keepalive) * 2 * 1.5 ) -> error("keepalive timeout")
    end.

%%--------------------------------------------------------------------
%% Shared Subscriptions
%%--------------------------------------------------------------------

t_shared_subscriptions_client_terminates_when_qos_eq_2(_) ->
    process_flag(trap_exit, true),
    application:set_env(emqx, shared_dispatch_ack_enabled, true),

    Topic = nth(1, ?TOPICS),
    Shared_topic = list_to_binary("$share/sharename/" ++ binary_to_list(<<"TopicA">>)),

    CRef = counters:new(1, [atomics]),
    meck:expect(emqtt, connected, 
                fun(cast, ?PUBLISH_PACKET(?QOS_2, _PacketId), _State) -> 
                                        ok = counters:add(CRef, 1, 1),
                                        {stop, {shutdown, for_testiong}};
                    (Arg1, ARg2, Arg3) -> meck:passthrough([Arg1, ARg2, Arg3])
                                        end),

    {ok, Sub1} = emqtt:start_link([{proto_ver, v5},
                                    {clientid, <<"sub_client_1">>},
                                    {keepalive, 5}]),
    {ok, _} = emqtt:connect(Sub1),
    {ok, _, [2]} = emqtt:subscribe(Sub1, Shared_topic, qos2),
    {ok, Sub2} = emqtt:start_link([{proto_ver, v5},
                                    {clientid, <<"sub_client_2">>},
                                    {keepalive, 5}]),
    {ok, _} = emqtt:connect(Sub2),
    {ok, _, [2]} = emqtt:subscribe(Sub2, Shared_topic, qos2),

    {ok, Pub} = emqtt:start_link([{proto_ver, v5}, {clientid, <<"pub_client">>}]),
    {ok, _} = emqtt:connect(Pub),
    {ok, _} = emqtt:publish(Pub, Topic, <<"t_shared_subscriptions_client_terminates_when_qos_eq_2">>, 2),

    receive
        {disconnected,shutdown,for_testiong} -> ok
    end,
    ?assertEqual(1, counters:get(CRef, 1)).
