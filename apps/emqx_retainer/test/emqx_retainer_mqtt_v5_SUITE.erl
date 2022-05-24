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

-module(emqx_retainer_mqtt_v5_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_retainer_SUITE:load_conf(),
    %% Start Apps
    emqx_common_test_helpers:start_apps([emqx_retainer]),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:stop_apps([emqx_retainer]).

client_info(Key, Client) ->
    maps:get(Key, maps:from_list(emqtt:info(Client)), undefined).

receive_messages(Count) ->
    receive_messages(Count, []).

receive_messages(0, Msgs) ->
    Msgs;
receive_messages(Count, Msgs) ->
    receive
        {publish, Msg} ->
            receive_messages(Count - 1, [Msg | Msgs]);
        _Other ->
            receive_messages(Count, Msgs)
    after 300 ->
        Msgs
    end.

receive_disconnect_reasoncode() ->
    receive
        {disconnected, ReasonCode, _} -> ReasonCode;
        _Other -> receive_disconnect_reasoncode()
    after 100 ->
        error("no disconnect packet")
    end.

clean_retained(Topic) ->
    {ok, Clean} = emqtt:start_link([{clean_start, true}]),
    {ok, _} = emqtt:connect(Clean),
    {ok, _} = emqtt:publish(Clean, Topic, #{}, <<"">>, [{qos, qos2}, {retain, true}]),
    ok = emqtt:disconnect(Clean).

%%--------------------------------------------------------------------
%% Publish
%%--------------------------------------------------------------------

t_publish_retain_message(_) ->
    Topic = <<"Topic/A">>,

    {ok, Client1} = emqtt:start_link([{proto_ver, v5}]),
    {ok, _} = emqtt:connect(Client1),
    {ok, _} = emqtt:publish(
        Client1,
        Topic,
        #{},
        <<"retained message">>,
        [{qos, 2}, {retain, true}]
    ),
    {ok, _} = emqtt:publish(
        Client1,
        Topic,
        #{},
        <<"new retained message">>,
        [{qos, 2}, {retain, true}]
    ),
    {ok, _} = emqtt:publish(
        Client1,
        Topic,
        #{},
        <<"not retained message">>,
        [{qos, 2}, {retain, false}]
    ),
    {ok, _, [2]} = emqtt:subscribe(Client1, Topic, 2),

    [Msg] = receive_messages(3),
    %% [MQTT-3.3.1-5] [MQTT-3.3.1-8]
    ?assertEqual(<<"new retained message">>, maps:get(payload, Msg)),

    {ok, _, [0]} = emqtt:unsubscribe(Client1, Topic),
    {ok, _} = emqtt:publish(Client1, Topic, #{}, <<"">>, [{qos, 2}, {retain, true}]),
    {ok, _, [2]} = emqtt:subscribe(Client1, Topic, 2),

    %% [MQTT-3.3.1-6] [MQTT-3.3.1-7]
    ?assertEqual(0, length(receive_messages(1))),

    ok = emqtt:disconnect(Client1).

t_publish_message_expiry_interval(_) ->
    {ok, Client1} = emqtt:start_link([{proto_ver, v5}]),
    {ok, _} = emqtt:connect(Client1),
    {ok, _} = emqtt:publish(
        Client1,
        <<"topic/A">>,
        #{'Message-Expiry-Interval' => 1},
        <<"retained message">>,
        [{qos, 1}, {retain, true}]
    ),
    {ok, _} = emqtt:publish(
        Client1,
        <<"topic/B">>,
        #{'Message-Expiry-Interval' => 1},
        <<"retained message">>,
        [{qos, 2}, {retain, true}]
    ),
    {ok, _} = emqtt:publish(
        Client1,
        <<"topic/C">>,
        #{'Message-Expiry-Interval' => 10},
        <<"retained message">>,
        [{qos, 1}, {retain, true}]
    ),
    {ok, _} = emqtt:publish(
        Client1,
        <<"topic/D">>,
        #{'Message-Expiry-Interval' => 10},
        <<"retained message">>,
        [{qos, 2}, {retain, true}]
    ),
    timer:sleep(1500),
    {ok, _, [2]} = emqtt:subscribe(Client1, <<"topic/+">>, 2),
    Msgs = receive_messages(6),
    %% [MQTT-3.3.2-5]
    ?assertEqual(2, length(Msgs)),

    L = lists:map(
        fun(Msg) ->
            MessageExpiryInterval = maps:get(
                'Message-Expiry-Interval',
                maps:get(properties, Msg)
            ),
            MessageExpiryInterval < 10
        end,
        Msgs
    ),
    %% [MQTT-3.3.2-6]
    ?assertEqual(2, length(L)),

    ok = emqtt:disconnect(Client1),
    clean_retained(<<"topic/C">>),
    clean_retained(<<"topic/D">>).

%%--------------------------------------------------------------------
%% Subsctibe
%%--------------------------------------------------------------------

t_subscribe_retain_handing(_) ->
    {ok, Client1} = emqtt:start_link([{proto_ver, v5}]),
    {ok, _} = emqtt:connect(Client1),
    ok = emqtt:publish(
        Client1,
        <<"topic/A">>,
        #{},
        <<"retained message">>,
        [{qos, 0}, {retain, true}]
    ),
    {ok, _} = emqtt:publish(
        Client1,
        <<"topic/B">>,
        #{},
        <<"retained message">>,
        [{qos, 1}, {retain, true}]
    ),
    {ok, _} = emqtt:publish(
        Client1,
        <<"topic/C">>,
        #{},
        <<"retained message">>,
        [{qos, 2}, {retain, true}]
    ),

    timer:sleep(200),

    {ok, _, [2]} = emqtt:subscribe(Client1, #{}, [{<<"topic/+">>, [{rh, 1}, {qos, 2}]}]),
    %% [MQTT-3.3.1-10]
    ?assertEqual(3, length(receive_messages(3))),

    {ok, _, [2]} = emqtt:subscribe(Client1, #{}, [{<<"topic/+">>, [{rh, 2}, {qos, 2}]}]),
    %% [MQTT-3.3.1-11]
    ?assertEqual(0, length(receive_messages(3))),

    {ok, _, [2]} = emqtt:subscribe(Client1, #{}, [{<<"topic/+">>, [{rh, 0}, {qos, 2}]}]),
    %% [MQTT-3.3.1-9]
    ?assertEqual(3, length(receive_messages(3))),

    {ok, _, [2]} = emqtt:subscribe(Client1, #{}, [{<<"topic/+">>, [{rh, 1}, {qos, 2}]}]),
    %% [MQTT-3.3.1-10]
    ?assertEqual(0, length(receive_messages(3))),

    {ok, _, [2]} = emqtt:subscribe(Client1, #{}, [{<<"topic/+">>, [{rh, 0}, {qos, 2}]}]),
    %% [MQTT-3.8.4-4]
    ?assertEqual(3, length(receive_messages(3))),

    ok = emqtt:disconnect(Client1),
    clean_retained(<<"topic/A">>),
    clean_retained(<<"topic/B">>),
    clean_retained(<<"topic/C">>).
