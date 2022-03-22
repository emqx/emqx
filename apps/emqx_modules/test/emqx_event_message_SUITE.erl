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

-module(emqx_event_message_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(EVENT_MESSAGE, <<
    ""
    "\n"
    "event_message: {\n"
    "    client_connected: true\n"
    "    client_disconnected: true\n"
    "    client_subscribed: true\n"
    "    client_unsubscribed: true\n"
    "    message_delivered: true\n"
    "    message_acked: true\n"
    "    message_dropped: true\n"
    "}"
    ""
>>).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_common_test_helpers:boot_modules(all),
    emqx_common_test_helpers:start_apps([emqx_modules]),
    ok = emqx_common_test_helpers:load_config(emqx_modules_schema, ?EVENT_MESSAGE),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:stop_apps([emqx_modules]).

t_event_topic(_) ->
    ok = emqx_event_message:enable(),
    {ok, C1} = emqtt:start_link([{clientid, <<"monsys">>}]),
    {ok, _} = emqtt:connect(C1),
    {ok, _, [?QOS_1]} = emqtt:subscribe(C1, <<"$event/client_connected">>, qos1),
    {ok, C2} = emqtt:start_link([
        {clientid, <<"clientid">>},
        {username, <<"username">>}
    ]),
    {ok, _} = emqtt:connect(C2),
    ok = recv_connected(<<"clientid">>),

    {ok, _, [?QOS_1]} = emqtt:subscribe(C1, <<"$event/client_subscribed">>, qos1),
    _ = receive_publish(100),
    timer:sleep(50),
    {ok, _, [?QOS_1]} = emqtt:subscribe(C2, <<"test_sub">>, qos1),
    ok = recv_subscribed(<<"clientid">>),
    emqtt:unsubscribe(C1, <<"$event/client_subscribed">>),
    timer:sleep(50),

    {ok, _, [?QOS_1]} = emqtt:subscribe(C1, <<"$event/message_delivered">>, qos1),
    {ok, _, [?QOS_1]} = emqtt:subscribe(C1, <<"$event/message_acked">>, qos1),
    _ = emqx:publish(emqx_message:make(<<"test">>, ?QOS_1, <<"test_sub">>, <<"test">>)),
    {ok, #{qos := QOS1, topic := Topic1}} = receive_publish(100),
    {ok, #{qos := QOS2, topic := Topic2}} = receive_publish(100),
    recv_message_publish_or_delivered(<<"clientid">>, QOS1, Topic1),
    recv_message_publish_or_delivered(<<"clientid">>, QOS2, Topic2),
    recv_message_acked(<<"clientid">>),

    {ok, _, [?QOS_1]} = emqtt:subscribe(C1, <<"$event/message_dropped">>, qos1),
    ok = emqtt:publish(C2, <<"test_sub1">>, <<"test">>),
    recv_message_dropped(<<"clientid">>),

    {ok, _, [?QOS_1]} = emqtt:subscribe(C1, <<"$event/client_unsubscribed">>, qos1),
    _ = emqtt:unsubscribe(C2, <<"test_sub">>),
    ok = recv_unsubscribed(<<"clientid">>),

    {ok, _, [?QOS_1]} = emqtt:subscribe(C1, <<"$event/client_disconnected">>, qos1),
    ok = emqtt:disconnect(C2),
    ok = recv_disconnected(<<"clientid">>),
    ok = emqtt:disconnect(C1),
    ok = emqx_event_message:disable().

t_reason(_) ->
    ?assertEqual(normal, emqx_event_message:reason(normal)),
    ?assertEqual(discarded, emqx_event_message:reason({shutdown, discarded})),
    ?assertEqual(tcp_error, emqx_event_message:reason({tcp_error, einval})),
    ?assertEqual(internal_error, emqx_event_message:reason(<<"unknown error">>)).

recv_connected(ClientId) ->
    {ok, #{qos := ?QOS_0, topic := Topic, payload := Payload}} = receive_publish(100),
    ?assertMatch(<<"$event/client_connected">>, Topic),
    ?assertMatch(
        #{
            <<"clientid">> := ClientId,
            <<"username">> := <<"username">>,
            <<"ipaddress">> := <<"127.0.0.1">>,
            <<"proto_name">> := <<"MQTT">>,
            <<"proto_ver">> := ?MQTT_PROTO_V4,
            <<"clean_start">> := true,
            <<"expiry_interval">> := 0,
            <<"keepalive">> := 60
        },
        emqx_json:decode(Payload, [return_maps])
    ).

recv_subscribed(_ClientId) ->
    {ok, #{qos := ?QOS_0, topic := Topic}} = receive_publish(100),
    ?assertMatch(<<"$event/client_subscribed">>, Topic).

recv_message_dropped(_ClientId) ->
    {ok, #{qos := ?QOS_0, topic := Topic}} = receive_publish(100),
    ?assertMatch(<<"$event/message_dropped">>, Topic).

recv_message_publish_or_delivered(_ClientId, 0, Topic) ->
    ?assertMatch(<<"$event/message_delivered">>, Topic);
recv_message_publish_or_delivered(_ClientId, 1, Topic) ->
    ?assertMatch(<<"test_sub">>, Topic).

recv_message_acked(_ClientId) ->
    {ok, #{qos := ?QOS_0, topic := Topic}} = receive_publish(100),
    ?assertMatch(<<"$event/message_acked">>, Topic).

recv_unsubscribed(_ClientId) ->
    {ok, #{qos := ?QOS_0, topic := Topic}} = receive_publish(100),
    ?assertMatch(<<"$event/client_unsubscribed">>, Topic).

recv_disconnected(ClientId) ->
    {ok, #{qos := ?QOS_0, topic := Topic, payload := Payload}} = receive_publish(100),
    ?assertMatch(<<"$event/client_disconnected">>, Topic),
    ?assertMatch(
        #{
            <<"clientid">> := ClientId,
            <<"username">> := <<"username">>,
            <<"reason">> := <<"normal">>
        },
        emqx_json:decode(Payload, [return_maps])
    ).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

receive_publish(Timeout) ->
    receive
        {publish, Publish} ->
            {ok, Publish}
    after Timeout -> {error, timeout}
    end.
