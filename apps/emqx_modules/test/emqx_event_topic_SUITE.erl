%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_event_topic_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:boot_modules(all),
    emqx_ct_helpers:start_apps([emqx_modules]),
    meck:new(emqx_schema, [non_strict, passthrough, no_history, no_link]),
    meck:expect(emqx_schema, includes, fun() -> ["event_topic"] end ),
    meck:expect(emqx_schema, extra_schema_fields, fun(FieldName) -> emqx_modules_schema:fields(FieldName) end),
    ok = emqx_config:update([event_topic, topics], [<<"$event/client_connected">>,
                                                    <<"$event/client_disconnected">>,
                                                    <<"$event/session_subscribed">>,
                                                    <<"$event/session_unsubscribed">>,
                                                    <<"$event/message_delivered">>,
                                                    <<"$event/message_acked">>,
                                                    <<"$event/message_dropped">>]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([emqx_modules]),
    meck:unload(emqx_schema).

t_event_topic(_) ->
    ok = emqx_event_topic:enable(),
    {ok, C1} = emqtt:start_link([{clientid, <<"monsys">>}]),
    {ok, _} = emqtt:connect(C1),
    {ok, _, [?QOS_1]} = emqtt:subscribe(C1, <<"$event/client_connected">>, qos1),
    {ok, C2} = emqtt:start_link([{clientid, <<"clientid">>},
                                 {username, <<"username">>}]),
    {ok, _} = emqtt:connect(C2),
    ok = recv_connected(<<"clientid">>),

    {ok, _, [?QOS_1]} = emqtt:subscribe(C1, <<"$event/session_subscribed">>, qos1),
    _ = receive_publish(100),
    timer:sleep(50),
    {ok, _, [?QOS_1]} = emqtt:subscribe(C2, <<"test_sub">>, qos1),
    ok = recv_subscribed(<<"clientid">>),
    emqtt:unsubscribe(C1, <<"$event/session_subscribed">>),
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
    ok= emqtt:publish(C2, <<"test_sub1">>, <<"test">>),
    recv_message_dropped(<<"clientid">>),

    {ok, _, [?QOS_1]} = emqtt:subscribe(C1, <<"$event/session_unsubscribed">>, qos1),
    _ = emqtt:unsubscribe(C2, <<"test_sub">>),
    ok = recv_unsubscribed(<<"clientid">>),

    {ok, _, [?QOS_1]} = emqtt:subscribe(C1, <<"$event/client_disconnected">>, qos1),
    ok = emqtt:disconnect(C2),
    ok = recv_disconnected(<<"clientid">>),
    ok = emqtt:disconnect(C1),
    ok = emqx_event_topic:disable().

t_reason(_) ->
    ?assertEqual(normal, emqx_event_topic:reason(normal)),
    ?assertEqual(discarded, emqx_event_topic:reason({shutdown, discarded})),
    ?assertEqual(tcp_error, emqx_event_topic:reason({tcp_error, einval})),
    ?assertEqual(internal_error, emqx_event_topic:reason(<<"unknown error">>)).

recv_connected(ClientId) ->
    {ok, #{qos := ?QOS_0, topic := Topic, payload := Payload}} = receive_publish(100),
    ?assertMatch(<<"$event/client_connected">>, Topic),
    ?assertMatch(#{<<"clientid">> := ClientId,
                           <<"username">> := <<"username">>,
                           <<"ipaddress">> := <<"127.0.0.1">>,
                           <<"proto_name">> := <<"MQTT">>,
                           <<"proto_ver">> := ?MQTT_PROTO_V4,
                           <<"connack">> := ?RC_SUCCESS,
                           <<"clean_start">> := true}, emqx_json:decode(Payload, [return_maps])).

recv_subscribed(_ClientId) ->
    {ok, #{qos := ?QOS_0, topic := Topic}} = receive_publish(100),
    ?assertMatch(<<"$event/session_subscribed">>, Topic).

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
    ?assertMatch(<<"$event/session_unsubscribed">>, Topic).

recv_disconnected(ClientId) ->
    {ok, #{qos := ?QOS_0, topic := Topic, payload := Payload}} = receive_publish(100),
    ?assertMatch(<<"$event/client_disconnected">>, Topic),
    ?assertMatch(#{<<"clientid">> := ClientId,
                           <<"username">> := <<"username">>,
                           <<"reason">> := <<"normal">>}, emqx_json:decode(Payload, [return_maps])).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

receive_publish(Timeout) ->
    receive
        {publish, Publish} ->
            {ok, Publish}
    after
        Timeout -> {error, timeout}
    end.
