%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_channel_await_rel_expire_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx, "mqtt { await_rel_timeout = 100ms, max_awaiting_rel = 5 }"}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

%% We test here that Packet Ids are released even if the client is not connected
t_check_rel_cleanup(_Config) ->
    %% Connect client and fill the awaiting_rel queue
    Host = "127.0.0.1",
    Port = 1883,
    ConnectPacket = ?CONNECT_PACKET(
        #mqtt_packet_connect{
            proto_ver = ?MQTT_PROTO_V5,
            properties = #{'Session-Expiry-Interval' => 30000},
            clientid = <<"clientid">>,
            clean_start = false
        }
    ),
    {ok, Client0} = emqx_mqtt_test_client:start_link(Host, Port),
    ok = emqx_mqtt_test_client:send(Client0, ConnectPacket),
    {ok, ?CONNACK_PACKET(?RC_SUCCESS, _, _)} = emqx_mqtt_test_client:receive_packet(),
    ok = lists:foreach(
        fun(N) ->
            PacketId = N,
            ok = emqx_mqtt_test_client:publish(Client0, PacketId, <<"topic">>, <<"hello">>, 2),
            ?assertMatch(
                {ok, ?PUBREC_PACKET(PacketId, ?RC_NO_MATCHING_SUBSCRIBERS, _)},
                emqx_mqtt_test_client:receive_packet()
            )
        end,
        lists:seq(1, 5)
    ),

    %% Check that the queue is full
    ok = emqx_mqtt_test_client:publish(Client0, 6, <<"topic">>, <<"hello">>, 2),
    ?assertMatch(
        {ok, ?DISCONNECT_PACKET(?RC_RECEIVE_MAXIMUM_EXCEEDED, _)},
        emqx_mqtt_test_client:receive_packet()
    ),

    %% Wait for the queue to be cleaned up
    ct:sleep(200),

    %% Reconnect after the queue is cleaned up
    {ok, Client1} = emqx_mqtt_test_client:start_link(Host, Port),
    ok = emqx_mqtt_test_client:send(Client1, ConnectPacket),
    {ok, ?CONNACK_PACKET(?RC_SUCCESS, _, _)} = emqx_mqtt_test_client:receive_packet(),

    %% Check that the packets are expired
    PacketId = 6,
    ok = emqx_mqtt_test_client:publish(Client1, PacketId, <<"topic">>, <<"hello">>, 2),
    ?assertMatch(
        {ok, ?PUBREC_PACKET(PacketId, ?RC_NO_MATCHING_SUBSCRIBERS, _)},
        emqx_mqtt_test_client:receive_packet()
    ),

    %% Disconnect the client
    emqx_mqtt_test_client:stop(Client1).
