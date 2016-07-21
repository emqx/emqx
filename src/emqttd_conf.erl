%%--------------------------------------------------------------------
%% Copyright (c) 2012-2016 Feng Lee <feng@emqtt.io>.
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

-module(emqttd_conf).

-export([mqtt/0, retained/0, session/0, queue/0, bridge/0, pubsub/0]).

mqtt() ->
    [
    %% Max ClientId Length Allowed.
    {max_clientid_len,    emqttd:conf(mqtt_max_clientid_len, 512)},
    %% Max Packet Size Allowed, 64K by default.
    {max_packet_size,     emqttd:conf(mqtt_max_packet_size, 65536)},
    %% Client Idle Timeout.
    {client_idle_timeout, emqttd:conf(mqtt_client_idle_timeout, 30)}
    ].

retained() ->
    [
    %% Expired after seconds, never expired if 0
    {expired_after, emqttd:conf(retained_expired_after, 0)},
    %% Max number of retained messages
    {max_message_num, emqttd:conf(retained_max_message_num, 100000)},
    %% Max Payload Size of retained message
    {max_playload_size, emqttd:conf(retained_max_playload_size, 65536)}
    ].

session() ->
    [
    %% Max number of QoS 1 and 2 messages that can be “inflight” at one time.
    %% 0 means no limit
    {max_inflight, emqttd:conf(session_max_inflight, 100)},

    %% Retry interval for redelivering QoS1/2 messages.
    {unack_retry_interval, emqttd:conf(session_unack_retry_interval, 60)},

    %% Awaiting PUBREL Timeout
    {await_rel_timeout, emqttd:conf(session_await_rel_timeout, 20)},

    %% Max Packets that Awaiting PUBREL, 0 means no limit
    {max_awaiting_rel, emqttd:conf(session_max_awaiting_rel, 0)},

    %% Statistics Collection Interval(seconds)
    {collect_interval, emqttd:conf(session_collect_interval, 0)},

    %% Expired after 2 day (unit: minute)
    {expired_after, emqttd:conf(session_expired_after, 2880)}
    ].

queue() ->
    [
    %% Type: simple | priority
    {type, emqttd:conf(queue_type, simple)},

    %% Topic Priority: 0~255, Default is 0
    {priority, emqttd:conf(queue_priority, [])},

    %% Max queue length. Enqueued messages when persistent client disconnected,
    %% or inflight window is full.
    {max_length, emqttd:conf(queue_max_length, infinity)},

    %% Low-water mark of queued messages
    {low_watermark, emqttd:conf(queue_low_watermark, 0.2)},

    %% High-water mark of queued messages
    {high_watermark, emqttd:conf(queue_high_watermark, 0.6)},

    %% Queue Qos0 messages?
    {queue_qos0, emqttd:conf(queue_qos0, true)}
    ].

bridge() ->
    [
    %% TODO: Bridge Queue Size
    {max_queue_len,      emqttd:conf(bridge_max_queue_len, 10000)},

    %% Ping Interval of bridge node
    {ping_down_interval, emqttd:conf(bridge_ping_down_interval, 1)}
    ].

pubsub() ->
    [
    %% PubSub and Router. Default should be scheduler numbers.
    {pool_size, emqttd:conf(pubsub_pool_size, 8)}
    ].

