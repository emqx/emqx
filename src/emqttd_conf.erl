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

-export([init/0]).

-export([mqtt/0, session/0, queue/0, bridge/0, pubsub/0]).

-export([value/1, value/2, list/1]).

-define(APP, emqttd).

init() -> gen_conf:init(?APP).

mqtt() ->
    with_env(mqtt_protocol, [
        %% Max ClientId Length Allowed.
        {max_clientid_len,    value(mqtt_max_clientid_len, 512)},
        %% Max Packet Size Allowed, 64K by default.
        {max_packet_size,     value(mqtt_max_packet_size, 65536)},
        %% Client Idle Timeout.
        {client_idle_timeout, value(mqtt_client_idle_timeout, 30)}
    ]).

session() ->
    with_env(mqtt_session, [
        %% Max number of QoS 1 and 2 messages that can be “inflight” at one time.
        %% 0 means no limit
        {max_inflight,         value(session_max_inflight, 100)},

        %% Retry interval for redelivering QoS1/2 messages.
        {unack_retry_interval, value(session_unack_retry_interval, 60)},

        %% Awaiting PUBREL Timeout
        {await_rel_timeout,    value(session_await_rel_timeout, 20)},

        %% Max Packets that Awaiting PUBREL, 0 means no limit
        {max_awaiting_rel,     value(session_max_awaiting_rel, 0)},

        %% Statistics Collection Interval(seconds)
        {collect_interval,     value(session_collect_interval, 0)},

        %% Expired after 2 day (unit: minute)
        {expired_after,        value(session_expired_after, 2880)}
    ]).

queue() ->
    with_env(mqtt_queue, [
        %% Type: simple | priority
        {type,             value(queue_type, simple)},

        %% Topic Priority: 0~255, Default is 0
        {priority,         value(queue_priority, [])},

        %% Max queue length. Enqueued messages when persistent client disconnected,
        %% or inflight window is full.
        {max_length,       value(queue_max_length, infinity)},

        %% Low-water mark of queued messages
        {low_watermark,    value(queue_low_watermark, 0.2)},

        %% High-water mark of queued messages
        {high_watermark,   value(queue_high_watermark, 0.6)},

        %% Queue Qos0 messages?
        {queue_qos0,       value(queue_qos0, true)}
    ]).

bridge() ->
    with_env(mqtt_bridge, [
        {max_queue_len,      value(bridge_max_queue_len, 10000)},

        %% Ping Interval of bridge node
        {ping_down_interval, value(bridge_ping_down_interval, 1)}
    ]).

pubsub() ->
    with_env(mqtt_pubsub, [
        %% PubSub and Router. Default should be scheduler numbers.
        {pool_size, value(pubsub_pool_size, 8)}
    ]).

value(Key) ->
    with_env(Key, gen_conf:value(?APP, Key)).

value(Key, Default) ->
    with_env(Key, gen_conf:value(?APP, Key, Default)).

with_env(Key, Conf) ->
    case application:get_env(?APP, Key) of
        undefined ->
            application:set_env(?APP, Key, Conf), Conf;
        {ok, Val} ->
            Val
    end.

list(Key) -> gen_conf:list(?APP, Key).

