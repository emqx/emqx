%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mod_topic_metrics).

-behaviour(emqx_gen_mod).

-include("emqx.hrl").
-include("logger.hrl").
-include("emqx_mqtt.hrl").

-logger_header("[TOPIC_METRICS]").

-export([ load/1
        , unload/1
        ]).

-export([ on_message_publish/1
        , on_message_delivered/2
        , on_message_dropped/3
        ]).

load(_Env) ->
    emqx:hook('message.publish', fun ?MODULE:on_message_publish/1, []),
    emqx:hook('message.dropped', fun ?MODULE:on_message_dropped/3, []),
    emqx:hook('message.delivered', fun ?MODULE:on_message_delivered/2, []).

unload(_Env) ->
    emqx:unhook('message.publish', fun ?MODULE:on_message_publish/1),
    emqx:unhook('message.dropped', fun ?MODULE:on_message_dropped/3),
    emqx:unhook('message.delivered', fun ?MODULE:on_message_delivered/2).

on_message_publish(#message{topic = Topic, qos = QoS}) ->
    case emqx_topic_metrics:is_registered(Topic) of
        true ->
            emqx_topic_metrics:inc(Topic, 'messages.in'),
            case QoS of
                ?QOS_0 -> emqx_topic_metrics:inc(Topic, 'messages.qos0.in');
                ?QOS_1 -> emqx_topic_metrics:inc(Topic, 'messages.qos1.in');
                ?QOS_2 -> emqx_topic_metrics:inc(Topic, 'messages.qos2.in')
            end;
        false ->
            ok
    end.

on_message_delivered(_, #message{topic = Topic, qos = QoS}) ->
    case emqx_topic_metrics:is_registered(Topic) of
        true ->
            emqx_topic_metrics:inc(Topic, 'messages.out'),
            case QoS of
                ?QOS_0 -> emqx_topic_metrics:inc(Topic, 'messages.qos0.out');
                ?QOS_1 -> emqx_topic_metrics:inc(Topic, 'messages.qos1.out');
                ?QOS_2 -> emqx_topic_metrics:inc(Topic, 'messages.qos2.out')
            end;
        false ->
            ok
    end.

on_message_dropped(#message{topic = Topic}, _, _) ->
    case emqx_topic_metrics:is_registered(Topic) of
        true ->
            emqx_topic_metrics:inc(Topic, 'messages.dropped');
        false ->
            ok
    end.





