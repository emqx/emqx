%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_bridge_msg).

-export([ to_binary/1
        , from_binary/1
        , to_export/3
        , to_broker_msgs/1
        , estimate_size/1
        ]).

-export_type([msg/0]).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").
-include("emqx_client.hrl").

-type msg() :: emqx_types:message().
-type exp_msg() :: emqx_types:message() | #mqtt_msg{}.

%% @doc Make export format:
%% 1. Mount topic to a prefix
%% 2. Fix QoS to 1
%% @end
%% Shame that we have to know the callback module here
%% would be great if we can get rid of #mqtt_msg{} record
%% and use #message{} in all places.
-spec to_export(emqx_bridge_rpc | emqx_bridge_mqtt,
                undefined | binary(), msg()) -> exp_msg().
to_export(emqx_bridge_mqtt, Mountpoint,
          #message{topic = Topic,
                   payload = Payload,
                   flags = Flags
                  }) ->
    Retain = maps:get(retain, Flags, false),
    #mqtt_msg{qos = ?QOS_1,
              retain = Retain,
              topic = topic(Mountpoint, Topic),
              payload = Payload};
to_export(_Module, Mountpoint,
          #message{topic = Topic} = Msg) ->
    Msg#message{topic = topic(Mountpoint, Topic), qos = 1}.

%% @doc Make `binary()' in order to make iodata to be persisted on disk.
-spec to_binary(msg()) -> binary().
to_binary(Msg) -> term_to_binary(Msg).

%% @doc Unmarshal binary into `msg()'.
-spec from_binary(binary()) -> msg().
from_binary(Bin) -> binary_to_term(Bin).

%% @doc Estimate the size of a message.
%% Count only the topic length + payload size
-spec estimate_size(msg()) -> integer().
estimate_size(#message{topic = Topic, payload = Payload}) ->
    size(Topic) + size(Payload).

%% @doc By message/batch receiver, transform received batch into
%% messages to dispatch to local brokers.
to_broker_msgs(Batch) -> lists:map(fun to_broker_msg/1, Batch).

to_broker_msg(#message{} = Msg) ->
    %% internal format from another EMQX node via rpc
    Msg;
to_broker_msg(#{qos := QoS, dup := Dup, retain := Retain, topic := Topic,
                properties := Props, payload := Payload}) ->
    %% published from remote node over a MQTT connection
    emqx_message:set_headers(Props,
        emqx_message:set_flags(#{dup => Dup, retain => Retain},
            emqx_message:make(bridge, QoS, Topic, Payload))).

topic(Prefix, Topic) -> emqx_topic:prepend(Prefix, Topic).
