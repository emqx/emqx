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

-module(emqx_portal_msg).

-export([ to_binary/1
        , from_binary/1
        , apply_mountpoint/2
        , to_broker_msgs/1
        , estimate_size/1
        ]).

-export_type([msg/0]).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").

-type msg() :: emqx_types:message().

%% @doc Mount topic to a prefix.
-spec apply_mountpoint(msg(), undefined | binary()) -> msg().
apply_mountpoint(#{topic := Topic} = Msg, Mountpoint) ->
    Msg#{topic := topic(Mountpoint, Topic)}.

%% @doc Make `binary()' in order to make iodata to be persisted on disk.
-spec to_binary(msg()) -> binary().
to_binary(Msg) -> term_to_binary(Msg).

%% @doc Unmarshal binary into `msg()'.
-spec from_binary(binary()) -> msg().
from_binary(Bin) -> binary_to_term(Bin).

%% @doc Estimate the size of a message.
%% Count only the topic length + payload size
-spec estimate_size(msg()) -> integer().
estimate_size(#{topic := Topic, payload := Payload}) ->
    size(Topic) + size(Payload).

%% @doc By message/batch receiver, transform received batch into
%% messages to dispatch to local brokers.
to_broker_msgs(Batch) -> lists:map(fun to_broker_msg/1, Batch).

to_broker_msg(#{qos := QoS, dup := Dup, retain := Retain, topic := Topic,
                properties := Props, payload := Payload}) ->
    emqx_message:set_headers(Props,
        emqx_message:set_flags(#{dup => Dup, retain => Retain},
            emqx_message:make(portal, QoS, Topic, Payload))).

topic(Prefix, Topic) -> emqx_topic:prepend(Prefix, Topic).

