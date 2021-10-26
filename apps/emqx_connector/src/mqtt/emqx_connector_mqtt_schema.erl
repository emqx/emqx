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

-module(emqx_connector_mqtt_schema).

-include_lib("typerefl/include/types.hrl").

-behaviour(hocon_schema).

-export([ roots/0
        , fields/1]).

-import(emqx_schema, [mk_duration/2]).

roots() ->
    fields("config").

fields("config") ->
    [ {server,
        sc(emqx_schema:ip_port(),
           #{ default => "127.0.0.1:1883"
            , desc => "The host and port of the remote MQTT broker"
            })}
    , {reconnect_interval, mk_duration("reconnect interval", #{default => "30s"})}
    , {proto_ver,
        sc(hoconsc:enum([v3, v4, v5]),
           #{ default => v4
            , desc => "The MQTT protocol version"
            })}
    , {bridge_mode,
        sc(boolean(),
           #{ default => true
            , desc => "The bridge mode of the MQTT protocol"
            })}
    , {username,
        sc(binary(),
           #{ default => "emqx"
            , desc => "The username of the MQTT protocol"
            })}
    , {password,
        sc(binary(),
           #{ default => "emqx"
            , desc => "The password of the MQTT protocol"
            })}
    , {clientid,
        sc(binary(),
           #{ default => "emqx_${nodename}"
            , desc => "The clientid of the MQTT protocol"
            })}
    , {clean_start,
        sc(boolean(),
           #{ default => true
            , desc => "The clean-start or the clean-session of the MQTT protocol"
            })}
    , {keepalive, mk_duration("keepalive", #{default => "300s"})}
    , {retry_interval, mk_duration("retry interval", #{default => "30s"})}
    , {max_inflight,
        sc(integer(),
           #{ default => 32
            , desc => "Max inflight messages (sent but ACK has not received) of the MQTT protocol"
            })}
    , {replayq,
        sc(ref("replayq"),
           #{ desc => """
Queue messages in disk files.
"""
            })}
    , {ingress,
        sc(ref("ingress"),
           #{ default => #{}
            , desc => """
The ingress config defines how this bridge receive messages from the remote MQTT broker, and then
send them to the local broker.<br>
Template with variables is allowed in 'to_local_topic', 'subscribe_qos', 'qos', 'retain',
'payload'.<br>
NOTE: if this bridge is used as the input of a rule (emqx rule engine), and also to_local_topic is
configured, then messages got from the remote broker will be sent to both the 'to_local_topic' and
the rule.
"""
            })}
    , {egress,
        sc(ref("egress"),
           #{ default => #{}
            , desc => """
The egress config defines how this bridge forwards messages from the local broker to the remote
broker.<br>
Template with variables is allowed in 'to_remote_topic', 'qos', 'retain', 'payload'.<br>
NOTE: if this bridge is used as the output of a rule (emqx rule engine), and also from_local_topic
is configured, then both the data got from the rule and the MQTT messages that matches
from_local_topic will be forwarded.
"""
            })}
    ] ++ emqx_connector_schema_lib:ssl_fields();

fields("ingress") ->
    %% the message maybe subscribed by rules, in this case 'to_local_topic' is not necessary
    [ {from_remote_topic,
        sc(binary(),
           #{ nullable => false
            , desc => "Receive messages from which topic of the remote broker"
            })}
    , {subscribe_qos,
        sc(qos(),
           #{ default => 1
            , desc => "The QoS level to be used when subscribing to the remote broker"
            })}
    , {to_local_topic,
        sc(binary(),
           #{ desc => """
Send messages to which topic of the local broker.<br>
Template with variables is allowed.
"""
            })}
    ] ++ common_inout_confs();

fields("egress") ->
    %% the message maybe sent from rules, in this case 'from_local_topic' is not necessary
    [ {from_local_topic,
        sc(binary(),
           #{ desc => "The local topic to be forwarded to the remote broker"
            })}
    , {to_remote_topic,
        sc(binary(),
           #{ default => <<"${topic}">>
            , desc => """
Forward to which topic of the remote broker.<br>
Template with variables is allowed.
"""
            })}
    ] ++ common_inout_confs();

fields("replayq") ->
    [ {dir,
        sc(hoconsc:union([boolean(), string()]),
           #{ desc => """
The dir where the replayq file saved.<br>
Set to 'false' disables the replayq feature.
"""
            })}
    , {seg_bytes,
        sc(emqx_schema:bytesize(),
           #{ default => "100MB"
            , desc => """
The size in bytes of a single segment.<br>
A segment is mapping to a file in the replayq dir. If the current segment is full, a new segment
(file) will be opened to write.
"""
            })}
    , {offload,
        sc(boolean(),
           #{ default => false
            , desc => """
In offload mode, the disk queue is only used to offload queue tail segments.<br>
The messages are cached in the memory first, then it write to the replayq files after the size of
the memory cache reaches 'seg_bytes'.
"""
            })}
    ].

common_inout_confs() ->
    [ {qos,
        sc(qos(),
           #{ default => <<"${qos}">>
            , desc => """
The QoS of the MQTT message to be sent.<br>
Template with variables is allowed."""
            })}
    , {retain,
        sc(hoconsc:union([boolean(), binary()]),
           #{ default => <<"${retain}">>
            , desc => """
The retain flag of the MQTT message to be sent.<br>
Template with variables is allowed."""
            })}
    , {payload,
        sc(binary(),
           #{ default => <<"${payload}">>
            , desc => """
The payload of the MQTT message to be sent.<br>
Template with variables is allowed."""
            })}
    ].

qos() ->
    hoconsc:union([typerefl:integer(0), typerefl:integer(1), typerefl:integer(2), binary()]).

sc(Type, Meta) -> hoconsc:mk(Type, Meta).
ref(Field) -> hoconsc:ref(?MODULE, Field).
