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

-module(emqx_connector_mqtt_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-behaviour(hocon_schema).

-export([ roots/0
        , fields/1
        , desc/1
        ]).

-export([ ingress_desc/0
        , egress_desc/0
        ]).

-export([non_empty_string/1]).

-import(emqx_schema, [mk_duration/2]).

roots() ->
    fields("config").

fields("config") ->
    fields("connector") ++
    topic_mappings();

fields("connector") ->
    [ {mode,
        sc(hoconsc:enum([cluster_shareload]),
           #{ default => cluster_shareload
            , desc => ?DESC("mode")
            })}
    , {server,
        sc(emqx_schema:ip_port(),
           #{ default => "127.0.0.1:1883"
            , desc => ?DESC("server")
            })}
    , {reconnect_interval, mk_duration(
        "Reconnect interval. Delay for the MQTT bridge to retry establishing the connection "
        "in case of transportation failure.",
        #{default => "15s"})}
    , {proto_ver,
        sc(hoconsc:enum([v3, v4, v5]),
           #{ default => v4
            , desc => ?DESC("proto_ver")
            })}
    , {username,
        sc(binary(),
           #{ default => "emqx"
            , desc => ?DESC("username")
            })}
    , {password,
        sc(binary(),
           #{ default => "emqx"
            , desc => ?DESC("password")
            })}
    , {clean_start,
        sc(boolean(),
           #{ default => true
            , desc => ?DESC("clean_start")
            })}
    , {keepalive, mk_duration("MQTT Keepalive.", #{default => "300s"})}
    , {retry_interval, mk_duration(
        "Message retry interval. Delay for the MQTT bridge to retry sending the QoS1/QoS2 "
        "messages in case of ACK not received.",
        #{default => "15s"})}
    , {max_inflight,
        sc(non_neg_integer(),
           #{ default => 32
            , desc => ?DESC("max_inflight")
            })}
    , {replayq,
        sc(ref("replayq"), #{})}
    ] ++ emqx_connector_schema_lib:ssl_fields();

fields("ingress") ->
    %% the message maybe subscribed by rules, in this case 'local_topic' is not necessary
    [ {remote_topic,
        sc(binary(),
           #{ required => true
            , validator => fun ?MODULE:non_empty_string/1
            , desc => ?DESC("ingress_remote_topic")
            })}
    , {remote_qos,
        sc(qos(),
           #{ default => 1
            , desc => ?DESC("ingress_remote_qos")
            })}
    , {local_topic,
        sc(binary(),
           #{ validator => fun ?MODULE:non_empty_string/1
            , desc => ?DESC("ingress_local_topic")
            })}
    , {local_qos,
        sc(qos(),
           #{ default => <<"${qos}">>
            , desc => ?DESC("ingress_local_qos")
            })}
    , {hookpoint,
        sc(binary(),
           #{ desc => ?DESC("ingress_hookpoint")
            })}
    ] ++ common_inout_confs();

fields("egress") ->
    %% the message maybe sent from rules, in this case 'local_topic' is not necessary
    [ {local_topic,
        sc(binary(),
           #{ desc => ?DESC("egress_local_topic")
            , validator => fun ?MODULE:non_empty_string/1
            })}
    , {remote_topic,
        sc(binary(),
           #{ default => <<"${topic}">>
            , validator => fun ?MODULE:non_empty_string/1
            , desc => ?DESC("egress_remote_topic")
            })}
    , {remote_qos,
        sc(qos(),
           #{ default => <<"${qos}">>
            , desc => ?DESC("egress_remote_qos")
            })}
    ] ++ common_inout_confs();

fields("replayq") ->
    [ {dir,
        sc(hoconsc:union([boolean(), string()]),
           #{ desc => ?DESC("dir")
            })}
    , {seg_bytes,
        sc(emqx_schema:bytesize(),
           #{ default => "100MB"
            , desc => ?DESC("seg_bytes")
            })}
    , {offload,
        sc(boolean(),
           #{ default => false
            , desc => ?DESC("offload")
            })}
    ].

desc("connector") ->
    "Generic configuration for the connector.";
desc("ingress") ->
    ingress_desc();
desc("egress") ->
    egress_desc();
desc("replayq") ->
    "Queue messages in disk files.";
desc(_) ->
    undefined.

topic_mappings() ->
    [ {ingress,
        sc(ref("ingress"),
           #{ default => #{}
            })}
    , {egress,
        sc(ref("egress"),
           #{ default => #{}
            })}
    ].

ingress_desc() -> "
The ingress config defines how this bridge receive messages from the remote MQTT broker, and then
send them to the local broker.<br/>
Template with variables is allowed in 'local_topic', 'remote_qos', 'qos', 'retain',
'payload'.<br/>
NOTE: if this bridge is used as the input of a rule (emqx rule engine), and also local_topic is
configured, then messages got from the remote broker will be sent to both the 'local_topic' and
the rule.
".

egress_desc() -> "
The egress config defines how this bridge forwards messages from the local broker to the remote
broker.<br/>
Template with variables is allowed in 'remote_topic', 'qos', 'retain', 'payload'.<br/>
NOTE: if this bridge is used as the output of a rule (emqx rule engine), and also local_topic
is configured, then both the data got from the rule and the MQTT messages that matches
local_topic will be forwarded.
".

common_inout_confs() ->
    [ {retain,
        sc(hoconsc:union([boolean(), binary()]),
           #{ default => <<"${retain}">>
            , desc => ?DESC("retain")
            })}
    , {payload,
        sc(binary(),
           #{ default => <<"${payload}">>
            , desc => ?DESC("payload")
            })}
    ].

qos() ->
    hoconsc:union([emqx_schema:qos(), binary()]).

non_empty_string(<<>>) -> {error, empty_string_not_allowed};
non_empty_string("") -> {error, empty_string_not_allowed};
non_empty_string(S) when is_binary(S); is_list(S) -> ok;
non_empty_string(_) -> {error, invalid_string}.

sc(Type, Meta) -> hoconsc:mk(Type, Meta).
ref(Field) -> hoconsc:ref(?MODULE, Field).
