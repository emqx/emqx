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
    , {max_inflight, sc(integer(), #{default => 32})}
    , {replayq, sc(ref("replayq"))}
    , {ingress_channels, sc(hoconsc:map(id, ref("ingress_channels")), #{default => []})}
    , {egress_channels, sc(hoconsc:map(id, ref("egress_channels")), #{default => []})}
    ] ++ emqx_connector_schema_lib:ssl_fields();

fields("ingress_channels") ->
    %% the message maybe subscribed by rules, in this case 'local_topic' is not necessary
    [ {subscribe_remote_topic, sc(binary(), #{nullable => false})}
    , {local_topic, sc(binary())}
    , {subscribe_qos, sc(qos(), #{default => 1})}
    ] ++ common_inout_confs();

fields("egress_channels") ->
    %% the message maybe sent from rules, in this case 'subscribe_local_topic' is not necessary
    [ {subscribe_local_topic, sc(binary())}
    , {remote_topic, sc(binary(), #{default => <<"${topic}">>})}
    ] ++ common_inout_confs();

fields("replayq") ->
    [ {dir, hoconsc:union([boolean(), string()])}
    , {seg_bytes, sc(emqx_schema:bytesize(), #{default => "100MB"})}
    , {offload, sc(boolean(), #{default => false})}
    , {max_total_bytes, sc(emqx_schema:bytesize(), #{default => "1024MB"})}
    ].

common_inout_confs() ->
    [ {qos, sc(qos(), #{default => <<"${qos}">>})}
    , {retain, sc(hoconsc:union([boolean(), binary()]), #{default => <<"${retain}">>})}
    , {payload, sc(binary(), #{default => <<"${payload}">>})}
    ].

qos() ->
    hoconsc:union([typerefl:integer(0), typerefl:integer(1), typerefl:integer(2), binary()]).

sc(Type, Meta) -> hoconsc:mk(Type, Meta).
ref(Field) -> hoconsc:ref(?MODULE, Field).
