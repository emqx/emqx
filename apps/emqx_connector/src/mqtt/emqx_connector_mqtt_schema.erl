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

roots() ->
    [{config, #{type => hoconsc:ref(?MODULE, "config")}}].

fields("config") ->
    [ {server, hoconsc:mk(emqx_schema:ip_port(), #{default => "127.0.0.1:1883"})}
    , {reconnect_interval, hoconsc:mk(emqx_schema:duration_ms(), #{default => "30s"})}
    , {proto_ver, fun proto_ver/1}
    , {bridge_mode, hoconsc:mk(boolean(), #{default => true})}
    , {username, hoconsc:mk(string())}
    , {password, hoconsc:mk(string())}
    , {clean_start, hoconsc:mk(boolean(), #{default => true})}
    , {keepalive, hoconsc:mk(integer(), #{default => 300})}
    , {retry_interval, hoconsc:mk(emqx_schema:duration_ms(), #{default => "30s"})}
    , {max_inflight, hoconsc:mk(integer(), #{default => 32})}
    , {replayq, hoconsc:mk(hoconsc:ref(?MODULE, "replayq"))}
    , {ingress_channels, hoconsc:mk(hoconsc:map(id, hoconsc:ref(?MODULE, "ingress_channels")), #{default => []})}
    , {egress_channels, hoconsc:mk(hoconsc:map(id, hoconsc:ref(?MODULE, "egress_channels")), #{default => []})}
    ] ++ emqx_connector_schema_lib:ssl_fields();

fields("ingress_channels") ->
    %% the message maybe subscribed by rules, in this case 'local_topic' is not necessary
    [ {subscribe_remote_topic, hoconsc:mk(binary(), #{nullable => false})}
    , {local_topic, hoconsc:mk(binary())}
    , {subscribe_qos, hoconsc:mk(qos(), #{default => 1})}
    ] ++ common_inout_confs();

fields("egress_channels") ->
    %% the message maybe sent from rules, in this case 'subscribe_local_topic' is not necessary
    [ {subscribe_local_topic, hoconsc:mk(binary())}
    , {remote_topic, hoconsc:mk(binary(), #{default => <<"${topic}">>})}
    ] ++ common_inout_confs();

fields("replayq") ->
    [ {dir, hoconsc:union([boolean(), string()])}
    , {seg_bytes, hoconsc:mk(emqx_schema:bytesize(), #{default => "100MB"})}
    , {offload, hoconsc:mk(boolean(), #{default => false})}
    , {max_total_bytes, hoconsc:mk(emqx_schema:bytesize(), #{default => "1024MB"})}
    ].

common_inout_confs() ->
    [ {qos, hoconsc:mk(qos(), #{default => <<"${qos}">>})}
    , {retain, hoconsc:mk(hoconsc:union([boolean(), binary()]), #{default => <<"${retain}">>})}
    , {payload, hoconsc:mk(binary(), #{default => <<"${payload}">>})}
    ].

qos() ->
    hoconsc:union([typerefl:integer(0), typerefl:integer(1), typerefl:integer(2), binary()]).

proto_ver(type) -> hoconsc:enum([v3, v4, v5]);
proto_ver(default) -> v4;
proto_ver(_) -> undefined.
