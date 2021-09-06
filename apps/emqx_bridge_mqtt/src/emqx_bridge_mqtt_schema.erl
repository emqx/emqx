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

-module(emqx_bridge_mqtt_schema).

-include_lib("typerefl/include/types.hrl").

-behaviour(hocon_schema).

-export([ namespace/0
        , roots/0
        , fields/1]).

namespace() -> "bridge_mqtt".

roots() -> [array("bridge_mqtt")].

array(Name) -> {Name, hoconsc:array(hoconsc:ref(?MODULE, Name))}.

fields("bridge_mqtt") ->
    [ {name, sc(string(), #{default => true})}
    , {start_type, fun start_type/1}
    , {forwards, fun forwards/1}
    , {forward_mountpoint, sc(string(), #{})}
    , {reconnect_interval, sc(emqx_schema:duration_ms(), #{default => "30s"})}
    , {batch_size, sc(integer(), #{default => 100})}
    , {queue, sc(hoconsc:ref(?MODULE, "queue"), #{})}
    , {config, sc(hoconsc:union([hoconsc:ref(?MODULE, "mqtt"),
                                 hoconsc:ref(?MODULE, "rpc")]),
                  #{})}
    ];

fields("mqtt") ->
    [ {conn_type, fun conn_type/1}
    , {address, sc(string(), #{default => "127.0.0.1:1883"})}
    , {proto_ver, fun proto_ver/1}
    , {bridge_mode, sc(boolean(), #{default => true})}
    , {clientid, sc(string(), #{})}
    , {username, sc(string(), #{})}
    , {password, sc(string(), #{})}
    , {clean_start, sc(boolean(), #{default => true})}
    , {keepalive, sc(integer(), #{default => 300})}
    , {subscriptions, sc(hoconsc:array(hoconsc:ref(?MODULE, "subscriptions")), #{})}
    , {receive_mountpoint, sc(string(), #{})}
    , {retry_interval, sc(emqx_schema:duration_ms(), #{default => "30s"})}
    , {max_inflight, sc(integer(), #{default => 32})}
    ];

fields("rpc") ->
    [ {conn_type, fun conn_type/1}
    , {node, sc(atom(), #{default => 'emqx@127.0.0.1'})}
    ];

fields("subscriptions") ->
    [ {topic, #{type => binary(), nullable => false}}
    , {qos, sc(integer(), #{default => 1})}
    ];

fields("queue") ->
    [ {replayq_dir, hoconsc:union([boolean(), string()])}
    , {replayq_seg_bytes, sc(emqx_schema:bytesize(), #{default => "100MB"})}
    , {replayq_offload_mode, sc(boolean(), #{default => false})}
    , {replayq_max_total_bytes, sc(emqx_schema:bytesize(), #{default => "1024MB"})}
    ].

conn_type(type) -> hoconsc:enum([mqtt, rpc]);
conn_type(_) -> undefined.

proto_ver(type) -> hoconsc:enum([v3, v4, v5]);
proto_ver(default) -> v4;
proto_ver(_) -> undefined.

start_type(type) -> hoconsc:enum([auto, manual]);
start_type(default) -> auto;
start_type(_) -> undefined.

forwards(type) -> hoconsc:array(binary());
forwards(default) -> [];
forwards(_) -> undefined.

sc(Type, Meta) -> hoconsc:mk(Type, Meta).
