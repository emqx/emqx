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

-export([ structs/0
        , fields/1]).

structs() -> ["emqx_bridge_mqtt"].

fields("emqx_bridge_mqtt") ->
    [ {bridges, hoconsc:array("bridges")}
    ];

fields("bridges") ->
    [ {name, emqx_schema:t(string(), undefined, true)}
    , {start_type, fun start_type/1}
    , {forwards, fun forwards/1}
    , {forward_mountpoint, emqx_schema:t(string())}
    , {reconnect_interval, emqx_schema:t(emqx_schema:duration_ms(), undefined, "30s")}
    , {batch_size, emqx_schema:t(integer(), undefined, 100)}
    , {queue, emqx_schema:t(hoconsc:ref("queue"))}
    , {config, hoconsc:union([hoconsc:ref("mqtt"), hoconsc:ref("rpc")])}
    ];

fields("mqtt") ->
    [ {conn_type, fun conn_type/1}
    , {address, emqx_schema:t(string(), undefined, "127.0.0.1:1883")}
    , {proto_ver, fun proto_ver/1}
    , {bridge_mode, emqx_schema:t(boolean(), undefined, true)}
    , {clientid, emqx_schema:t(string())}
    , {username, emqx_schema:t(string())}
    , {password, emqx_schema:t(string())}
    , {clean_start, emqx_schema:t(boolean(), undefined, true)}
    , {keepalive, emqx_schema:t(integer(), undefined, 300)}
    , {subscriptions, hoconsc:array("subscriptions")}
    , {receive_mountpoint, emqx_schema:t(string())}
    , {retry_interval, emqx_schema:t(emqx_schema:duration_ms(), undefined, "30s")}
    , {max_inflight, emqx_schema:t(integer(), undefined, 32)}
    ];

fields("rpc") ->
    [ {conn_type, fun conn_type/1}
    , {node, emqx_schema:t(atom(), undefined, 'emqx@127.0.0.1')}
    ];

fields("subscriptions") ->
    [ {topic, #{type => binary(), nullable => false}}
    , {qos, emqx_schema:t(integer(), undefined, 1)}
    ];

fields("queue") ->
    [ {replayq_dir, hoconsc:union([boolean(), string()])}
    , {replayq_seg_bytes, emqx_schema:t(emqx_schema:bytesize(), undefined, "100MB")}
    , {replayq_offload_mode, emqx_schema:t(boolean(), undefined, false)}
    , {replayq_max_total_bytes, emqx_schema:t(emqx_schema:bytesize(), undefined, "1024MB")}
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
