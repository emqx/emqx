%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_utils_types).

-export_type([
    flag/0,
    flags/0,
    proto_ver/0,
    protocol/0,
    peerhost/0,
    username/0,
    headers/0
]).

-type flag() :: sys | dup | retain | atom().
-type flags() :: #{flag() := boolean()}.

-define(MQTT_PROTO_V3, 3).
-define(MQTT_PROTO_V4, 4).
-define(MQTT_PROTO_V5, 5).

-type proto_ver() ::
    ?MQTT_PROTO_V3
    | ?MQTT_PROTO_V4
    | ?MQTT_PROTO_V5
    | non_neg_integer()
    % For lwm2m, mqtt-sn...
    | binary().

-type protocol() :: mqtt | 'mqtt-sn' | coap | lwm2m | stomp | none | atom().
-type peerhost() :: inet:ip_address().
-type properties() :: #{atom() => term()}.
-type username() :: undefined | binary().

-type headers() :: #{
    proto_ver => proto_ver(),
    protocol => protocol(),
    username => username(),
    peerhost => peerhost(),
    properties => properties(),
    allow_publish => boolean(),
    atom() => term()
}.
