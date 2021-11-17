%%--------------------------------------------------------------------
%% Copyright (c) 2017-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-ifndef(EMQ_X_PLACEHOLDER_HRL).
-define(EMQ_X_PLACEHOLDER_HRL, true).

-define(PH(Type), <<"${", Type/binary, "}">>).

%% publish/subscribe/all
-define(PH_PUB, ?PH(<<"publish">>)).
-define(PH_SUB, ?PH(<<"subscribe">>)).
-define(PH_PUB_SUB, ?PH(<<"all">>)).

%% cert
-define(PH_CRET_SUBJECT, ?PH(<<"cert_subject">>)).
-define(PH_CRET_CN_NAME, ?PH(<<"cert_common_name">>)).

%% MQTT
-define(PH_PASSWORD, ?PH(<<"password">>)).
-define(PH_CLIENTID, ?PH(<<"clientid">>)).
-define(PH_FROM_CLIENTID, ?PH(<<"from_clienid">>)).
-define(PH_USERNAME, ?PH(<<"username">>)).
-define(PH_FROM_USERNAME, ?PH(<<"from_username">>)).
-define(PH_TOPIC, ?PH(<<"topic">>)).
%% MQTT payload
-define(PH_PAYLOAD, ?PH(<<"payload">>)).
%% client IPAddress
-define(PH_PEERHOST, ?PH(<<"peerhost">>)).
%% Enumeration of message QoS 0,1,2
-define(PH_QOS, ?PH(<<"qos">>)).
-define(PH_FLAGS, ?PH(<<"flags">>)).
%% Additional data related to process within the MQTT message
-define(PH_HEADERS, ?PH(<<"hearders">>)).
%% protocol name
-define(PH_PROTONAME, ?PH(<<"proto_name">>)).
%% protocol version
-define(PH_PROTOVER, ?PH(<<"proto_ver">>)).
%% MQTT keepalive interval
-define(PH_KEEPALIVE, ?PH(<<"keepalive">>)).
%% MQTT clean_start
-define(PH_CLEAR_START, ?PH(<<"clean_start">>)).
%% MQTT Session Expiration time
-define(PH_EXPIRY_INTERVAL, ?PH(<<"expiry_interval">>)).

%% Time when PUBLISH message reaches Broker (ms)
-define(PH_PUBLISH_RECEIVED_AT, ?PH(<<"publish_received_at">>)).
%% Mountpoint for bridging messages
-define(PH_MOUNTPOINT, ?PH(<<"mountpoint">>)).
%% IPAddress and Port of terminal
-define(PH_PEERNAME, ?PH(<<"peername">>)).
%% IPAddress and Port listened by emqx
-define(PH_SOCKNAME, ?PH(<<"sockname">>)).
%% whether it is MQTT bridge connection
-define(PH_IS_BRIDGE, ?PH(<<"is_bridge">>)).
%% Terminal connection completion time (s)
-define(PH_CONNECTED_AT, ?PH(<<"connected_at">>)).
%% Event trigger time(millisecond)
-define(PH_TIMESTAMP, ?PH(<<"timestamp">>)).
%% Terminal disconnection completion time (s)
-define(PH_DISCONNECTED_AT, ?PH(<<"disconnected_at">>)).

-define(PH_NODE, ?PH(<<"node">>)).
-define(PH_REASON, ?PH(<<"reason">>)).

-endif.
