%%--------------------------------------------------------------------
%% Copyright (c) 2017-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-ifndef(EMQX_PLACEHOLDER_HRL).
-define(EMQX_PLACEHOLDER_HRL, true).

-define(PH(Type), <<"${", Type/binary, "}">>).

%% action: publish/subscribe/all
-define(PH_ACTION, <<"${action}">>).

%% cert
-define(PH_CERT_SUBJECT, <<"${cert_subject}">>).
-define(PH_CERT_CN_NAME, <<"${cert_common_name}">>).

%% MQTT
-define(PH_PASSWORD, <<"${password}">>).
-define(PH_CLIENTID, <<"${clientid}">>).
-define(PH_FROM_CLIENTID, <<"${from_clientid}">>).
-define(PH_USERNAME, <<"${username}">>).
-define(PH_FROM_USERNAME, <<"${from_username}">>).
-define(PH_TOPIC, <<"${topic}">>).
%% MQTT payload
-define(PH_PAYLOAD, <<"${payload}">>).
%% client IPAddress
-define(PH_PEERHOST, <<"${peerhost}">>).
%% ip & port
-define(PH_HOST, <<"${host}">>).
-define(PH_PORT, <<"${port}">>).
%% Enumeration of message QoS 0,1,2
-define(PH_QOS, <<"${qos}">>).
-define(PH_FLAGS, <<"${flags}">>).
%% Additional data related to process within the MQTT message
-define(PH_HEADERS, <<"${headers}">>).
%% protocol name
-define(PH_PROTONAME, <<"${proto_name}">>).
%% protocol version
-define(PH_PROTOVER, <<"${proto_ver}">>).
%% MQTT keepalive interval
-define(PH_KEEPALIVE, <<"${keepalive}">>).
%% MQTT clean_start
-define(PH_CLEAR_START, <<"${clean_start}">>).
%% MQTT Session Expiration time
-define(PH_EXPIRY_INTERVAL, <<"${expiry_interval}">>).

%% Time when PUBLISH message reaches Broker (ms)
-define(PH_PUBLISH_RECEIVED_AT, <<"${publish_received_at}">>).
%% Mountpoint for bridging messages
-define(PH_MOUNTPOINT, <<"${mountpoint}">>).
%% IPAddress and Port of terminal
-define(PH_PEERNAME, <<"${peername}">>).
%% IPAddress and Port listened by emqx
-define(PH_SOCKNAME, <<"${sockname}">>).
%% whether it is MQTT bridge connection
-define(PH_IS_BRIDGE, <<"${is_bridge}">>).
%% Terminal connection completion time (s)
-define(PH_CONNECTED_AT, <<"${connected_at}">>).
%% Event trigger time(millisecond)
-define(PH_TIMESTAMP, <<"${timestamp}">>).
%% Terminal disconnection completion time (s)
-define(PH_DISCONNECTED_AT, <<"${disconnected_at}">>).

-define(PH_NODE, <<"${node}">>).
-define(PH_REASON, <<"${reason}">>).

-define(PH_ENDPOINT_NAME, <<"${endpoint_name}">>).

%% sync change these place holder with binary def.
-define(PH_S_ACTION, "${action}").
-define(PH_S_CERT_SUBJECT, "${cert_subject}").
-define(PH_S_CERT_CN_NAME, "${cert_common_name}").
-define(PH_S_PASSWORD, "${password}").
-define(PH_S_CLIENTID, "${clientid}").
-define(PH_S_FROM_CLIENTID, "${from_clientid}").
-define(PH_S_USERNAME, "${username}").
-define(PH_S_FROM_USERNAME, "${from_username}").
-define(PH_S_TOPIC, "${topic}").
-define(PH_S_PAYLOAD, "${payload}").
-define(PH_S_PEERHOST, "${peerhost}").
-define(PH_S_HOST, "${host}").
-define(PH_S_PORT, "${port}").
-define(PH_S_QOS, "${qos}").
-define(PH_S_FLAGS, "${flags}").
-define(PH_S_HEADERS, "${headers}").
-define(PH_S_PROTONAME, "${proto_name}").
-define(PH_S_PROTOVER, "${proto_ver}").
-define(PH_S_KEEPALIVE, "${keepalive}").
-define(PH_S_CLEAR_START, "${clean_start}").
-define(PH_S_EXPIRY_INTERVAL, "${expiry_interval}").
-define(PH_S_PUBLISH_RECEIVED_AT, "${publish_received_at}").
-define(PH_S_MOUNTPOINT, "${mountpoint}").
-define(PH_S_PEERNAME, "${peername}").
-define(PH_S_SOCKNAME, "${sockname}").
-define(PH_S_IS_BRIDGE, "${is_bridge}").
-define(PH_S_CONNECTED_AT, "${connected_at}").
-define(PH_S_TIMESTAMP, "${timestamp}").
-define(PH_S_DISCONNECTED_AT, "${disconnected_at}").
-define(PH_S_NODE, "${node}").
-define(PH_S_REASON, "${reason}").
-define(PH_S_ENDPOINT_NAME, "${endpoint_name}").

-endif.
