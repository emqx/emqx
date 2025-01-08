%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-define(PH_VAR_THIS, <<"$_THIS_">>).

-define(PH(Var), <<"${" Var "}">>).

%% action: publish/subscribe
-define(VAR_ACTION, "action").
-define(PH_ACTION, ?PH(?VAR_ACTION)).

%% cert
-define(VAR_CERT_SUBJECT, "cert_subject").
-define(VAR_CERT_CN_NAME, "cert_common_name").
-define(VAR_CERT_PEM, "cert_pem").
-define(PH_CERT_SUBJECT, ?PH(?VAR_CERT_SUBJECT)).
-define(PH_CERT_CN_NAME, ?PH(?VAR_CERT_CN_NAME)).
-define(PH_CERT_PEM, ?PH(?VAR_CERT_PEM)).

%% MQTT/Gateway
-define(VAR_PASSWORD, "password").
-define(VAR_CLIENTID, "clientid").
-define(VAR_USERNAME, "username").
-define(VAR_TOPIC, "topic").
-define(VAR_ENDPOINT_NAME, "endpoint_name").
-define(VAR_NS_CLIENT_ATTRS, {var_namespace, "client_attrs"}).
-define(VAR_ZONE, "zone").

-define(PH_PASSWORD, ?PH(?VAR_PASSWORD)).
-define(PH_CLIENTID, ?PH(?VAR_CLIENTID)).
-define(PH_FROM_CLIENTID, ?PH("from_clientid")).
-define(PH_USERNAME, ?PH(?VAR_USERNAME)).
-define(PH_FROM_USERNAME, ?PH("from_username")).
-define(PH_TOPIC, ?PH(?VAR_TOPIC)).
%% MQTT payload
-define(PH_PAYLOAD, ?PH("payload")).
%% client IPAddress
-define(VAR_PEERHOST, "peerhost").
-define(PH_PEERHOST, ?PH(?VAR_PEERHOST)).
%% client Port
-define(VAR_PEERPORT, "peerport").
-define(PH_PEERPORT, ?PH(?VAR_PEERPORT)).
%% ip & port
-define(PH_HOST, ?PH("host")).
-define(PH_PORT, ?PH("port")).
%% Enumeration of message QoS 0,1,2
-define(VAR_QOS, "qos").
-define(PH_QOS, ?PH(?VAR_QOS)).
-define(PH_FLAGS, ?PH("flags")).
%% Additional data related to process within the MQTT message
-define(PH_HEADERS, ?PH("headers")).
%% protocol name
-define(VAR_PROTONAME, "proto_name").
-define(PH_PROTONAME, ?PH(?VAR_PROTONAME)).
%% protocol version
-define(PH_PROTOVER, ?PH("proto_ver")).
%% MQTT keepalive interval
-define(PH_KEEPALIVE, ?PH("keepalive")).
%% MQTT clean_start
-define(PH_CLEAR_START, ?PH("clean_start")).
%% MQTT Session Expiration time
-define(PH_EXPIRY_INTERVAL, ?PH("expiry_interval")).

%% Time when PUBLISH message reaches Broker (ms)
-define(PH_PUBLISH_RECEIVED_AT, ?PH("publish_received_at")).
%% Mountpoint for bridging messages
-define(VAR_MOUNTPOINT, "mountpoint").
-define(PH_MOUNTPOINT, ?PH(?VAR_MOUNTPOINT)).
%% IPAddress and Port of terminal
-define(PH_PEERNAME, ?PH("peername")).
%% IPAddress and Port listened by emqx
-define(PH_SOCKNAME, ?PH("sockname")).
%% whether it is MQTT bridge connection
-define(PH_IS_BRIDGE, ?PH("is_bridge")).
%% Terminal connection completion time (s)
-define(PH_CONNECTED_AT, ?PH("connected_at")).
%% Event trigger time(millisecond)
-define(PH_TIMESTAMP, ?PH("timestamp")).
%% Terminal disconnection completion time (s)
-define(PH_DISCONNECTED_AT, ?PH("disconnected_at")).

-define(PH_NODE, ?PH("node")).
-define(PH_REASON, ?PH("reason")).

-define(PH_ENDPOINT_NAME, ?PH(?VAR_ENDPOINT_NAME)).
-define(VAR_RETAIN, "retain").
-define(PH_RETAIN, ?PH(?VAR_RETAIN)).

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
-define(PH_S_PEERPORT, "${peerport}").
-define(PH_S_SOCKNAME, "${sockname}").
-define(PH_S_IS_BRIDGE, "${is_bridge}").
-define(PH_S_CONNECTED_AT, "${connected_at}").
-define(PH_S_TIMESTAMP, "${timestamp}").
-define(PH_S_DISCONNECTED_AT, "${disconnected_at}").
-define(PH_S_NODE, "${node}").
-define(PH_S_REASON, "${reason}").
-define(PH_S_ENDPOINT_NAME, "${endpoint_name}").
-define(PH_S_RETAIN, "${retain}").

-endif.
