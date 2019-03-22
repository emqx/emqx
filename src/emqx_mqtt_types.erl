%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mqtt_types).

-include("emqx_mqtt.hrl").

-export_type([version/0, qos/0, qos_name/0]).
-export_type([connack/0, reason_code/0]).
-export_type([properties/0, subopts/0]).
-export_type([topic_filters/0]).
-export_type([packet_id/0, packet_type/0, packet/0]).

-type(qos() :: ?QOS_0 | ?QOS_1 | ?QOS_2).
-type(version() :: ?MQTT_PROTO_V3 | ?MQTT_PROTO_V4 | ?MQTT_PROTO_V5).
-type(qos_name() :: qos0 | at_most_once |
                    qos1 | at_least_once |
                    qos2 | exactly_once).
-type(packet_type() :: ?RESERVED..?AUTH).
-type(connack() :: ?CONNACK_ACCEPT..?CONNACK_AUTH).
-type(reason_code() :: 0..16#FF).
-type(packet_id() :: 1..16#FFFF).
-type(properties() :: #{atom() => term()}).
-type(subopts() :: #{rh  := 0 | 1 | 2,
                     rap := 0 | 1,
                     nl  := 0 | 1,
                     qos := qos(),
                     rc  => reason_code()
                    }).
-type(topic_filters() :: [{emqx_topic:topic(), subopts()}]).
-type(packet() :: #mqtt_packet{}).

