%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_types).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").
-include("types.hrl").

-export_type([ ver/0
             , qos/0
             , qos_name/0
             ]).

-export_type([ zone/0
             , pubsub/0
             , topic/0
             , subid/0
             ]).

-export_type([ socktype/0
             , sockstate/0
             , conninfo/0
             , clientinfo/0
             , clientid/0
             , username/0
             , password/0
             , peerhost/0
             , peername/0
             , protocol/0
             ]).

-export_type([ connack/0
             , subopts/0
             , reason_code/0
             , alias_id/0
             , topic_aliases/0
             , properties/0
             ]).

-export_type([ packet_id/0
             , packet_type/0
             , packet/0
             ]).

-export_type([ subscription/0
             , subscriber/0
             , topic_filters/0
             ]).

-export_type([ payload/0
             , message/0
             ]).

-export_type([ deliver/0
             , delivery/0
             , publish_result/0
             , deliver_result/0
             ]).

-export_type([ route/0
             , route_entry/0
             ]).

-export_type([ alarm/0
             , plugin/0
             , banned/0
             , command/0
             ]).

-export_type([ caps/0
             , attrs/0
             , infos/0
             , stats/0
             ]).

-export_type([oom_policy/0]).

-type(ver() :: ?MQTT_PROTO_V3
             | ?MQTT_PROTO_V4
             | ?MQTT_PROTO_V5).
-type(qos() :: ?QOS_0 | ?QOS_1 | ?QOS_2).
-type(qos_name() :: qos0 | at_most_once |
                    qos1 | at_least_once |
                    qos2 | exactly_once).

-type(zone() :: emqx_zone:zone()).
-type(pubsub() :: publish | subscribe).
-type(topic() :: emqx_topic:topic()).
-type(subid() :: binary() | atom()).

-type(socktype() :: tcp | udp | ssl | proxy | atom()).
-type(sockstate() :: idle | running | blocked | closed).
-type(conninfo() :: #{socktype := socktype(),
                      sockname := peername(),
                      peername := peername(),
                      peercert := esockd_peercert:peercert(),
                      conn_mod := module(),
                      proto_name := binary(),
                      proto_ver := ver(),
                      clean_start := boolean(),
                      clientid := clientid(),
                      username := username(),
                      conn_props := properties(),
                      connected := boolean(),
                      connected_at := erlang:timestamp(),
                      keepalive := 0..16#FFFF,
                      receive_maximum := non_neg_integer(),
                      expiry_interval := non_neg_integer(),
                      atom() => term()
                     }).
-type(clientinfo() :: #{zone         := zone(),
                        protocol     := protocol(),
                        peerhost     := peerhost(),
                        sockport     := non_neg_integer(),
                        clientid     := clientid(),
                        username     := username(),
                        peercert     := esockd_peercert:peercert(),
                        is_bridge    := boolean(),
                        is_superuser := boolean(),
                        mountpoint   := maybe(binary()),
                        ws_cookie    := maybe(list()),
                        password     => maybe(binary()),
                        auth_result  => auth_result(),
                        anonymous    => boolean(),
                        atom()       => term()
                       }).
-type(clientid() :: binary()|atom()).
-type(username() :: maybe(binary())).
-type(password() :: maybe(binary())).
-type(peerhost() :: inet:ip_address()).
-type(peername() :: {inet:ip_address(), inet:port_number()}).
-type(protocol() :: mqtt | 'mqtt-sn' | coap | lwm2m | stomp | none | atom()).
-type(auth_result() :: success
                     | client_identifier_not_valid
                     | bad_username_or_password
                     | bad_clientid_or_password
                     | not_authorized
                     | server_unavailable
                     | server_busy
                     | banned
                     | bad_authentication_method).

-type(packet_type() :: ?RESERVED..?AUTH).
-type(connack() :: ?CONNACK_ACCEPT..?CONNACK_AUTH).
-type(subopts() :: #{rh  := 0 | 1 | 2,
                     rap := 0 | 1,
                     nl  := 0 | 1,
                     qos := qos(),
                     share  => binary(),
                     atom() => term()
                    }).
-type(reason_code() :: 0..16#FF).
-type(packet_id() :: 1..16#FFFF).
-type(alias_id() :: 0..16#FFFF).
-type(topic_aliases() :: #{inbound => maybe(map()),
                           outbound => maybe(map())}).
-type(properties() :: #{atom() => term()}).
-type(topic_filters() :: list({topic(), subopts()})).
-type(packet() :: #mqtt_packet{}).

-type(subscription() :: #subscription{}).
-type(subscriber() :: {pid(), subid()}).
-type(payload() :: binary() | iodata()).
-type(message() :: #message{}).
-type(banned() :: #banned{}).
-type(deliver() :: {deliver, topic(), message()}).
-type(delivery() :: #delivery{}).
-type(deliver_result() :: ok | {error, term()}).
-type(publish_result() :: [{node(), topic(), deliver_result()} |
                           {share, topic(), deliver_result()}]).
-type(route() :: #route{}).
-type(sub_group() :: tuple() | binary()).
-type(route_entry() :: {topic(), node()} | {topic, sub_group()}).
-type(alarm() :: #alarm{}).
-type(plugin() :: #plugin{}).
-type(command() :: #command{}).

-type(caps() :: emqx_mqtt_caps:caps()).
-type(attrs() :: #{atom() => term()}).
-type(infos() :: #{atom() => term()}).
-type(stats() :: #{atom() => non_neg_integer()|stats()}).

-type(oom_policy() :: #{message_queue_len => non_neg_integer(),
                        max_heap_size => non_neg_integer()
                       }).

