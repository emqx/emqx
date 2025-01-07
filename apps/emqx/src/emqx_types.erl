%%--------------------------------------------------------------------
%% Copyright (c) 2018-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-export_type([
    proto_ver/0,
    qos/0,
    qos_name/0
]).

-export_type([
    zone/0,
    pubsub/0,
    pubsub_action/0,
    subid/0
]).

-export_type([
    group/0,
    topic/0,
    word/0,
    words/0
]).

-export_type([
    share/0
]).

-export_type([
    socktype/0,
    sockstate/0,
    conninfo/0,
    clientinfo/0,
    tns/0,
    clientid/0,
    username/0,
    password/0,
    peerhost/0,
    peername/0,
    protocol/0,
    client_attrs/0
]).

-export_type([
    connack/0,
    subopts/0,
    reason_code/0,
    alias_id/0,
    topic_aliases/0,
    properties/0
]).

-export_type([
    packet_id/0,
    packet_type/0,
    packet/0
]).

-export_type([
    subscription/0,
    subscriber/0,
    topic_filters/0
]).

-export_type([
    payload/0,
    message/0,
    flag/0,
    flags/0,
    headers/0
]).

-export_type([
    deliver/0,
    delivery/0,
    publish_result/0,
    deliver_result/0
]).

-export_type([
    route/0,
    route_entry/0
]).

-export_type([
    banned/0,
    banned_who/0,
    command/0
]).

-export_type([
    caps/0,
    channel_attrs/0,
    infos/0,
    stats/0
]).

-export_type([oom_policy/0]).

-export_type([takeover_data/0]).

-export_type([startlink_ret/0]).

-type proto_ver() ::
    ?MQTT_PROTO_V3
    | ?MQTT_PROTO_V4
    | ?MQTT_PROTO_V5
    | non_neg_integer()
    % For lwm2m, mqtt-sn...
    | binary().

-type qos() :: ?QOS_0 | ?QOS_1 | ?QOS_2.
-type qos_name() ::
    qos0
    | at_most_once
    | qos1
    | at_least_once
    | qos2
    | exactly_once.

-type zone() :: atom().
-type pubsub_action() :: publish | subscribe.

-type pubsub() ::
    #{action_type := subscribe, qos := qos()}
    | #{action_type := publish, qos := qos(), retain := boolean()}.

-type subid() :: binary() | atom().

-type group() :: binary().
-type topic() :: binary().
-type word() :: '' | '+' | '#' | binary().
-type words() :: list(word()).

-type share() :: #share{}.

-type socktype() :: tcp | udp | ssl | proxy | atom().
-type sockstate() :: idle | running | blocked | closed.
-type conninfo() :: #{
    socktype := socktype(),
    sockname := peername(),
    peername := peername(),
    peercert => nossl | undefined | esockd_peercert:peercert(),
    conn_mod := module(),
    proto_name => binary(),
    proto_ver => proto_ver(),
    clean_start => boolean(),
    clientid => clientid(),
    username => username(),
    conn_props => properties(),
    connected => boolean(),
    connected_at => non_neg_integer(),
    disconnected_at => non_neg_integer(),
    keepalive => 0..16#FFFF,
    receive_maximum => non_neg_integer(),
    expiry_interval => non_neg_integer(),
    atom() => term()
}.
-type clientinfo() :: #{
    zone := option(zone()),
    protocol := protocol(),
    peerhost := peerhost(),
    sockport := non_neg_integer(),
    clientid := clientid(),
    username := username(),
    is_bridge := boolean(),
    is_superuser := boolean(),
    mountpoint := option(binary()),
    ws_cookie => option(list()),
    password => option(binary()),
    auth_result => auth_result(),
    anonymous => boolean(),
    cn => binary(),
    dn => binary(),
    %% Extra client attributes, commented out for bpapi spec backward compatibility.
    %% This field is never used in RPC calls.
    %% client_attrs => client_attrs(),
    atom() => term()
}.
-type client_attrs() :: #{binary() => binary()}.
-type tns() :: binary().
-type clientid() :: binary() | atom().
-type username() :: option(binary()).
-type password() :: option(binary()).
-type peerhost() :: inet:ip_address().
-type peername() ::
    {inet:ip_address(), inet:port_number()}
    | inet:returned_non_ip_address().
-type protocol() :: mqtt | 'mqtt-sn' | coap | lwm2m | stomp | none | atom().
-type auth_result() ::
    success
    | client_identifier_not_valid
    | bad_username_or_password
    | bad_clientid_or_password
    | not_authorized
    | server_unavailable
    | server_busy
    | banned
    | bad_authentication_method.

-type packet_type() :: ?RESERVED..?AUTH.
-type connack() :: ?CONNACK_ACCEPT..?CONNACK_AUTH.
-type subopts() :: #{
    rh := 0 | 1 | 2,
    rap := 0 | 1,
    nl := 0 | 1,
    qos := qos(),
    atom() => term()
}.
-type reason_code() :: 0..16#FF.
-type packet_id() :: 1..16#FFFF.
-type alias_id() :: 0..16#FFFF.
-type topic_aliases() :: #{
    inbound => option(map()),
    outbound => option(map())
}.
-type properties() :: #{atom() => term()}.
-type topic_filters() :: list({topic(), subopts()}).
-type packet() :: #mqtt_packet{}.

-type subscription() :: #subscription{}.
-type subscriber() :: {pid(), subid()}.
-type payload() :: binary() | iodata().
-type message() :: #message{}.
-type flag() :: sys | dup | retain | atom().
-type flags() :: #{flag() := boolean()}.
-type headers() :: #{
    proto_ver => proto_ver(),
    protocol => protocol(),
    username => username(),
    peerhost => peerhost(),
    properties => properties(),
    allow_publish => boolean(),
    atom() => term()
}.

-type banned() :: #banned{}.
-type banned_who() ::
    {clientid, binary()}
    | {peerhost, inet:ip_address()}
    | {username, binary()}
    | {clientid_re, {_RE :: tuple(), binary()}}
    | {username_re, {_RE :: tuple(), binary()}}
    | {peerhost_net, esockd_cidr:cidr()}.

-type deliver() :: {deliver, topic(), message()}.
-type delivery() :: #delivery{}.
-type deliver_result() :: ok | {ok, non_neg_integer()} | {error, term()}.
-type publish_result() ::
    [
        {node(), topic(), deliver_result()}
        | {share, topic(), deliver_result()}
        | {emqx_external_broker:dest(), topic(), deliver_result()}
        | persisted
    ]
    %% If schema validation failure action is set to `disconnect'.
    | disconnect
    %% If caller specifies `hook_prohibition_as_error => true'.
    | {blocked, message()}.
-type route() :: #route{}.
-type route_entry() :: {topic(), node()} | {topic, group()}.
-type command() :: #command{}.

-type caps() :: emqx_mqtt_caps:caps().
-type channel_attrs() :: #{atom() => term()}.
-type infos() :: #{atom() => term()}.
-type stats() :: [{atom(), term()}].

-type oom_policy() :: #{
    max_mailbox_size => non_neg_integer(),
    max_heap_size => non_neg_integer(),
    enable => boolean()
}.

-type takeover_data() :: map().
