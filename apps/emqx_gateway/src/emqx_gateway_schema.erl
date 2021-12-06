%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_gateway_schema).

-behaviour(hocon_schema).

-dialyzer(no_return).
-dialyzer(no_match).
-dialyzer(no_contracts).
-dialyzer(no_unused).
-dialyzer(no_fail_call).

-include_lib("emqx/include/emqx_authentication.hrl").
-include_lib("typerefl/include/types.hrl").

-type ip_port() :: tuple().
-type duration() :: integer().
-type duration_s() :: integer().
-type bytesize() :: integer().
-type comma_separated_list() :: list().

-typerefl_from_string({ip_port/0, emqx_schema, to_ip_port}).
-typerefl_from_string({duration/0, emqx_schema, to_duration}).
-typerefl_from_string({duration_s/0, emqx_schema, to_duration_s}).
-typerefl_from_string({bytesize/0, emqx_schema, to_bytesize}).
-typerefl_from_string({comma_separated_list/0, emqx_schema,
                       to_comma_separated_list}).

-reflect_type([ duration/0
              , duration_s/0
              , bytesize/0
              , comma_separated_list/0
              , ip_port/0
              ]).
-elvis([{elvis_style, dont_repeat_yourself, disable}]).

-export([namespace/0, roots/0 , fields/1]).

namespace() -> gateway.

roots() -> [gateway].

fields(gateway) ->
    [{stomp,
      sc(ref(stomp),
         #{ nullable => {true, recursively}
          , desc =>
"The Stomp Gateway configuration.<br>
This gateway supports v1.2/1.1/1.0"
          })},
     {mqttsn,
      sc(ref(mqttsn),
         #{ nullable => {true, recursively}
          , desc =>
"The MQTT-SN Gateway configuration.<br>
This gateway only supports the v1.2 protocol"
          })},
     {coap,
      sc(ref(coap),
         #{ nullable => {true, recursively}
          , desc =>
"The CoAP Gateway configuration.<br>
This gateway is implemented based on RFC-7252 and
https://core-wg.github.io/coap-pubsub/draft-ietf-core-pubsub.html"
          })},
     {lwm2m,
      sc(ref(lwm2m),
         #{ nullable => {true, recursively}
          , desc =>
"The LwM2M Gateway configuration.<br>
This gateway only supports the v1.0.1 protocol"
          })},
     {exproto,
      sc(ref(exproto),
         #{ nullable => {true, recursively}
          , desc => "The Extension Protocol configuration"
          })}
    ];

fields(stomp) ->
    [ {frame, sc(ref(stomp_frame))}
    , {listeners, sc(ref(tcp_listeners))}
    ] ++ gateway_common_options();

fields(stomp_frame) ->
    [ {max_headers,
       sc(integer(),
          #{ default => 10
           , desc => "The maximum number of Header"
           })}
    , {max_headers_length,
       sc(integer(),
          #{ default => 1024
           , desc => "The maximum string length of the Header Value"
           })}
    , {max_body_length,
       sc(integer(),
          #{ default => 65536
           , desc => "Maximum number of bytes of Body allowed per Stomp packet"
           })}
    ];

fields(mqttsn) ->
    [ {gateway_id,
       sc(integer(),
          #{ default => 1
           , desc =>
"MQTT-SN Gateway Id.<br>
When the <code>broadcast</code> option is enabled,
the gateway will broadcast ADVERTISE message with this value"
           })}
    , {broadcast,
       sc(boolean(),
          #{ default => false
           , desc => "Whether to periodically broadcast ADVERTISE messages"
           })}
    %% TODO: rename
    , {enable_qos3,
       sc(boolean(),
          #{ default => true
           , desc =>
"Allows connectionless clients to publish messages with a Qos of -1.<br>
This feature is defined for very simple client implementations
which do not support any other features except this one.<br>
There is no connection setup nor tear down, no registration nor subscription.<br>
The client just sends its PUBLISH messages to a GW"
           })}
    , {predefined,
       sc(hoconsc:array(ref(mqttsn_predefined)),
          #{ default => []
           , desc =>
"The Pre-defined topic ids and topic names.<br>
A 'pre-defined' topic id is a topic id whose mapping to a topic name
is known in advance by both the client's application and the gateway"
           })}
    , {listeners, sc(ref(udp_listeners))}
    ] ++ gateway_common_options();

fields(mqttsn_predefined) ->
    [ {id, sc(integer(), #{desc => "Topic Id.<br>Range: 1-65535"})}
    , {topic, sc(binary(), #{desc => "Topic Name"})}
    ];

fields(coap) ->
    [ {heartbeat,
       sc(duration(),
          #{ default => <<"30s">>
           , desc =>
"The gateway server required minimum hearbeat interval.<br>
When connection mode is enabled, this parameter is used to set the minimum
heartbeat interval for the connection to be alive."
           })}
    , {connection_required,
       sc(boolean(),
          #{ default => false
           , desc =>
"Enable or disable connection mode.<br>
Connection mode is a feature of non-standard protocols. When connection mode
is enabled, it is necessary to maintain the creation, authentication and alive
of connection resources"
           })}
    , {notify_type,
       sc(hoconsc:union([non, con, qos]),
          #{ default => qos
           , desc =>
"The Notification Message will be delivered to the CoAP client if a new message
received on an observed topic.
The type of delivered coap message can be set to:<br>
1. non: Non-confirmable;<br>
2. con: Confirmable;<br>
3. qos: Mapping from QoS type of recevied message, QoS0 -> non, QoS1,2 -> con"
           })}
    , {subscribe_qos,
       sc(hoconsc:union([qos0, qos1, qos2, coap]),
          #{ default => coap
           , desc =>
"The Default QoS Level indicator for subscribe request.<br>
This option specifies the QoS level for the CoAP Client when establishing a
subscription membership, if the subscribe request is not carried `qos` option.
The indicator can be set to:
  - qos0, qos1, qos2: Fixed default QoS level
  - coap: Dynamic QoS level by the message type of subscribe request
    * qos0: If the subscribe request is non-confirmable
    * qos1: If the subscribe request is confirmable"
           })}
    , {publish_qos,
       sc(hoconsc:union([qos0, qos1, qos2, coap]),
          #{ default => coap
           , desc =>
"The Default QoS Level indicator for publish request.<br>
This option specifies the QoS level for the CoAP Client when publishing a
message to EMQ X PUB/SUB system, if the publish request is not carried `qos`
option. The indicator can be set to:
  - qos0, qos1, qos2: Fixed default QoS level
  - coap: Dynamic QoS level by the message type of publish request
    * qos0: If the publish request is non-confirmable
    * qos1: If the publish request is confirmable"
           })}
    , {listeners, sc(ref(udp_listeners))}
    ] ++ gateway_common_options();

fields(lwm2m) ->
    [ {xml_dir,
       sc(binary(),
          #{ default =>"etc/lwm2m_xml"
           , desc => "The Directory for LwM2M Resource defination"
           })}
    , {lifetime_min,
       sc(duration(),
          #{ default => "1s"
           , desc => "Minimum value of lifetime allowed to be set by the LwM2M client"
           })}
    , {lifetime_max,
       sc(duration(),
          #{ default => "86400s"
           , desc => "Maximum value of lifetime allowed to be set by the LwM2M client"
           })}
    , {qmode_time_window,
       sc(duration_s(),
          #{ default => "22s"
           , desc =>
"The value of the time window during which the network link is considered
valid by the LwM2M Gateway in QMode mode.<br>
For example, after receiving an update message from a client, any messages
within this time window are sent directly to the LwM2M client, and all messages
beyond this time window are temporarily stored in memory."
           })}
    %% TODO: Support config resource path
    , {auto_observe,
       sc(boolean(),
          #{ default => false
           , desc => "Automatically observe the object list of REGISTER packet"
           })}
    %% FIXME: not working now
    , {update_msg_publish_condition,
       sc(hoconsc:union([always, contains_object_list]),
          #{ default => "contains_object_list"
           , desc =>
"Policy for publishing UPDATE event message to EMQ X.<br>
  - always: send update events as long as the UPDATE request is received.
  - contains_object_list: send update events only if the UPDATE request carries any Object List."
           })}
    , {translators,
       sc(ref(lwm2m_translators),
          #{ nullable => false
           , desc => "Topic configuration for LwM2M's gateway publishing and subscription"
           })}
    , {listeners, sc(ref(udp_listeners))}
    ] ++ gateway_common_options();

fields(exproto) ->
    [ {server,
       sc(ref(exproto_grpc_server),
          #{ desc => "Configurations for starting the <code>ConnectionAdapter</code> service"
           })}
    , {handler,
       sc(ref(exproto_grpc_handler),
          #{ desc => "Configurations for request to <code>ConnectionHandler</code> service"
           })}
    , {listeners, sc(ref(udp_tcp_listeners))}
    ] ++ gateway_common_options();

fields(exproto_grpc_server) ->
    [ {bind,
       sc(hoconsc:union([ip_port(), integer()]))}
    , {ssl,
       sc(ref(ssl_server_opts),
          #{ nullable => {true, recursively}
           })}
    ];

fields(exproto_grpc_handler) ->
    [ {address, sc(binary())}
    , {ssl,
       sc(ref(ssl_client_opts),
          #{ nullable => {true, recursively}
           })}
    ];

fields(ssl_server_opts) ->
    emqx_schema:server_ssl_opts_schema(
      #{ depth => 10
       , reuse_sessions => true
       , versions => tls_all_available
       , ciphers => tls_all_available
       }, true);

fields(ssl_client_opts) ->
    emqx_schema:client_ssl_opts_schema(#{});

fields(clientinfo_override) ->
    [ {username, sc(binary())}
    , {password, sc(binary())}
    , {clientid, sc(binary())}
    ];

fields(lwm2m_translators) ->
    [ {command,
       sc(ref(translator),
          #{ desc =>
"The topic for receiving downstream commands.<br>
For each new LwM2M client that succeeds in going online, the gateway creates
a the subscription relationship to receive downstream commands and send it to
the LwM2M client"
           })}
    , {response,
       sc(ref(translator),
          #{ desc =>
"The topic for gateway to publish the acknowledge events from LwM2M client"
           })}
    , {notify,
       sc(ref(translator),
          #{ desc =>
"The topic for gateway to publish the notify events from LwM2M client.<br>
After succeed observe a resource of LwM2M client, Gateway will send the
notifyevents via this topic, if the client reports any resource changes"
           })}
    , {register,
       sc(ref(translator),
          #{ desc =>
"The topic for gateway to publish the register events from LwM2M client.<br>"
           })}
    , {update,
       sc(ref(translator),
          #{ desc =>
"The topic for gateway to publish the update events from LwM2M client.<br>"
           })}
    ];

fields(translator) ->
    [ {topic, sc(binary())}
    , {qos, sc(range(0, 2), #{default => 0})}
    ];

fields(udp_listeners) ->
    [ {udp, sc(map(name, ref(udp_listener)))}
    , {dtls, sc(map(name, ref(dtls_listener)))}
    ];

fields(tcp_listeners) ->
    [ {tcp, sc(map(name, ref(tcp_listener)))}
    , {ssl, sc(map(name, ref(ssl_listener)))}
    ];

fields(udp_tcp_listeners) ->
    [ {udp, sc(map(name, ref(udp_listener)))}
    , {dtls, sc(map(name, ref(dtls_listener)))}
    , {tcp, sc(map(name, ref(tcp_listener)))}
    , {ssl, sc(map(name, ref(ssl_listener)))}
    ];

fields(tcp_listener) ->
    [ %% some special confs for tcp listener
      {acceptors, sc(integer(), #{default => 16})}
    ] ++
    tcp_opts() ++
    proxy_protocol_opts() ++
    common_listener_opts();

fields(ssl_listener) ->
    fields(tcp_listener) ++
    [{ssl,
      sc(hoconsc:ref(emqx_schema, "listener_ssl_opts"),
         #{ desc => "SSL listener options"
          })}
    ];

fields(udp_listener) ->
    [
     %% some special confs for udp listener
    ] ++
    udp_opts() ++
    common_listener_opts();

fields(dtls_listener) ->
    [ {acceptors, sc(integer(), #{default => 16})}
    ] ++
    fields(udp_listener) ++
    [{dtls, sc(ref(dtls_opts), #{desc => "DTLS listener options"})}];

fields(udp_opts) ->
    [ {active_n, sc(integer(), #{default => 100})}
    , {recbuf, sc(bytesize())}
    , {sndbuf, sc(bytesize())}
    , {buffer, sc(bytesize())}
    , {reuseaddr, sc(boolean(), #{default => true})}
    ];

fields(dtls_opts) ->
    emqx_schema:server_ssl_opts_schema(
        #{ depth => 10
         , reuse_sessions => true
         , versions => dtls_all_available
         , ciphers => dtls_all_available
         }, false).

authentication_schema() ->
    sc(emqx_authn_schema:authenticator_type(),
       #{ nullable => {true, recursively}
        , desc =>
"""Default authentication configs for all of the gateway listeners.<br>
For per-listener overrides see <code>authentication</code>
in listener configs"""
        }).

gateway_common_options() ->
    [ {enable,
       sc(boolean(),
          #{ default => true
           , desc => "Whether to enable this gateway"
           })}
    , {enable_stats,
       sc(boolean(),
          #{ default => true
           , desc => "Whether to enable client process statistic"
           })}
    , {idle_timeout,
       sc(duration(),
          #{ default => <<"30s">>
           , desc =>
"The idle time of the client connection process.<br>
it has two purposes:
1. A newly created client process that does not receive any client requests
   after that time will be closed directly.
2. A running client process that does not receive any client requests after
   this time will go into hibernation to save resources."
           })}
    , {mountpoint,
       sc(binary(),
          #{ default => <<>>
           %% TODO: variable support?
           , desc => ""
           })}
    , {clientinfo_override,
       sc(ref(clientinfo_override),
          #{ desc => ""
           })}
    , {?EMQX_AUTHENTICATION_CONFIG_ROOT_NAME,  authentication_schema()}
    ].

common_listener_opts() ->
    [ {enable,
       sc(boolean(),
          #{ default => true
           })}
    , {bind,
       sc(hoconsc:union([ip_port(), integer()]),
          #{})}
    , {max_connections,
       sc(integer(),
          #{ default => 1024
           })}
    , {max_conn_rate,
       sc(integer(),
          #{ default => 1000
           })}
    , {?EMQX_AUTHENTICATION_CONFIG_ROOT_NAME,  authentication_schema()}
    , {mountpoint,
       sc(binary(),
          #{ default => undefined
           })}
    , {access_rules,
       sc(hoconsc:array(string()),
          #{ default => []
           })}
    ].

tcp_opts() ->
    [{tcp, sc(ref(emqx_schema, "tcp_opts"), #{})}].

udp_opts() ->
    [{udp, sc(ref(udp_opts), #{})}].

proxy_protocol_opts() ->
    [ {proxy_protocol,
       sc(boolean(),
          #{ default => false
           })}
    , {proxy_protocol_timeout,
       sc(duration(),
          #{ default => "15s"
           })}
    ].

sc(Type) ->
    sc(Type, #{}).

sc(Type, Meta) ->
    hoconsc:mk(Type, Meta).

map(Name, Type) ->
    hoconsc:map(Name, Type).

ref(StructName) ->
    ref(?MODULE, StructName).

ref(Mod, Field) ->
    hoconsc:ref(Mod, Field).
