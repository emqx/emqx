%%--------------------------------------------------------------------
%% Copyright (c) 2021-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").

-type ip_port() :: tuple() | integer().
-type duration() :: non_neg_integer().
-type duration_s() :: non_neg_integer().
-type bytesize() :: pos_integer().
-type comma_separated_list() :: list().

-typerefl_from_string({ip_port/0, emqx_schema, to_ip_port}).
-typerefl_from_string({duration/0, emqx_schema, to_duration}).
-typerefl_from_string({duration_s/0, emqx_schema, to_duration_s}).
-typerefl_from_string({bytesize/0, emqx_schema, to_bytesize}).
-typerefl_from_string({comma_separated_list/0, emqx_schema, to_comma_separated_list}).

-reflect_type([
    duration/0,
    duration_s/0,
    bytesize/0,
    comma_separated_list/0,
    ip_port/0
]).
-elvis([{elvis_style, dont_repeat_yourself, disable}]).

-export([namespace/0, roots/0, fields/1, desc/1, tags/0]).

-export([proxy_protocol_opts/0]).

-export([mountpoint/0, mountpoint/1, gateway_common_options/0, gateway_schema/1]).

namespace() -> gateway.

tags() ->
    [<<"Gateway">>].

roots() -> [gateway].

fields(gateway) ->
    [
        {coap,
            sc(
                ref(coap),
                #{
                    required => {false, recursively},
                    desc => ?DESC(coap)
                }
            )},
        {lwm2m,
            sc(
                ref(lwm2m),
                #{
                    required => {false, recursively},
                    desc => ?DESC(lwm2m)
                }
            )},
        {exproto,
            sc(
                ref(exproto),
                #{
                    required => {false, recursively},
                    desc => ?DESC(exproto)
                }
            )}
    ] ++ gateway_schemas();
fields(coap) ->
    [
        {heartbeat,
            sc(
                duration(),
                #{
                    default => <<"30s">>,
                    desc => ?DESC(coap_heartbeat)
                }
            )},
        {connection_required,
            sc(
                boolean(),
                #{
                    default => false,
                    desc => ?DESC(coap_connection_required)
                }
            )},
        {notify_type,
            sc(
                hoconsc:enum([non, con, qos]),
                #{
                    default => qos,
                    desc => ?DESC(coap_notify_type)
                }
            )},
        {subscribe_qos,
            sc(
                hoconsc:enum([qos0, qos1, qos2, coap]),
                #{
                    default => coap,
                    desc => ?DESC(coap_subscribe_qos)
                }
            )},
        {publish_qos,
            sc(
                hoconsc:enum([qos0, qos1, qos2, coap]),
                #{
                    default => coap,
                    desc => ?DESC(coap_publish_qos)
                }
            )},
        {mountpoint, mountpoint()},
        {listeners,
            sc(
                ref(udp_listeners),
                #{desc => ?DESC(udp_listeners)}
            )}
    ] ++ gateway_common_options();
fields(lwm2m) ->
    [
        {xml_dir,
            sc(
                binary(),
                #{
                    %% since this is not packaged with emqx, nor
                    %% present in the packages, we must let the user
                    %% specify it rather than creating a dynamic
                    %% default (especially difficult to handle when
                    %% generating docs).
                    example => <<"/etc/emqx/lwm2m_xml">>,
                    required => true,
                    desc => ?DESC(lwm2m_xml_dir)
                }
            )},
        {lifetime_min,
            sc(
                duration(),
                #{
                    default => <<"15s">>,
                    desc => ?DESC(lwm2m_lifetime_min)
                }
            )},
        {lifetime_max,
            sc(
                duration(),
                #{
                    default => <<"86400s">>,
                    desc => ?DESC(lwm2m_lifetime_max)
                }
            )},
        {qmode_time_window,
            sc(
                duration_s(),
                #{
                    default => <<"22s">>,
                    desc => ?DESC(lwm2m_qmode_time_window)
                }
            )},
        %% TODO: Support config resource path
        {auto_observe,
            sc(
                boolean(),
                #{
                    default => false,
                    desc => ?DESC(lwm2m_auto_observe)
                }
            )},
        %% FIXME: not working now
        {update_msg_publish_condition,
            sc(
                hoconsc:enum([always, contains_object_list]),
                #{
                    default => contains_object_list,
                    desc => ?DESC(lwm2m_update_msg_publish_condition)
                }
            )},
        {translators,
            sc(
                ref(lwm2m_translators),
                #{
                    required => true,
                    desc => ?DESC(lwm2m_translators)
                }
            )},
        {mountpoint, mountpoint("lwm2m/${endpoint_name}/")},
        {listeners, sc(ref(udp_listeners), #{desc => ?DESC(udp_listeners)})}
    ] ++ gateway_common_options();
fields(exproto) ->
    [
        {server,
            sc(
                ref(exproto_grpc_server),
                #{
                    required => true,
                    desc => ?DESC(exproto_server)
                }
            )},
        {handler,
            sc(
                ref(exproto_grpc_handler),
                #{
                    required => true,
                    desc => ?DESC(exproto_handler)
                }
            )},
        {mountpoint, mountpoint()},
        {listeners, sc(ref(tcp_udp_listeners), #{desc => ?DESC(tcp_udp_listeners)})}
    ] ++ gateway_common_options();
fields(exproto_grpc_server) ->
    [
        {bind,
            sc(
                hoconsc:union([ip_port(), integer()]),
                #{
                    required => true,
                    desc => ?DESC(exproto_grpc_server_bind)
                }
            )},
        {ssl_options,
            sc(
                ref(ssl_server_opts),
                #{
                    required => {false, recursively},
                    desc => ?DESC(exproto_grpc_server_ssl)
                }
            )}
    ];
fields(exproto_grpc_handler) ->
    [
        {address, sc(binary(), #{required => true, desc => ?DESC(exproto_grpc_handler_address)})},
        {ssl_options,
            sc(
                ref(emqx_schema, "ssl_client_opts"),
                #{
                    required => {false, recursively},
                    desc => ?DESC(exproto_grpc_handler_ssl)
                }
            )}
    ];
fields(ssl_server_opts) ->
    emqx_schema:server_ssl_opts_schema(
        #{
            depth => 10,
            reuse_sessions => true,
            versions => tls_all_available
        },
        true
    );
fields(clientinfo_override) ->
    [
        {username, sc(binary(), #{desc => ?DESC(gateway_common_clientinfo_override_username)})},
        {password,
            sc(binary(), #{
                desc => ?DESC(gateway_common_clientinfo_override_password),
                sensitive => true,
                format => <<"password">>,
                converter => fun emqx_schema:password_converter/2
            })},
        {clientid, sc(binary(), #{desc => ?DESC(gateway_common_clientinfo_override_clientid)})}
    ];
fields(lwm2m_translators) ->
    [
        {command,
            sc(
                ref(translator),
                #{
                    desc => ?DESC(lwm2m_translators_command),
                    required => true
                }
            )},
        {response,
            sc(
                ref(translator),
                #{
                    desc => ?DESC(lwm2m_translators_response),
                    required => true
                }
            )},
        {notify,
            sc(
                ref(translator),
                #{
                    desc => ?DESC(lwm2m_translators_notify),
                    required => true
                }
            )},
        {register,
            sc(
                ref(translator),
                #{
                    desc => ?DESC(lwm2m_translators_register),
                    required => true
                }
            )},
        {update,
            sc(
                ref(translator),
                #{
                    desc => ?DESC(lwm2m_translators_update),
                    required => true
                }
            )}
    ];
fields(translator) ->
    [
        {topic,
            sc(
                binary(),
                #{
                    required => true,
                    desc => ?DESC(translator_topic)
                }
            )},
        {qos,
            sc(
                emqx_schema:qos(),
                #{
                    default => 0,
                    desc => ?DESC(translator_qos)
                }
            )}
    ];
fields(udp_listeners) ->
    [
        {udp, sc(map(name, ref(udp_listener)), #{desc => ?DESC(udp_listener)})},
        {dtls, sc(map(name, ref(dtls_listener)), #{desc => ?DESC(dtls_listener)})}
    ];
fields(tcp_listeners) ->
    [
        {tcp, sc(map(name, ref(tcp_listener)), #{desc => ?DESC(tcp_listener)})},
        {ssl, sc(map(name, ref(ssl_listener)), #{desc => ?DESC(ssl_listener)})}
    ];
fields(tcp_udp_listeners) ->
    [
        {tcp, sc(map(name, ref(tcp_listener)), #{desc => ?DESC(tcp_listener)})},
        {ssl, sc(map(name, ref(ssl_listener)), #{desc => ?DESC(ssl_listener)})},
        {udp, sc(map(name, ref(udp_listener)), #{desc => ?DESC(udp_listener)})},
        {dtls, sc(map(name, ref(dtls_listener)), #{desc => ?DESC(dtls_listener)})}
    ];
fields(tcp_listener) ->
    %% some special configs for tcp listener
    [
        {acceptors, sc(integer(), #{default => 16, desc => ?DESC(tcp_listener_acceptors)})}
    ] ++
        tcp_opts() ++
        proxy_protocol_opts() ++
        common_listener_opts();
fields(ssl_listener) ->
    fields(tcp_listener) ++
        [
            {ssl_options,
                sc(
                    hoconsc:ref(emqx_schema, "listener_ssl_opts"),
                    #{desc => ?DESC(ssl_listener_options)}
                )}
        ];
fields(udp_listener) ->
    [
        %% some special configs for udp listener
    ] ++
        udp_opts() ++
        common_listener_opts();
fields(dtls_listener) ->
    [{acceptors, sc(integer(), #{default => 16, desc => ?DESC(dtls_listener_acceptors)})}] ++
        fields(udp_listener) ++
        [{dtls_options, sc(ref(dtls_opts), #{desc => ?DESC(dtls_listener_dtls_opts)})}];
fields(udp_opts) ->
    [
        {active_n,
            sc(
                integer(),
                #{
                    default => 100,
                    desc => ?DESC(udp_listener_active_n)
                }
            )},
        {recbuf, sc(bytesize(), #{desc => ?DESC(udp_listener_recbuf)})},
        {sndbuf, sc(bytesize(), #{desc => ?DESC(udp_listener_sndbuf)})},
        {buffer, sc(bytesize(), #{desc => ?DESC(udp_listener_buffer)})},
        {reuseaddr, sc(boolean(), #{default => true, desc => ?DESC(udp_listener_reuseaddr)})}
    ];
fields(dtls_opts) ->
    emqx_schema:server_ssl_opts_schema(
        #{
            depth => 10,
            reuse_sessions => true,
            versions => dtls_all_available
        },
        false
    ).

desc(gateway) ->
    "EMQX Gateway configuration root.";
desc(coap) ->
    "The CoAP protocol gateway provides EMQX with the access capability of the CoAP protocol.\n"
    "It allows publishing, subscribing, and receiving messages to EMQX in accordance\n"
    "with a certain defined CoAP message format.";
desc(lwm2m) ->
    "The LwM2M protocol gateway.";
desc(exproto) ->
    "Settings for EMQX extension protocol (exproto).";
desc(exproto_grpc_server) ->
    "Settings for the exproto gRPC server.";
desc(exproto_grpc_handler) ->
    "Settings for the exproto gRPC connection handler.";
desc(ssl_server_opts) ->
    "SSL configuration for the server.";
desc(clientinfo_override) ->
    "ClientInfo override.";
desc(lwm2m_translators) ->
    "MQTT topics that correspond to LwM2M events.";
desc(translator) ->
    "MQTT topic that corresponds to a particular type of event.";
desc(udp_listeners) ->
    "Settings for the UDP listeners.";
desc(tcp_listeners) ->
    "Settings for the TCP listeners.";
desc(tcp_udp_listeners) ->
    "Settings for the listeners.";
desc(tcp_listener) ->
    "Settings for the TCP listener.";
desc(ssl_listener) ->
    "Settings for the SSL listener.";
desc(udp_listener) ->
    "Settings for the UDP listener.";
desc(dtls_listener) ->
    "Settings for the DTLS listener.";
desc(udp_opts) ->
    "Settings for the UDP sockets.";
desc(dtls_opts) ->
    "Settings for the DTLS protocol.";
desc(_) ->
    undefined.

authentication_schema() ->
    sc(
        emqx_authn_schema:authenticator_type(),
        #{
            required => {false, recursively},
            desc => ?DESC(gateway_common_authentication),
            examples => emqx_authn_api:authenticator_examples()
        }
    ).

gateway_common_options() ->
    [
        {enable,
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(gateway_common_enable)
                }
            )},
        {enable_stats,
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(gateway_common_enable_stats)
                }
            )},
        {idle_timeout,
            sc(
                duration(),
                #{
                    default => <<"30s">>,
                    desc => ?DESC(gateway_common_idle_timeout)
                }
            )},
        {clientinfo_override,
            sc(
                ref(clientinfo_override),
                #{desc => ?DESC(gateway_common_clientinfo_override)}
            )},
        {?EMQX_AUTHENTICATION_CONFIG_ROOT_NAME_ATOM, authentication_schema()}
    ].

mountpoint() ->
    mountpoint(<<"">>).
mountpoint(Default) ->
    sc(
        binary(),
        #{
            default => iolist_to_binary(Default),
            desc => ?DESC(gateway_common_mountpoint)
        }
    ).

common_listener_opts() ->
    [
        {enable,
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(gateway_common_listener_enable)
                }
            )},
        {bind,
            sc(
                hoconsc:union([ip_port(), integer()]),
                #{desc => ?DESC(gateway_common_listener_bind)}
            )},
        {max_connections,
            sc(
                integer(),
                #{
                    default => 1024,
                    desc => ?DESC(gateway_common_listener_max_connections)
                }
            )},
        {max_conn_rate,
            sc(
                integer(),
                #{
                    default => 1000,
                    desc => ?DESC(gateway_common_listener_max_conn_rate)
                }
            )},
        {?EMQX_AUTHENTICATION_CONFIG_ROOT_NAME_ATOM, authentication_schema()},
        {"enable_authn",
            sc(
                boolean(),
                #{
                    desc => ?DESC(gateway_common_listener_enable_authn),
                    default => true
                }
            )},
        {mountpoint,
            sc(
                binary(),
                #{
                    default => undefined,
                    desc => ?DESC(gateway_common_listener_mountpoint)
                }
            )},
        {access_rules,
            sc(
                hoconsc:array(string()),
                #{
                    default => [],
                    desc => ?DESC(gateway_common_listener_access_rules)
                }
            )}
    ].

tcp_opts() ->
    [{tcp_options, sc(ref(emqx_schema, "tcp_opts"), #{desc => ?DESC(tcp_listener_tcp_opts)})}].

udp_opts() ->
    [{udp_options, sc(ref(udp_opts), #{})}].

proxy_protocol_opts() ->
    [
        {proxy_protocol,
            sc(
                boolean(),
                #{
                    default => false,
                    desc => ?DESC(tcp_listener_proxy_protocol)
                }
            )},
        {proxy_protocol_timeout,
            sc(
                duration(),
                #{
                    default => <<"15s">>,
                    desc => ?DESC(tcp_listener_proxy_protocol_timeout)
                }
            )}
    ].

%%--------------------------------------------------------------------
%% dynamic schemas

%% FIXME: don't hardcode the gateway names
gateway_schema(coap) -> fields(coap);
gateway_schema(lwm2m) -> fields(lwm2m);
gateway_schema(exproto) -> fields(exproto);
gateway_schema(stomp) -> emqx_stomp_schema:fields(stomp);
gateway_schema(mqttsn) -> emqx_mqttsn_schema:fields(mqttsn).

gateway_schemas() ->
    lists:map(
        fun(#{name := Name, config_schema_module := Mod}) ->
            {Name,
                sc(
                    ref(Mod, Name),
                    #{
                        required => {false, recursively},
                        desc => ?DESC(Name)
                    }
                )}
        end,
        emqx_gateway_utils:find_gateway_definations()
    ).

%%--------------------------------------------------------------------
%% helpers

sc(Type, Meta) ->
    hoconsc:mk(Type, Meta).

map(Name, Type) ->
    hoconsc:map(Name, Type).

ref(StructName) ->
    ref(?MODULE, StructName).

ref(Mod, Field) ->
    hoconsc:ref(Mod, Field).
