%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx_auth/include/emqx_authn_chains.hrl").

-type ip_port() :: tuple() | integer().
-type duration() :: non_neg_integer().
-type duration_s() :: non_neg_integer().
-type bytesize() :: pos_integer().

-typerefl_from_string({ip_port/0, emqx_schema, to_ip_port}).
-typerefl_from_string({duration/0, emqx_schema, to_duration}).
-typerefl_from_string({duration_s/0, emqx_schema, to_duration_s}).
-typerefl_from_string({bytesize/0, emqx_schema, to_bytesize}).

-reflect_type([
    duration/0,
    duration_s/0,
    bytesize/0,
    ip_port/0
]).
-elvis([{elvis_style, dont_repeat_yourself, disable}]).
-elvis([{elvis_style, invalid_dynamic_call, disable}]).

-export([namespace/0, roots/0, fields/1, desc/1, tags/0]).

-export([proxy_protocol_opts/0]).

-export([mountpoint/0, mountpoint/1, gateway_common_options/0, gateway_schema/1, gateway_names/0]).

-export([ws_listener/0, wss_listener/0, ws_opts/2]).

namespace() -> gateway.

tags() ->
    [<<"Gateway">>].

roots() ->
    [{gateway, sc(ref(?MODULE, gateway), #{importance => ?IMPORTANCE_LOW})}].

fields(gateway) ->
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
        emqx_gateway_utils:find_gateway_definitions()
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
fields(udp_listeners) ->
    [
        {udp, sc(map(name, ref(udp_listener)), #{desc => ?DESC(listener_name_to_settings_map)})},
        {dtls, sc(map(name, ref(dtls_listener)), #{desc => ?DESC(listener_name_to_settings_map)})}
    ];
fields(tcp_listeners) ->
    [
        {tcp, sc(map(name, ref(tcp_listener)), #{desc => ?DESC(listener_name_to_settings_map)})},
        {ssl, sc(map(name, ref(ssl_listener)), #{desc => ?DESC(listener_name_to_settings_map)})}
    ];
fields(tcp_udp_listeners) ->
    [
        {tcp, sc(map(name, ref(tcp_listener)), #{desc => ?DESC(listener_name_to_settings_map)})},
        {ssl, sc(map(name, ref(ssl_listener)), #{desc => ?DESC(listener_name_to_settings_map)})},
        {udp, sc(map(name, ref(udp_listener)), #{desc => ?DESC(listener_name_to_settings_map)})},
        {dtls, sc(map(name, ref(dtls_listener)), #{desc => ?DESC(listener_name_to_settings_map)})}
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
                    #{
                        desc => ?DESC(ssl_listener_options),
                        validator => fun emqx_schema:validate_server_ssl_opts/1
                    }
                )}
        ];
fields(ws_listener) ->
    emqx_gateway_schema:ws_listener() ++
        [{websocket, sc(ref(websocket), #{})}];
fields(wss_listener) ->
    emqx_gateway_schema:wss_listener() ++
        [{websocket, sc(ref(websocket), #{})}];
fields(websocket) ->
    DefaultPath = <<>>,
    SubProtocols = <<>>,
    emqx_gateway_schema:ws_opts(DefaultPath, SubProtocols);
fields(udp_listener) ->
    [
        %% some special configs for udp listener
        {health_check,
            sc(
                ref(udp_health_check),
                #{
                    desc => ?DESC(
                        udp_health_check
                    ),
                    required => false
                }
            )}
    ] ++
        udp_opts() ++
        common_listener_opts();
fields(dtls_listener) ->
    [{acceptors, sc(integer(), #{default => 16, desc => ?DESC(dtls_listener_acceptors)})}] ++
        fields(udp_listener) ++
        [
            {dtls_options,
                sc(ref(dtls_opts), #{
                    desc => ?DESC(dtls_listener_dtls_opts),
                    validator => fun emqx_schema:validate_server_ssl_opts/1
                })}
        ];
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
        _IsRanchListener = false
    );
fields(udp_health_check) ->
    [
        {request, sc(binary(), #{desc => ?DESC(udp_health_check_request), required => false})},
        {reply, sc(binary(), #{desc => ?DESC(udp_health_check_reply), required => false})}
    ].

desc(gateway) ->
    "EMQX Gateway configuration root.";
desc(clientinfo_override) ->
    "ClientInfo override.";
desc(udp_listeners) ->
    "Settings for the UDP listeners.";
desc(tcp_listeners) ->
    "Settings for the TCP listeners.";
desc(tcp_udp_listeners) ->
    "Settings for TCP and UDP listeners.";
desc(tcp_listener) ->
    "Settings for TCP listener.";
desc(ssl_listener) ->
    "Settings for SSL listener.";
desc(udp_listener) ->
    "Settings for UDP listener.";
desc(dtls_listener) ->
    "Settings for DTLS listener.";
desc(udp_opts) ->
    "Settings for UDP sockets.";
desc(dtls_opts) ->
    "Settings for DTLS protocol.";
desc(websocket) ->
    "Websocket options";
desc(udp_health_check) ->
    "UDP health check";
desc(_) ->
    undefined.

authentication_schema() ->
    sc(
        emqx_authn_schema:authenticator_type(),
        #{
            required => {false, recursively},
            desc => ?DESC(gateway_common_authentication),
            %% we do not expose this to the user for now
            importance => ?IMPORTANCE_HIDDEN,
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
                    importance => ?IMPORTANCE_NO_DOC,
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
            desc => ?DESC(gateway_mountpoint)
        }
    ).

ws_listener() ->
    [
        {acceptors, sc(integer(), #{default => 16, desc => ?DESC(tcp_listener_acceptors)})}
    ] ++
        tcp_opts() ++
        proxy_protocol_opts() ++
        common_listener_opts().

wss_listener() ->
    ws_listener() ++
        [
            {ssl_options,
                sc(
                    hoconsc:ref(emqx_schema, "listener_wss_opts"),
                    #{
                        desc => ?DESC(ssl_listener_options),
                        validator => fun emqx_schema:validate_server_ssl_opts/1
                    }
                )}
        ].

ws_opts(DefaultPath, DefaultSubProtocols) when
    is_binary(DefaultPath), is_binary(DefaultSubProtocols)
->
    [
        {"path",
            sc(
                string(),
                #{
                    default => DefaultPath,
                    desc => ?DESC(fields_ws_opts_path)
                }
            )},
        {"piggyback",
            sc(
                hoconsc:enum([single, multiple]),
                #{
                    default => single,
                    desc => ?DESC(fields_ws_opts_piggyback)
                }
            )},
        {"compress",
            sc(
                boolean(),
                #{
                    default => false,
                    desc => ?DESC(fields_ws_opts_compress)
                }
            )},
        {"idle_timeout",
            sc(
                duration(),
                #{
                    default => <<"7200s">>,
                    desc => ?DESC(fields_ws_opts_idle_timeout)
                }
            )},
        {"max_frame_size",
            sc(
                hoconsc:union([infinity, integer()]),
                #{
                    default => infinity,
                    desc => ?DESC(fields_ws_opts_max_frame_size)
                }
            )},
        {"fail_if_no_subprotocol",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(fields_ws_opts_fail_if_no_subprotocol)
                }
            )},
        {"supported_subprotocols",
            sc(
                emqx_schema:comma_separated_list(),
                #{
                    default => DefaultSubProtocols,
                    desc => ?DESC(fields_ws_opts_supported_subprotocols)
                }
            )},
        {"check_origin_enable",
            sc(
                boolean(),
                #{
                    default => false,
                    desc => ?DESC(fields_ws_opts_check_origin_enable)
                }
            )},
        {"allow_origin_absence",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(fields_ws_opts_allow_origin_absence)
                }
            )},
        {"check_origins",
            sc(
                emqx_schema:comma_separated_binary(),
                #{
                    default => <<"http://localhost:18083, http://127.0.0.1:18083">>,
                    desc => ?DESC(fields_ws_opts_check_origins)
                }
            )},
        {"proxy_address_header",
            sc(
                string(),
                #{
                    default => <<"x-forwarded-for">>,
                    desc => ?DESC(fields_ws_opts_proxy_address_header)
                }
            )},
        {"proxy_port_header",
            sc(
                string(),
                #{
                    default => <<"x-forwarded-port">>,
                    desc => ?DESC(fields_ws_opts_proxy_port_header)
                }
            )},
        {"deflate_opts",
            sc(
                ref(emqx_schema, "deflate_opts"),
                #{}
            )}
    ].

common_listener_opts() ->
    [
        {enable,
            sc(
                boolean(),
                #{
                    default => true,
                    importance => ?IMPORTANCE_NO_DOC,
                    desc => ?DESC(gateway_common_listener_enable)
                }
            )},
        {bind,
            sc(
                ip_port(),
                #{desc => ?DESC(gateway_common_listener_bind)}
            )},
        {max_connections,
            sc(
                hoconsc:union([pos_integer(), infinity]),
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
                    desc => ?DESC(gateway_mountpoint)
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
                    default => <<"3s">>,
                    desc => ?DESC(tcp_listener_proxy_protocol_timeout)
                }
            )}
    ].

%%--------------------------------------------------------------------
%% dynamic schemas

gateway_schema(Name) ->
    case emqx_gateway_utils:find_gateway_definition(Name) of
        {ok, #{config_schema_module := SchemaMod}} ->
            SchemaMod:fields(Name);
        {error, _} = Error ->
            throw(Error)
    end.

gateway_names() ->
    Definations = emqx_gateway_utils:find_gateway_definitions(),
    [
        Name
     || #{name := Name} = Defination <- Definations,
        emqx_gateway_utils:check_gateway_edition(Defination)
    ].
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
