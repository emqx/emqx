%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_exproto_schema).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").

-type ip_port() :: tuple() | integer().

-typerefl_from_string({ip_port/0, emqx_schema, to_ip_port}).

-reflect_type([
    ip_port/0
]).

%% config schema provides
-export([namespace/0, fields/1, desc/1]).

namespace() -> "gateway".

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
        {mountpoint, emqx_gateway_schema:mountpoint()},
        {listeners,
            sc(ref(emqx_gateway_schema, tcp_udp_listeners), #{desc => ?DESC(tcp_udp_listeners)})}
    ] ++ emqx_gateway_schema:gateway_common_options();
fields(exproto_grpc_server) ->
    [
        {bind,
            sc(
                ip_port(),
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
        {service_name,
            sc(
                hoconsc:union(['ConnectionHandler', 'ConnectionUnaryHandler']),
                #{
                    required => true,
                    default => 'ConnectionUnaryHandler',
                    desc => ?DESC(exproto_grpc_handler_service_name)
                }
            )},
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
    ).

desc(exproto) ->
    "Settings for EMQX extension protocol (exproto).";
desc(exproto_grpc_server) ->
    "Settings for the exproto gRPC server.";
desc(exproto_grpc_handler) ->
    "Settings for the exproto gRPC connection handler.";
desc(ssl_server_opts) ->
    "SSL configuration for the server.";
desc(_) ->
    undefined.

%%--------------------------------------------------------------------
%% helpers

sc(Type, Meta) ->
    hoconsc:mk(Type, Meta).

ref(StructName) ->
    ref(?MODULE, StructName).

ref(Mod, Field) ->
    hoconsc:ref(Mod, Field).
