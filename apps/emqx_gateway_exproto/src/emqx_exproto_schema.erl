%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
