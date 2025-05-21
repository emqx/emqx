%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_nats_schema).

-include("emqx_nats.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").

%% config schema provides
-export([namespace/0, fields/1, desc/1]).

namespace() -> "gateway".

fields(nats) ->
    [
        {server_id,
            sc(binary(), #{
                desc => ?DESC(server_id),
                default => <<"emqx_nats_gateway">>
            })},
        {server_name,
            sc(binary(), #{
                desc => ?DESC(server_name),
                default => <<"emqx_nats_gateway">>
            })},
        {protocol, sc(ref(protocol))},
        {mountpoint, emqx_gateway_schema:mountpoint()},
        {listeners, sc(ref(tcp_ws_listeners), #{})}
    ] ++ emqx_gateway_schema:gateway_common_options();
fields(protocol) ->
    [
        {max_payload_size,
            sc(
                non_neg_integer(),
                #{
                    default => ?DEFAULT_MAX_PAYLOAD,
                    desc => ?DESC(max_payload_size)
                }
            )}
    ];
fields(tcp_ws_listeners) ->
    [
        {ws,
            sc(
                map(name, ref(ws_listener)),
                #{
                    desc => ?DESC(ws_listener)
                }
            )},
        {wss,
            sc(
                map(name, ref(wss_listener)),
                #{desc => ?DESC(wss_listener)}
            )}
    ] ++
        emqx_gateway_schema:fields(tcp_listeners);
fields(ws_listener) ->
    [
        {websocket, sc(ref(websocket), #{})}
    ] ++
        emqx_gateway_schema:ws_listener();
fields(wss_listener) ->
    [
        {websocket, sc(ref(websocket), #{})}
    ] ++
        emqx_gateway_schema:wss_listener();
fields(websocket) ->
    Override = #{
        path => <<>>,
        fail_if_no_subprotocol => false,
        supported_subprotocols => <<"NATS/1.0, NATS">>
    },
    emqx_gateway_schema:ws_opts(Override).

desc(nats) ->
    ?DESC(nats);
desc(protocol) ->
    ?DESC(protocol);
desc(tcp_ws_listeners) ->
    ?DESC(tcp_ws_listeners);
desc(ws_listener) ->
    ?DESC(ws_listener);
desc(wss_listener) ->
    ?DESC(wss_listener);
desc(websocket) ->
    ?DESC(websocket);
desc(_) ->
    undefined.

%%--------------------------------------------------------------------
%% internal functions

sc(Type) ->
    sc(Type, #{}).

sc(Type, Meta) ->
    hoconsc:mk(Type, Meta).

ref(StructName) ->
    ref(?MODULE, StructName).

ref(Mod, Field) ->
    hoconsc:ref(Mod, Field).

map(Name, Type) ->
    hoconsc:map(Name, Type).
