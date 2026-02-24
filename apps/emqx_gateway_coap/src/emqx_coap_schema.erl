%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_coap_schema).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").

%% config schema provides
-export([namespace/0, fields/1, desc/1]).
-export([block_size/0]).

-spec block_size() -> typerefl:type().
block_size() ->
    typerefl:alias("block_size", typerefl:union([16, 32, 64, 128, 256, 512, 1024])).

namespace() -> "gateway".

fields(coap) ->
    [
        {heartbeat,
            sc(
                emqx_schema:duration_s(),
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
        {blockwise,
            sc(
                ref(coap_blockwise),
                #{
                    desc => ?DESC(coap_blockwise),
                    importance => ?IMPORTANCE_LOW
                }
            )},
        {mountpoint, emqx_gateway_schema:mountpoint()},
        {listeners,
            sc(
                ref(emqx_gateway_schema, udp_listeners),
                #{desc => ?DESC(udp_listeners)}
            )}
    ] ++ emqx_gateway_schema:gateway_common_options();
fields(coap_blockwise) ->
    [
        {enable,
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(coap_blockwise_enable),
                    importance => ?IMPORTANCE_LOW
                }
            )},
        {max_block_size,
            sc(
                block_size(),
                #{
                    default => 1024,
                    desc => ?DESC(coap_blockwise_max_block_size),
                    importance => ?IMPORTANCE_LOW
                }
            )},
        {max_body_size,
            sc(
                emqx_schema:bytesize(),
                #{
                    default => <<"4MB">>,
                    desc => ?DESC(coap_blockwise_max_body_size),
                    importance => ?IMPORTANCE_LOW
                }
            )},
        {exchange_lifetime,
            sc(
                emqx_schema:timeout_duration_ms(),
                #{
                    default => <<"247s">>,
                    desc => ?DESC(coap_blockwise_exchange_lifetime),
                    importance => ?IMPORTANCE_LOW
                }
            )}
    ].

desc(coap) ->
    "CoAP gateway settings.";
desc(coap_blockwise) ->
    "Large payload transfer settings (split payload into CoAP blocks).";
desc(_) ->
    undefined.

%%--------------------------------------------------------------------
%% helpers

sc(Type, Meta) ->
    hoconsc:mk(Type, Meta).

ref(Mod, Field) ->
    hoconsc:ref(Mod, Field).

ref(Field) ->
    ref(?MODULE, Field).
