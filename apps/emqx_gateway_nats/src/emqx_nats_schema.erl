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
        {protocol, sc(ref(protocol))},
        {mountpoint, emqx_gateway_schema:mountpoint()},
        {listeners, sc(ref(emqx_gateway_schema, tcp_listeners), #{desc => ?DESC(tcp_listeners)})}
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
    ].

desc(nats) ->
    "The NATS protocol gateway provides EMQX with the ability to access NATS\n"
    "(Neural Autonomic Transport System) protocol.";
desc(protocol) ->
    "A group of settings for NATS Server.";
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
