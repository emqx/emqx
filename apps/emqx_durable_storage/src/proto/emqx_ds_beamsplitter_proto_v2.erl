%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_beamsplitter_proto_v2).

-behavior(emqx_bpapi).
-include_lib("emqx_utils/include/bpapi.hrl").

%% API:
-export([dispatch/6]).

%% behavior callbacks:
-export([introduced_in/0, deprecated_since/0]).

%%================================================================================
%% API functions
%%================================================================================

-spec dispatch(
    _SerializationToken,
    node(),
    emqx_ds:db(),
    emqx_ds_beamsplitter:pack_v2(),
    [emqx_ds_beamsplitter:destination()],
    map()
) -> true.
dispatch(SerializationToken, Node, DB, Pack, Destinations, Misc) ->
    emqx_rpc:cast(SerializationToken, Node, emqx_ds_beamsplitter, dispatch_v2, [
        DB, Pack, Destinations, Misc
    ]).

%%================================================================================
%% behavior callbacks
%%================================================================================

introduced_in() ->
    "5.9.0".

deprecated_since() ->
    "6.0.0".
