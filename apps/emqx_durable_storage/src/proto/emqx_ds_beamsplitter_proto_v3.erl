%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_beamsplitter_proto_v3).

-behavior(emqx_bpapi).
-include_lib("emqx_utils/include/bpapi.hrl").

%% API:
-export([dispatch/7]).

%% behavior callbacks:
-export([introduced_in/0]).

%% Changelog:
%%
%% == v3 ==
%% Packs no longer include DSKeys

%%================================================================================
%% API functions
%%================================================================================

-spec dispatch(
    _SerializationToken,
    node(),
    emqx_ds:db(),
    emqx_ds_payload_transform:schema(),
    emqx_ds_beamsplitter:pack_v3(),
    [emqx_ds_beamsplitter:destination()],
    map()
) -> true.
dispatch(SerializationToken, Node, DB, PTSchema, Pack, Destinations, Misc) ->
    emqx_rpc:cast(SerializationToken, Node, emqx_ds_beamsplitter, dispatch_v3, [
        DB, PTSchema, Pack, Destinations, Misc
    ]).

%%================================================================================
%% behavior callbacks
%%================================================================================

introduced_in() ->
    "6.0.0".
