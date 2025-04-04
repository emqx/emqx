%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_beamsplitter_proto_v1).

-behavior(emqx_bpapi).
-include_lib("emqx_utils/include/bpapi.hrl").

%% API:
-export([dispatch/2]).

%% behavior callbacks:
-export([introduced_in/0, deprecated_since/0]).

%%================================================================================
%% API functions
%%================================================================================

-spec dispatch(node(), emqx_ds_beamformer:beam()) -> true.
dispatch(Node, Beam) ->
    emqx_rpc:cast(Node, emqx_ds_beamformer, do_dispatch, [Beam]).

%%================================================================================
%% behavior callbacks
%%================================================================================

introduced_in() ->
    "5.8.0".

deprecated_since() ->
    "5.9.0".
