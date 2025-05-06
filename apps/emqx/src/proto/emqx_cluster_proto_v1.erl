%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster_proto_v1).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,
    can_i_join/2
]).

-include("bpapi.hrl").

introduced_in() ->
    "5.9.0".

-spec can_i_join(node(), node()) -> ok | {error, string()}.
can_i_join(SelfNode, PeerNode) ->
    erpc:call(PeerNode, emqx_cluster, can_i_join, [SelfNode]).
