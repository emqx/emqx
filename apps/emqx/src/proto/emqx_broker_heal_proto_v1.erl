%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_broker_heal_proto_v1).

-behaviour(emqx_bpapi).

-include("bpapi.hrl").

-export([
    introduced_in/0,

    start/2
]).

introduced_in() ->
    "5.10.4".

-spec start([node()], PartitionView) -> list() when
    PartitionView :: {[node()], [[node()]]}.
start(Nodes, PartitionView) ->
    erpc:multicall(Nodes, emqx_broker_heal, start_local, [PartitionView]).
