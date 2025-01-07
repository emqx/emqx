%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_eviction_agent_proto_v3).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,

    all_channels_count/2,

    %% Changed in v3:
    evict_session_channel/5
]).

-include_lib("emqx/include/bpapi.hrl").

introduced_in() ->
    "5.7.0".

-spec all_channels_count([node()], timeout()) -> emqx_rpc:erpc_multicall(non_neg_integer()).
all_channels_count(Nodes, Timeout) ->
    erpc:multicall(Nodes, emqx_eviction_agent, all_local_channels_count, [], Timeout).

%% Changed in v3:
-spec evict_session_channel(
    node(),
    emqx_types:clientid(),
    emqx_types:conninfo(),
    emqx_types:clientinfo(),
    emqx_maybe:t(emqx_types:message())
) -> supervisor:startchild_err() | emqx_rpc:badrpc().
evict_session_channel(Node, ClientId, ConnInfo, ClientInfo, MaybeWillMsg) ->
    rpc:call(
        Node,
        emqx_eviction_agent,
        do_evict_session_channel_v3,
        [ClientId, ConnInfo, ClientInfo, MaybeWillMsg]
    ).
