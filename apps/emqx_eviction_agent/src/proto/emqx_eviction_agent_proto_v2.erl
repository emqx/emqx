%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_eviction_agent_proto_v2).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,
    deprecated_since/0,

    evict_session_channel/4,

    %% Introduced in v2:
    all_channels_count/2
]).

-include_lib("emqx/include/bpapi.hrl").

introduced_in() ->
    "5.2.1".

deprecated_since() ->
    "5.7.0".

-spec evict_session_channel(
    node(),
    emqx_types:clientid(),
    emqx_types:conninfo(),
    emqx_types:clientinfo()
) -> supervisor:startchild_err() | emqx_rpc:badrpc().
evict_session_channel(Node, ClientId, ConnInfo, ClientInfo) ->
    rpc:call(Node, emqx_eviction_agent, evict_session_channel, [ClientId, ConnInfo, ClientInfo]).

%% Introduced in v2:
-spec all_channels_count([node()], timeout()) -> emqx_rpc:erpc_multicall(non_neg_integer()).
all_channels_count(Nodes, Timeout) ->
    erpc:multicall(Nodes, emqx_eviction_agent, all_local_channels_count, [], Timeout).
