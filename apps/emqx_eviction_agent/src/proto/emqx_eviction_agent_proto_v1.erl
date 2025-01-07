%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_eviction_agent_proto_v1).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,

    evict_session_channel/4
]).

-include_lib("emqx/include/bpapi.hrl").

introduced_in() ->
    "5.0.22".

-spec evict_session_channel(
    node(),
    emqx_types:clientid(),
    emqx_types:conninfo(),
    emqx_types:clientinfo()
) -> supervisor:startchild_err() | emqx_rpc:badrpc().
evict_session_channel(Node, ClientId, ConnInfo, ClientInfo) ->
    rpc:call(Node, emqx_eviction_agent, evict_session_channel, [ClientId, ConnInfo, ClientInfo]).
