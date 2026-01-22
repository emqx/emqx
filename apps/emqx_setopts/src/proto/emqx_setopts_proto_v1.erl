%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_setopts_proto_v1).

-behaviour(emqx_bpapi).

-export([introduced_in/0, call_client/3]).

-include_lib("emqx/include/bpapi.hrl").

-doc """
Return the bpapi version for setopts RPC.
""".
introduced_in() ->
    "6.2.0".

-doc """
Call setopts update on target nodes.
""".
-spec call_client([node()], emqx_types:clientid(), term()) -> emqx_rpc:erpc_multicall(term()).
call_client(Nodes, ClientId, Req) ->
    erpc:multicall(Nodes, emqx_setopts, do_call_client, [ClientId, Req], 30000).
