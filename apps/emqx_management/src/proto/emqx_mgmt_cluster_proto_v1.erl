%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mgmt_cluster_proto_v1).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,
    invite_node/2
]).

-include_lib("emqx/include/bpapi.hrl").

introduced_in() ->
    "5.0.0".

-spec invite_node(node(), node()) -> ok | ignore | {error, term()} | emqx_rpc:badrpc().
invite_node(Node, Self) ->
    rpc:call(Node, emqx_mgmt_api_cluster, join, [Self], 5000).
