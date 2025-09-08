%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_license_proto_v3).

-behaviour(emqx_bpapi).

-include_lib("emqx/include/bpapi.hrl").

-export([introduced_in/0]).

-export([
    remote_connection_counts/1,
    stats/2
]).

-define(TIMEOUT, 500).

introduced_in() ->
    "6.0.0".

-spec remote_connection_counts(list(node())) -> list({atom(), term()}).
remote_connection_counts(Nodes) ->
    erpc:multicall(Nodes, emqx_license_resources, local_connection_count, [], ?TIMEOUT).

%% @doc stats/1 replaces remote_connection_counts/1 since 6.0.0
-spec stats(list(node()), integer()) ->
    emqx_rpc:erpc_multicall(#{sessions := non_neg_integer(), tps := number()}).
stats(Nodes, Time) ->
    erpc:multicall(Nodes, emqx_license_resources, stats, [Time], ?TIMEOUT).
