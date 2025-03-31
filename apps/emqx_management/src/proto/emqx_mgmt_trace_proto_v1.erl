%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mgmt_trace_proto_v1).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,

    trace_file/2,
    get_trace_size/1,
    read_trace_file/4
]).

-include_lib("emqx/include/bpapi.hrl").

introduced_in() ->
    "5.0.0".

-spec get_trace_size([node()]) ->
    emqx_rpc:multicall_result(#{{node(), file:name_all()} => non_neg_integer()}).
get_trace_size(Nodes) ->
    rpc:multicall(Nodes, emqx_mgmt_api_trace, get_trace_size, [], 30000).

-spec trace_file([node()], file:name_all()) ->
    emqx_rpc:multicall_result(
        {ok, Node :: list(), Binary :: binary()}
        | {error, Node :: list(), Reason :: term()}
    ).
trace_file(Nodes, File) ->
    rpc:multicall(Nodes, emqx_trace, trace_file, [File], 60000).

-spec read_trace_file(node(), binary(), non_neg_integer(), non_neg_integer()) ->
    {ok, binary()}
    | {error, _}
    | {eof, non_neg_integer()}
    | {badrpc, _}.
read_trace_file(Node, Name, Position, Limit) ->
    rpc:call(Node, emqx_mgmt_api_trace, read_trace_file, [Name, Position, Limit]).
