%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mgmt_trace_proto_v3).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,

    list_trace_sizes/1,
    get_trace_details/2,
    stream_trace_log/4
]).

-include_lib("emqx/include/bpapi.hrl").

-define(RPC_TIMEOUT, 10_000).

%% Changes
%% === v3
%% * More accurate naming.
%% * Point all calls to the single module `emqx_mgmt_api_trace`.
%% * RPC `trace_file/2` is removed in favor of `stream_trace_log/4`.
%% * RPC `read_trace_file/4` generalized into `stream_trace_log/4`.
%% * Always dispatch trace-name-first, to make it easier to enforce some
%%   consistency guarantees.

introduced_in() ->
    "6.0.0".

-spec list_trace_sizes([node()]) ->
    emqx_rpc:multicall_result(#{{node(), emqx_trace:name()} => non_neg_integer()}).
list_trace_sizes(Nodes) ->
    rpc:multicall(Nodes, emqx_mgmt_api_trace, get_trace_size, [], ?RPC_TIMEOUT).

-spec get_trace_details([node()], emqx_trace:name()) ->
    emqx_rpc:multicall_result(
        {ok, #{
            size => non_neg_integer(),
            mtime => file:date_time() | non_neg_integer(),
            node => atom()
        }}
        | {error, #{reason => term(), node => atom()}}
    ).
get_trace_details(Nodes, Name) ->
    rpc:multicall(Nodes, emqx_mgmt_api_trace, get_trace_details, [Name], ?RPC_TIMEOUT).

-spec stream_trace_log(
    node(),
    emqx_trace:name(),
    start | {cont, emqx_trace:cursor()},
    pos_integer() | undefined
) ->
    {ok, binary(), {cont | eof | retry, emqx_trace:cursor()}}
    | {error, _}
    | {badrpc, _}.
stream_trace_log(Node, Name, Position, Limit) ->
    rpc:call(Node, emqx_mgmt_api_trace, read_trace_file, [Name, Position, Limit], ?RPC_TIMEOUT).
