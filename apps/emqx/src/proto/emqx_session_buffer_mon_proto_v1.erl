%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_session_buffer_mon_proto_v1).

-behaviour(emqx_bpapi).

-export([introduced_in/0]).

-export([
    start_top_scan/3,
    cancel_top_scan/3,
    top_scan_result/4
]).

-include_lib("emqx/include/bpapi.hrl").

introduced_in() ->
    "6.3.0".

-spec start_top_scan([node()], map(), timeout()) ->
    emqx_rpc:erpc_multicall({ok, accepted} | {error, term()}).
start_top_scan(Nodes, Req, Timeout) ->
    erpc:multicall(Nodes, emqx_session_buffer_mon, start_top_scan, [Req], Timeout).

-spec cancel_top_scan([node()], term(), timeout()) ->
    emqx_rpc:erpc_multicall({ok, cancelled} | {error, not_running}).
cancel_top_scan(Nodes, ScanId, Timeout) ->
    erpc:multicall(Nodes, emqx_session_buffer_mon, cancel_top_scan, [ScanId], Timeout).

-spec top_scan_result(
    node(), term(), node(), {ok, [emqx_session_buffer_mon:row()]} | {error, term()}
) ->
    ok.
top_scan_result(Node, ScanId, FromNode, Result) ->
    erpc:cast(Node, emqx_session_buffer_mon, top_scan_result, [ScanId, FromNode, Result]).
