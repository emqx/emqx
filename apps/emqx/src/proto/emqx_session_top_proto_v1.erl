%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_session_top_proto_v1).

-behaviour(emqx_bpapi).

-export([introduced_in/0]).

-export([
    start_top_scan/3,
    cancel_top_scan/2,
    top_scan_result/4
]).

-include_lib("emqx/include/bpapi.hrl").

introduced_in() ->
    "6.3.0".

-spec start_top_scan([node()], map(), timeout()) ->
    emqx_rpc:erpc_multicall({ok, accepted} | {error, term()}).
start_top_scan(Nodes, Req, Timeout) ->
    erpc:multicall(Nodes, emqx_session_top_scanner, start_scan, [Req], Timeout).

-spec cancel_top_scan([node()], term()) -> ok.
cancel_top_scan(Nodes, ScanId) ->
    erpc:multicast(Nodes, emqx_session_top_scanner, cancel, [ScanId]).

-spec top_scan_result(
    node(), term(), node(), {ok, [emqx_session_tool:row()]} | {error, term()}
) ->
    ok.
top_scan_result(Node, ScanId, FromNode, Result) ->
    erpc:cast(Node, emqx_session_top_collector, top_scan_result, [ScanId, FromNode, Result]).
