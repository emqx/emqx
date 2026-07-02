%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_session_tool_proto_v1).

-behaviour(emqx_bpapi).

-export([introduced_in/0]).

-export([
    scan/3
]).

-include_lib("emqx/include/bpapi.hrl").

introduced_in() ->
    "6.0.3".

%% Timeout is a caller-supplied parameter (not a module constant) so it can
%% be tuned without minting a new bpapi version. A scan throttles itself
%% with sleeps between batches, so a large session set on a busy node can
%% take a while; the caller picks a generous value.
-spec scan([node()], emqx_session_tool:scan_opts(), timeout()) ->
    [emqx_rpc:erpc([emqx_session_tool:row()])].
scan(Nodes, Opts, Timeout) ->
    erpc:multicall(Nodes, emqx_session_tool, scan, [Opts], Timeout).
