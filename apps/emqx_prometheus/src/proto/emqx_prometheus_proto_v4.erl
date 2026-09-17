%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_prometheus_proto_v4).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,

    supports_listener_accept_result/1
]).

-include_lib("emqx/include/bpapi.hrl").

-define(TIMEOUT, 5000).

introduced_in() ->
    "7.0.0".

%% Version 4 marks nodes that can aggregate `emqx_client_accept_result`.
%% We do not actually need to call this function.
%% It's enough to check the BPAPI version support.
-spec supports_listener_accept_result(node()) -> true.
supports_listener_accept_result(Node) ->
    erpc:call(Node, emqx_prometheus, supports_listener_accept_result, [], ?TIMEOUT).
