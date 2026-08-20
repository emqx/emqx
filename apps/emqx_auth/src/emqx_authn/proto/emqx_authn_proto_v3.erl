%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_proto_v3).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,
    need_use_ns_table_for_global/1
]).

-include_lib("emqx/include/bpapi.hrl").

-define(TIMEOUT, 5000).

introduced_in() ->
    "6.4.0".

%% We do not actually need to call this function.
%% It's enough to check the BPAPI version support.
-spec need_use_ns_table_for_global(node()) -> true.
need_use_ns_table_for_global(Node) ->
    erpc:call(Node, emqx_authn_mnesia, need_use_ns_table_for_global, [], ?TIMEOUT).
