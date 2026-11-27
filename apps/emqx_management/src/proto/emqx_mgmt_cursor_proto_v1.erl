%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mgmt_cursor_proto_v1).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,

    lookup/3
]).

-include_lib("emqx/include/bpapi.hrl").

-define(TIMEOUT, 5_000).

introduced_in() ->
    "6.0.5".

-spec lookup(node(), emqx_mgmt_cursor:domain(), emqx_mgmt_cursor:id()) ->
    {ok, term()} | {error, bad_cursor}.
lookup(Node, Domain, Id) ->
    erpc:call(Node, emqx_mgmt_cursor, lookup_local, [Domain, Id], ?TIMEOUT).
