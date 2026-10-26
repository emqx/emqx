%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Transaction funs of emqx_bpapi. A replicant passes them to mria by
%% name and a core node runs them, see apps/emqx_bpapi/README.md.
-module(emqx_bpapi_tx_proto_v1).

-behaviour(emqx_bpapi).

-include_lib("emqx_utils/include/bpapi.hrl").

-export([introduced_in/0]).

-export([announce/2]).

introduced_in() ->
    "6.3.3".

-spec announce(node(), [{emqx_bpapi:api(), emqx_bpapi:api_version()}]) -> ok.
announce(Node, Data) ->
    emqx_bpapi:announce_fun(Node, Data).
