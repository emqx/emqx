%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Transaction funs of emqx_mq_registry. A replicant passes them to mria
%% by name and a core node runs them, see apps/emqx_bpapi/README.md.
-module(emqx_mq_registry_tx_proto_v1).

-behaviour(emqx_bpapi).

-include_lib("emqx/include/bpapi.hrl").

-export([introduced_in/0]).

-export([update_index/3]).

introduced_in() ->
    "6.3.3".

-spec update_index(term(), term(), map()) -> ok | not_found.
update_index(Key, Id, UpdateFields) ->
    emqx_mq_registry:update_index_tx(Key, Id, UpdateFields).
