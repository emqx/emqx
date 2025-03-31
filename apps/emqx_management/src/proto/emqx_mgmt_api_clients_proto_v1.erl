%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mgmt_api_clients_proto_v1).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,

    clients_v2_ets_select/2
]).

-include_lib("emqx/include/bpapi.hrl").

-define(LIST_ETS_TIMEOUT, 15_000).

introduced_in() ->
    "5.9.0".

-spec clients_v2_ets_select(node(), emqx_mgmt_api_clients:list_clients_v2_params()) ->
    {[_Row], #{
        cont := undefined | emqx_mgmt_api_clients:ets_continuation(), node_idx := pos_integer()
    }}.
clients_v2_ets_select(Node, Params) ->
    erpc:call(Node, emqx_mgmt_api_clients, local_ets_select_v1, [Params], ?LIST_ETS_TIMEOUT).
