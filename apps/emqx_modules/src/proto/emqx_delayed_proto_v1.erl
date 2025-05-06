%%--------------------------------------------------------------------
%%Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_delayed_proto_v1).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,
    get_delayed_message/2,
    delete_delayed_message/2
]).

-include_lib("emqx/include/bpapi.hrl").

introduced_in() ->
    "5.0.0".

-spec get_delayed_message(node(), binary()) ->
    emqx_delayed:with_id_return(map()) | emqx_rpc:badrpc().
get_delayed_message(Node, Id) ->
    rpc:call(Node, emqx_delayed, get_delayed_message, [Id]).

-spec delete_delayed_message(node(), binary()) -> emqx_delayed:with_id_return() | emqx_rpc:badrpc().
delete_delayed_message(Node, Id) ->
    rpc:call(Node, emqx_delayed, delete_delayed_message, [Id]).
