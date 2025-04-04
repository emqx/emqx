%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_delayed_proto_v3).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,
    get_delayed_message/2,
    delete_delayed_message/2,

    %% Introduced in v2:
    clear_all/1,
    %% Introduced in v3:
    delete_delayed_messages_by_topic_name/2
]).

-include_lib("emqx/include/bpapi.hrl").

introduced_in() ->
    "5.5.0".

-spec get_delayed_message(node(), binary()) ->
    emqx_delayed:with_id_return(map()) | emqx_rpc:badrpc().
get_delayed_message(Node, Id) ->
    rpc:call(Node, emqx_delayed, get_delayed_message, [Id]).

-spec delete_delayed_message(node(), binary()) -> emqx_delayed:with_id_return() | emqx_rpc:badrpc().
delete_delayed_message(Node, Id) ->
    rpc:call(Node, emqx_delayed, delete_delayed_message, [Id]).

%% Introduced in v2:

-spec clear_all([node()]) -> emqx_rpc:erpc_multicall(ok).
clear_all(Nodes) ->
    erpc:multicall(Nodes, emqx_delayed, clear_all_local, []).

%% Introduced in v3:

-spec delete_delayed_messages_by_topic_name(list(), binary()) -> emqx_rpc:erpc_multicall(ok).
delete_delayed_messages_by_topic_name(Nodes, TopicName) ->
    erpc:multicall(Nodes, emqx_delayed, do_delete_delayed_messages_by_topic_name, [TopicName]).
