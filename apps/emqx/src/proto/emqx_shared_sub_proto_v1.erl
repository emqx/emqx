%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_shared_sub_proto_v1).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,
    send/4,
    dispatch_with_ack/5
]).

-include("bpapi.hrl").

%%================================================================================
%% behaviour callbacks
%%================================================================================

introduced_in() ->
    "5.0.8".

%%================================================================================
%% API functions
%%================================================================================

-spec send(node(), pid(), emqx_types:topic(), term()) -> true.
send(Node, Pid, Topic, Msg) ->
    emqx_rpc:cast(Topic, Node, erlang, send, [Pid, Msg]).

-spec dispatch_with_ack(
    pid(), emqx_types:group(), emqx_types:topic(), emqx_types:message(), timeout()
) ->
    ok | {error, _}.
dispatch_with_ack(Pid, Group, Topic, Msg, Timeout) ->
    emqx_rpc:call(
        Topic, node(Pid), emqx_shared_sub, do_dispatch_with_ack, [Pid, Group, Topic, Msg], Timeout
    ).
