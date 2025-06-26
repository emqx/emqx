%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_kinesis_proto_v1).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,

    try_consume_connector_limiter/5,
    try_consume_action_limiter/6
]).

-include_lib("emqx/include/bpapi.hrl").

introduced_in() ->
    "5.10.1".

-spec try_consume_connector_limiter(
    node(), binary(), [{emqx_limiter:name(), emqx_limiter:options()}], timeout(), timeout()
) ->
    ok | timeout.
try_consume_connector_limiter(Node, ConnResId, LimiterConfig, Timeout, RPCTimeout) ->
    erpc:call(
        Node,
        emqx_bridge_kinesis_impl_producer,
        try_consume_connector_limiter_v1,
        [ConnResId, LimiterConfig, Timeout],
        RPCTimeout
    ).

-spec try_consume_action_limiter(
    node(),
    binary(),
    binary(),
    [{emqx_limiter:name(), emqx_limiter:options()}],
    timeout(),
    timeout()
) ->
    ok | timeout.
try_consume_action_limiter(Node, ConnResId, ChanResId, LimiterConfig, Timeout, RPCTimeout) ->
    erpc:call(
        Node,
        emqx_bridge_kinesis_impl_producer,
        try_consume_action_limiter_v1,
        [ConnResId, ChanResId, LimiterConfig, Timeout],
        RPCTimeout
    ).
