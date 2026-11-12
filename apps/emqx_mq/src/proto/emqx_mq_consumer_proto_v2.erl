%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_consumer_proto_v2).

-moduledoc """
A node announces this version when it supports the serial quota index, purge, and reject queues.
See `emqx_mq_quota_index:is_serial_index_supported/0`. Callers use `emqx_mq_consumer_proto_v1`.
The static checks announce only API modules that make an RPC call, so this version has one.
""".

-behaviour(emqx_bpapi).

-include_lib("emqx/include/bpapi.hrl").

-export([
    introduced_in/0
]).

-export([
    mq_supported/1
]).

introduced_in() ->
    "7.0.0".

-spec mq_supported(node()) -> emqx_mq_consumer:supported().
mq_supported(Node) ->
    erpc:call(Node, emqx_mq_consumer, supported_v2, []).
