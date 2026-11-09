%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_quota_index_proto_v1).

-behaviour(emqx_bpapi).

-include_lib("emqx/include/bpapi.hrl").

-export([introduced_in/0, supports_serial_index/1]).

introduced_in() ->
    "7.0.0".

-doc "Nodes that announce this API support the serial quota index. Callers check the announced version".
-spec supports_serial_index(node()) -> true.
supports_serial_index(Node) ->
    erpc:call(Node, emqx_mq_quota_index, supports_serial_index, [], 5000).
