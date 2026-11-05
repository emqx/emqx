%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_quota_index_proto_v1).

-behaviour(emqx_bpapi).

-include_lib("emqx/include/bpapi.hrl").

-export([introduced_in/0, supports_common_index/1]).

%% This version marks support for common quota buckets and reject queues.
introduced_in() ->
    "7.0.0".

%% Callers check the advertised version without invoking this RPC.
-spec supports_common_index(node()) -> true.
supports_common_index(Node) ->
    erpc:call(Node, emqx_mq_quota_index, supports_common_index, [], 5000).
