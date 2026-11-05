%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_consumer_proto_v2).

-behaviour(emqx_bpapi).

-include_lib("emqx/include/bpapi.hrl").

-export([
    introduced_in/0
]).

-export([
    mq_server_stop/3
]).

%% This version stops a consumer together with its per-queue supervisor.
%% Other calls are unchanged and use `emqx_mq_consumer_proto_v1'.
introduced_in() ->
    "7.0.0".

-spec mq_server_stop(node(), emqx_mq_types:mq(), emqx_mq_types:consumer_ref()) -> ok.
mq_server_stop(Node, MQ, ConsumerRef) ->
    erpc:call(Node, emqx_mq_consumer, stop_v2, [MQ, ConsumerRef]).
