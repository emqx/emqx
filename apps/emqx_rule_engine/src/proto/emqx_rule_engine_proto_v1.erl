%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_rule_engine_proto_v1).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,

    reset_metrics/1
]).

-include_lib("emqx/include/bpapi.hrl").
-include_lib("emqx_rule_engine/include/rule_engine.hrl").

introduced_in() ->
    "5.0.0".

-spec reset_metrics(rule_id()) -> ok | {error, any()}.
reset_metrics(RuleId) ->
    emqx_cluster_rpc:multicall(emqx_rule_engine, reset_metrics_for_rule, [RuleId]).
