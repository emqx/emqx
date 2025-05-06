%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_resource_proto_v2).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,
    reset_metrics/1
]).

-include_lib("emqx/include/bpapi.hrl").
-include("emqx_resource.hrl").

introduced_in() ->
    "5.6.0".

-spec reset_metrics(resource_id()) -> ok | {error, any()}.
reset_metrics(ResId) ->
    emqx_cluster_rpc:multicall(emqx_resource, reset_metrics_local, [ResId]).
