%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_cluster_link_metrics).

-include("emqx_cluster_link.hrl").

%% API
-export([
    maybe_create_metrics/1,
    drop_metrics/1,

    get_metrics/1,
    routes_inc/2
]).

%%--------------------------------------------------------------------
%% Type definitions
%%--------------------------------------------------------------------

-define(METRICS, [
    ?route_metric
]).
-define(RATE_METRICS, []).

%%--------------------------------------------------------------------
%% metrics API
%%--------------------------------------------------------------------

get_metrics(ClusterName) ->
    Nodes = emqx:running_nodes(),
    Timeout = 15_000,
    Results = emqx_metrics_proto_v2:get_metrics(Nodes, ?METRIC_NAME, ClusterName, Timeout),
    sequence_multicall_results(Nodes, Results).

maybe_create_metrics(ClusterName) ->
    case emqx_metrics_worker:has_metrics(?METRIC_NAME, ClusterName) of
        true ->
            ok = emqx_metrics_worker:reset_metrics(?METRIC_NAME, ClusterName);
        false ->
            ok = emqx_metrics_worker:create_metrics(
                ?METRIC_NAME, ClusterName, ?METRICS, ?RATE_METRICS
            )
    end.

drop_metrics(ClusterName) ->
    ok = emqx_metrics_worker:clear_metrics(?METRIC_NAME, ClusterName).

routes_inc(ClusterName, Val) ->
    catch emqx_metrics_worker:inc(?METRIC_NAME, ClusterName, ?route_metric, Val).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

-spec sequence_multicall_results([node()], emqx_rpc:erpc_multicall(term())) ->
    {ok, [{node(), term()}]} | {error, [term()]}.
sequence_multicall_results(Nodes, Results) ->
    case lists:partition(fun is_ok/1, lists:zip(Nodes, Results)) of
        {OkResults, []} ->
            {ok, [{Node, Res} || {Node, {ok, Res}} <- OkResults]};
        {_OkResults, BadResults} ->
            {error, BadResults}
    end.

is_ok({_Node, {ok, _}}) -> true;
is_ok(_) -> false.
