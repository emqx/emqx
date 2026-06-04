%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_metrics_installer).
-moduledoc """
This module implements a helper process that owns the lifetime of a set
of metrics, for an existing metrics worker:
* Metrics are created on process startup.
* Metrics are cleaerd on terminate.

Supposed to be included as a child in a supervisor for some resource that
needs metrics to be present in a shared "parent" metrics worker.
""".

-behaviour(gen_server).

%% API functions
-export([
    start_link/4,
    child_spec/5
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_info/2,
    handle_cast/2,
    terminate/2
]).

-spec child_spec(
    _ChildId :: term(),
    emqx_metrics_worker:handler_name(),
    emqx_metrics_worker:metric_id(),
    [emqx_metrics_worker:metric_spec()],
    [emqx_metrics_worker:metric_name()]
) -> supervisor:child_spec().
child_spec(ChldName, Name, MetricId, Metrics, RateMetrics) ->
    #{
        id => ChldName,
        start => {?MODULE, start_link, [Name, MetricId, Metrics, RateMetrics]},
        restart => permanent,
        shutdown => 5000,
        type => worker
    }.

-spec start_link(
    emqx_metrics_worker:handler_name(),
    emqx_metrics_worker:metric_id(),
    [emqx_metrics_worker:metric_spec()],
    [emqx_metrics_worker:metric_name()]
) -> gen_server:start_ret().
start_link(Name, MetricId, Metrics, RateMetrics) ->
    gen_server:start_link(?MODULE, {Name, MetricId, Metrics, RateMetrics}, []).

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

init({Name, MetricId, Metrics, RateMetrics}) ->
    erlang:process_flag(trap_exit, true),
    case emqx_metrics_worker:ensure_metrics(Name, MetricId, Metrics, RateMetrics) of
        {ok, _} ->
            ok = emqx_metrics_worker:reset_metrics(Name, MetricId),
            {ok, {Name, MetricId}, hibernate};
        {error, Reason} ->
            {stop, Reason}
    end.

handle_call(_Request, _From, State) ->
    {reply, {?MODULE, invalid_call}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, {Name, MetricId}) ->
    emqx_metrics_worker:clear_metrics(Name, MetricId).
