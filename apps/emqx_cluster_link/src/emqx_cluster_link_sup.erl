%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster_link_sup).

-behaviour(supervisor).

-include("emqx_cluster_link.hrl").

-export([start_link/1]).

-export([
    ensure_actor/1,
    ensure_actor_stopped/1
]).

-export([init/1]).

-define(SERVER, ?MODULE).
-define(ACTOR_MODULE, emqx_cluster_link_router_syncer).

start_link(LinksConf) ->
    supervisor:start_link({local, ?SERVER}, ?SERVER, LinksConf).

init(LinksConf) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 5
    },
    Metrics = emqx_metrics_worker:child_spec(metrics, ?METRIC_NAME),
    BookKeeper = bookkeeper_spec(),
    ExtrouterGC = extrouter_gc_spec(),
    RouteActors = [
        sup_spec(Name, ?ACTOR_MODULE, [LinkConf])
     || #{name := Name} = LinkConf <- LinksConf
    ],
    {ok, {SupFlags, [Metrics, BookKeeper, ExtrouterGC | RouteActors]}}.

extrouter_gc_spec() ->
    %% NOTE: This one is currently global, not per-link.
    #{
        id => {extrouter, gc},
        start => {emqx_cluster_link_extrouter_gc, start_link, []},
        restart => permanent,
        type => worker
    }.

sup_spec(Id, Mod, Args) ->
    #{
        id => Id,
        start => {Mod, start_link, Args},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [Mod]
    }.

bookkeeper_spec() ->
    #{
        id => bookkeeper,
        start => {emqx_cluster_link_bookkeeper, start_link, []},
        restart => permanent,
        type => worker,
        shutdown => 5_000
    }.

ensure_actor(#{name := Name} = LinkConf) ->
    case supervisor:start_child(?SERVER, sup_spec(Name, ?ACTOR_MODULE, [LinkConf])) of
        {ok, Pid} ->
            {ok, Pid};
        {error, {already_started, Pid}} ->
            {ok, Pid};
        Err ->
            Err
    end.

ensure_actor_stopped(ClusterName) ->
    case supervisor:terminate_child(?MODULE, ClusterName) of
        ok ->
            _ = supervisor:delete_child(?MODULE, ClusterName),
            ok;
        {error, not_found} ->
            ok
    end.
