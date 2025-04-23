%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster_link_sup).

-behaviour(supervisor).

-include("emqx_cluster_link.hrl").

-export([start_link/0]).

%% Internal API / Routerepl
-export([
    actor_info/1,
    persistent_actor_info/2
]).

-export([
    ensure_actor/1,
    ensure_actor_stopped/1
]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?SERVER, root).

init(root) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 5
    },
    Links = emqx_cluster_link_config:get_enabled_links(),
    Metrics = emqx_metrics_worker:child_spec(metrics, ?METRIC_NAME),
    BookKeeper = bookkeeper_spec(),
    ExtrouterGC = extrouter_gc_spec(),
    RouteReplSups = [routerepl_sup_spec(LinkConf) || LinkConf <- Links],
    {ok, {SupFlags, [Metrics, BookKeeper, ExtrouterGC | RouteReplSups]}};
init({routerepl, LinkConf}) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 5,
        period => 5
    },
    ChildSpecs = [routerepl_spec(Type, Actor, LinkConf) || {Type, Actor} <- actors()],
    {ok, {SupFlags, ChildSpecs}}.

extrouter_gc_spec() ->
    %% NOTE: This one is currently global, not per-link.
    #{
        id => {extrouter, gc},
        start => {emqx_cluster_link_extrouter_gc, start_link, []},
        restart => permanent,
        type => worker
    }.

routerepl_sup_spec(LinkConf = #{name := Cluster}) ->
    sup_spec(Cluster, supervisor, [?MODULE, {routerepl, LinkConf}]).

routerepl_spec(Type = node, Actor, LinkConf) ->
    ActorMF = {?MODULE, actor_info, []},
    sup_spec(Type, emqx_cluster_link_routerepl, [Actor, ActorMF, LinkConf]);
routerepl_spec(Type = persistent, Actor, LinkConf) ->
    ActorMF = {?MODULE, persistent_actor_info, [LinkConf]},
    sup_spec(Type, emqx_cluster_link_routerepl, [Actor, ActorMF, LinkConf]).

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

ensure_actor(LinkConf) ->
    case supervisor:start_child(?SERVER, routerepl_sup_spec(LinkConf)) of
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

actors() ->
    lists:append(
        [{node, node()}],
        [{persistent, ?PS_ROUTE_ACTOR} || emqx_persistent_message:is_persistence_enabled()]
    ).

actor_info(incarnation) ->
    %% TODO: Subject to clock skew, need something more robust.
    erlang:system_time(millisecond);
actor_info(marker) ->
    "routesync".

persistent_actor_info(incarnation, #{ps_actor_incarnation := Incarnation}) ->
    Incarnation;
persistent_actor_info(marker, #{}) ->
    "routesync-ps".
