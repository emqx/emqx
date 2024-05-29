%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster_link_sup).

-behaviour(supervisor).

-export([start_link/1]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link(LinksConf) ->
    supervisor:start_link({local, ?SERVER}, ?SERVER, LinksConf).

init(LinksConf) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 5
    },
    ExtrouterGC = extrouter_gc_spec(),
    RouteActors = [
        sup_spec(Name, emqx_cluster_link_router_syncer, [Name])
     || #{upstream := Name} <- LinksConf
    ],
    {ok, {SupFlags, [ExtrouterGC | RouteActors]}}.

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
