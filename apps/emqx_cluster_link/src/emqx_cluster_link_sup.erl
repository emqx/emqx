%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster_link_sup).

-behaviour(supervisor).

-export([start_link/1]).

-export([init/1]).

-define(COORD_SUP, emqx_cluster_link_coord_sup).
-define(SERVER, ?MODULE).

start_link(LinksConf) ->
    supervisor:start_link({local, ?SERVER}, ?SERVER, LinksConf).

init(LinksConf) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 5
    },
    %% Children = [sup_spec(?COORD_SUP, ?COORD_SUP, LinksConf)],
    Children = [
        sup_spec(Name, emqx_cluster_link_router_syncer, [Name])
     || #{upstream := Name} <- LinksConf
    ],
    {ok, {SupFlags, Children}}.

sup_spec(Id, Mod, Args) ->
    #{
        id => Id,
        start => {Mod, start_link, Args},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [Mod]
    }.
