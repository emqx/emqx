%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster_link_coord_sup).

-behaviour(supervisor).

-export([start_link/1]).
-export([init/1]).

-export([
    start_coordinator/1,
    restart_coordinator/1,
    stop_coordinator/1
]).

-define(SERVER, ?MODULE).
-define(COORDINATOR_MOD, emqx_cluster_link_coordinator).

start_link(LinksConf) ->
    supervisor:start_link({local, ?SERVER}, ?SERVER, LinksConf).

init(LinksConf) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 5
    },
    {ok, {SupFlags, children(LinksConf)}}.

start_coordinator(#{upstream := Name} = LinkConf) ->
    supervisor:start_child(?SERVER, worker_spec(Name, LinkConf)).

restart_coordinator(#{upstream := Name} = _LinkConf) ->
    supervisor:restart_child(?SERVER, Name).

stop_coordinator(#{upstream := Name} = _LinkConf) ->
    case supervisor:terminate_child(?SERVER, Name) of
        ok ->
            supervisor:delete_child(?SERVER, Name);
        Err ->
            Err
    end.

worker_spec(Id, LinkConf) ->
    #{
        id => Id,
        start => {?COORDINATOR_MOD, start_link, [LinkConf]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [?COORDINATOR_MOD]
    }.

children(LinksConf) ->
    [worker_spec(Name, Conf) || #{upstream := Name, enable := true} = Conf <- LinksConf].
