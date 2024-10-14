%%%-------------------------------------------------------------------
%% @doc alinkiot top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(alinkcore_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).


init([]) ->
    ChildSpecs = [
        #{
            id => alinkcore_cache,
            start => {alinkcore_cache, start_link, []},
            restart => permanent,
            shutdown => 2000,
            type => worker,
            modules => [alinkcore_cache]
        },
        #{
            id => alinkcore_lanzun_watcher,
            start => {alinkcore_lanzun_watcher, start_link, []},
            restart => permanent,
            shutdown => 2000,
            type => worker,
            modules => [alinkcore_lanzun_watcher]
        },
        #{
            id => alinkcore_cehou_watcher,
            start => {alinkcore_cehou_watcher, start_link, []},
            restart => permanent,
            shutdown => 2000,
            type => worker,
            modules => [alinkcore_cehou_watcher]
        }
    ],
    {ok, {#{strategy => one_for_one,
        intensity => 5,
        period => 30},
        ChildSpecs}
    }.
