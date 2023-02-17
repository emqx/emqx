%%%-------------------------------------------------------------------
%% @doc emqx_ctl top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(emqx_ctl_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    SupFlags = #{
        strategy => one_for_all,
        intensity => 0,
        period => 1
    },
    ChildSpecs = [
        #{
            id => emqx_ctl,
            start => {emqx_ctl, start_link, []},
            type => worker,
            restart => permanent
        }
    ],
    {ok, {SupFlags, ChildSpecs}}.
