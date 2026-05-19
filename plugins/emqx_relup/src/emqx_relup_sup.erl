-module(emqx_relup_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    RuleEngineRegistry = worker_spec(emqx_relup_main),
    SupFlags = #{
        strategy => one_for_one,
        intensity => 20,
        period => 10
    },
    Children = [
        RuleEngineRegistry
    ],
    {ok, {SupFlags, Children}}.

worker_spec(Mod) ->
    #{
        id => Mod,
        start => {Mod, start_link, []},
        restart => permanent,
        shutdown => 5_000,
        type => worker,
        modules => [Mod]
    }.
