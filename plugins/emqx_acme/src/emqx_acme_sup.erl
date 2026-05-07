-module(emqx_acme_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    %% Own the challenge ETS table from this long-lived supervisor so
    %% it outlives every challenge-listener (re)start.
    ok = emqx_acme_challenge:create_tab(),
    SupFlags = #{
        strategy => one_for_all,
        intensity => 5,
        period => 60
    },
    Children = [
        #{
            id => emqx_acme_issuer,
            start => {emqx_acme_issuer, start_link, []},
            type => worker,
            restart => permanent,
            shutdown => 5_000
        }
    ],
    {ok, {SupFlags, Children}}.
