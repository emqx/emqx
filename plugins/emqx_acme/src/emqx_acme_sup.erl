-module(emqx_acme_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{
        strategy => one_for_all,
        intensity => 5,
        period => 60
    },
    Children = [
        %% Challenge responder runs on every cluster node so the HTTP-01
        %% request can be served wherever an NLB lands it. The leader
        %% drives the listener start/stop fanout via cluster_start_listener.
        #{
            id => emqx_acme_challenge,
            start => {emqx_acme_challenge, start_link, []},
            type => worker,
            restart => permanent,
            shutdown => 5_000
        },
        #{
            id => emqx_acme_issuer,
            start => {emqx_acme_issuer, start_link, []},
            type => worker,
            restart => permanent,
            shutdown => 5_000
        }
    ],
    {ok, {SupFlags, Children}}.
