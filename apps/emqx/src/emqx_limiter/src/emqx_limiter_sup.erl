%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_limiter_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%%--------------------------------------------------------------------
%%  API functions
%%--------------------------------------------------------------------

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%--------------------------------------------------------------------
%%  Supervisor callbacks
%%--------------------------------------------------------------------

init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 3600
    },

    ok = emqx_limiter_bucket_registry:create_table(),
    Childs = [
        child_spec(emqx_limiter_registry, worker),
        child_spec(emqx_limiter_bucket_registry, worker)
    ],

    {ok, {SupFlags, Childs}}.

child_spec(Mod, Type) ->
    #{
        id => Mod,
        start => {Mod, start_link, []},
        restart => transient,
        type => Type,
        modules => [Mod]
    }.
