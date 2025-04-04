%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_plugins_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags =
        #{
            strategy => one_for_one,
            intensity => 100,
            period => 10
        },
    ChildSpecs = [child_spec(emqx_plugins_serde)],
    {ok, {SupFlags, ChildSpecs}}.

child_spec(Mod) ->
    #{
        id => Mod,
        start => {Mod, start_link, []},
        restart => permanent,
        shutdown => 5_000,
        type => worker
    }.
