%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_telemetry_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).
-define(CHILD(Mod), #{
    id => Mod,
    start => {Mod, start_link, []},
    restart => transient,
    shutdown => 5000,
    type => worker,
    modules => [Mod]
}).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 5
    },
    ChildSpecs = [?CHILD(emqx_telemetry)],
    {ok, {SupFlags, ChildSpecs}}.
