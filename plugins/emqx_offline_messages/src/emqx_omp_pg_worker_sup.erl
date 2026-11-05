%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_omp_pg_worker_sup).
-moduledoc "Supervise a fixed number of indexed workers started with `Module:start_link(Index, Config)`.".
-behaviour(supervisor).
-export([child_spec/5, start_link/4, init/1]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-doc "Return a child spec for `Count` workers of `Module`, each stopped within `Shutdown`.".
-spec child_spec(atom(), module(), pos_integer(), timeout(), map()) -> supervisor:child_spec().
child_spec(Id, Module, Count, Shutdown, Config) ->
    #{
        id => Id,
        start => {?MODULE, start_link, [Module, Count, Shutdown, Config]},
        type => supervisor,
        shutdown => infinity
    }.

-spec start_link(module(), pos_integer(), timeout(), emqx_omp_pg_config:config()) ->
    supervisor:startlink_ret().
start_link(Module, Count, Shutdown, Config) ->
    supervisor:start_link(?MODULE, {Module, Count, Shutdown, Config}).

%%------------------------------------------------------------------------------
%% Callbacks
%%------------------------------------------------------------------------------

init({Module, Count, Shutdown, Config}) ->
    Children = [
        #{id => Index, start => {Module, start_link, [Index, Config]}, shutdown => Shutdown}
     || Index <- lists:seq(0, Count - 1)
    ],
    {ok, {#{strategy => one_for_one, intensity => 10, period => 10}, Children}}.
