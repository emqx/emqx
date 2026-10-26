%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_omp_pg_sup).
-moduledoc "Supervise the configured PostgreSQL backend.".
-behaviour(supervisor).
-export([start_link/0, start_link/1, start/1, stop/0, init/1]).

-define(RUNTIME, runtime).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec start_link() -> supervisor:startlink_ret().
start_link() -> supervisor:start_link({local, ?MODULE}, ?MODULE, []).
-spec start_link(emqx_omp_pg_config:config()) -> supervisor:startlink_ret().
start_link(Config) -> supervisor:start_link(?MODULE, Config).

-doc "Start the backend processes for `Config`.".
-spec start(emqx_omp_pg_config:config()) -> supervisor:startchild_ret().
start(Config) ->
    supervisor:start_child(?MODULE, #{
        id => ?RUNTIME,
        start => {?MODULE, start_link, [Config]},
        type => supervisor,
        shutdown => infinity
    }).

-doc "Stop the backend processes. The child spec is deleted so that `start/1` can run again.".
-spec stop() -> ok.
stop() ->
    _ = supervisor:terminate_child(?MODULE, ?RUNTIME),
    _ = supervisor:delete_child(?MODULE, ?RUNTIME),
    ok.

%%------------------------------------------------------------------------------
%% Callbacks
%%------------------------------------------------------------------------------

init([]) ->
    {ok, {#{strategy => one_for_one}, []}};
init(
    #{
        async_batched := Async,
        writer := #{pool_size := Writers, call_timeout := Shutdown}
    } = Config
) ->
    Children =
        emqx_omp_pg_store:pool_specs(Config) ++
            [
                emqx_omp_pg_worker_sup:child_spec(
                    writers, emqx_omp_pg_writer, Writers, Shutdown, Config
                )
             || Async
            ] ++
            [#{id => maintenance, start => {emqx_omp_pg_maintenance, start_link, [Config]}}],
    {ok, {#{strategy => one_for_one, intensity => 10, period => 10}, Children}}.
