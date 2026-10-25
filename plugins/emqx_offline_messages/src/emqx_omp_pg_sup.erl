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

start_link() -> supervisor:start_link({local, ?MODULE}, ?MODULE, []).
start_link(Config) -> supervisor:start_link(?MODULE, Config).

-doc "Start the backend processes for `Config`.".
start(Config) ->
    supervisor:start_child(?MODULE, #{
        id => ?RUNTIME,
        start => {?MODULE, start_link, [Config]},
        type => supervisor,
        shutdown => infinity
    }).

-doc "Stop the backend processes. The child spec is deleted so that `start/1` can run again.".
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
        write_pool_size := Write,
        read_pool_size := Read,
        writer := #{pool_size := Writers, call_timeout := Shutdown}
    } = Config
) ->
    Children = [
        emqx_omp_pg_store:pool_spec(omp_pg_write, Write, Config),
        emqx_omp_pg_store:pool_spec(omp_pg_read, Read, Config),
        emqx_omp_pg_worker_sup:child_spec(writers, emqx_omp_pg_writer, Writers, Shutdown, Config),
        #{id => maintenance, start => {emqx_omp_pg_maintenance, start_link, [Config]}}
    ],
    {ok, {#{strategy => one_for_one, intensity => 10, period => 10}, Children}}.
