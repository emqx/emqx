%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_sup).

-moduledoc """
Supervisor for the Message Streams application.

The structure of the supervisor is as follows:
- ?ROOT_SUP supervisor
  - ?GC_SUP supervisor — to host garbage collection tasks (emqx_streams_gc_worker)
  - ?STREAMS_SUP supervisor
    - ?METRICS_SUP supervisor - start Streams metrics workers
    - emqx_streams_gc - scheduler for garbage collection tasks
  - emqx_streams_controller (starts and stops services under ?STREAMS_SUP supervisor)
""".

-export([
    start_link/0,
    start_metrics/0,
    stop_metrics/0,
    start_gc_scheduler/0,
    stop_gc_scheduler/0,
    start_gc_sup/0,
    start_gc/0,
    start_streams_sup/0
]).

-behaviour(supervisor).
-export([init/1]).

-define(ROOT_SUP, ?MODULE).
-define(GC_SUP, emqx_streams_gc_sup).
-define(STREAMS_SUP, emqx_streams_enabled_sup).

%%

start_link() ->
    supervisor:start_link({local, ?ROOT_SUP}, ?MODULE, ?ROOT_SUP).

start_gc_sup() ->
    supervisor:start_link({local, ?GC_SUP}, ?MODULE, ?GC_SUP).

start_streams_sup() ->
    supervisor:start_link({local, ?STREAMS_SUP}, ?MODULE, ?STREAMS_SUP).

start_gc_scheduler() ->
    ensure_child(?STREAMS_SUP, emqx_streams_gc:child_spec()).

start_metrics() ->
    ensure_child(?STREAMS_SUP, emqx_streams_metrics:child_spec()).

stop_metrics() ->
    ensure_no_child(?STREAMS_SUP, emqx_streams_metrics).

stop_gc_scheduler() ->
    ensure_no_child(?STREAMS_SUP, emqx_streams_gc).

start_gc() ->
    ensure_child(?GC_SUP, emqx_streams_gc_worker:child_spec()).

%%

init(?ROOT_SUP) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 10
    },
    ChildSpecs = [
        gc_sup_child_spec(),
        streams_sup_child_spec(),
        emqx_streams_controller:child_spec()
    ],
    {ok, {SupFlags, ChildSpecs}};
init(?GC_SUP) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 10
    },
    ChildSpecs = [],
    {ok, {SupFlags, ChildSpecs}};
init(?STREAMS_SUP) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 10
    },
    ChildSpecs = [],
    {ok, {SupFlags, ChildSpecs}}.

streams_sup_child_spec() ->
    #{
        id => ?STREAMS_SUP,
        start => {?MODULE, start_streams_sup, []},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [?MODULE]
    }.

gc_sup_child_spec() ->
    #{
        id => ?GC_SUP,
        start => {?MODULE, start_gc_sup, []},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [?MODULE]
    }.

ensure_child(SupRef, ChildSpec) ->
    case supervisor:start_child(SupRef, ChildSpec) of
        {ok, _Pid} ->
            ok;
        {error, {already_started, _Pid}} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

ensure_no_child(SupRef, ChildId) ->
    case supervisor:terminate_child(SupRef, ChildId) of
        ok -> supervisor:delete_child(SupRef, ChildId);
        {error, not_found} -> ok;
        {error, Reason} -> {error, Reason}
    end.
