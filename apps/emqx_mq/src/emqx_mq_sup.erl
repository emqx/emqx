%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_sup).

-moduledoc """
Supervisor for the Message Queue application.

The structure of the supervisor is as follows:
- ?ROOT_SUP supervisor
  - ?GC_SUP supervisor — to host garbage collection tasks (emqx_mq_gc_worker)
  - ?CONSUMER_SUP supervisor - to host MQ consumers (emqx_mq_consumer)
  - ?MQ_SUP supervisor
    - ?METRICS_SUP supervisor - start MQ metrics workers
    - emqx_mq_gc - scheduler for garbage collection tasks
  - emqx_mq_controller (starts and stops services under ?MQ_SUP supervisor)
""".

-behaviour(supervisor).

%% Maintain the basic supervision tree
-export([
    start_link/0,
    start_consumer_sup/0,
    start_gc_sup/0,
    start_mq_sup/0
]).

%% Start and stop services/taks
-export([
    start_consumer/2,
    start_metrics/0,
    stop_metrics/0,
    start_gc_scheduler/0,
    stop_gc_scheduler/0,
    start_gc/0
]).

%% Supervisor callbacks
-export([init/1]).

-define(ROOT_SUP, ?MODULE).
-define(CONSUMER_SUP, emqx_mq_consumer_sup).
-define(GC_SUP, emqx_mq_gc_sup).
-define(MQ_SUP, emqx_mq_enabled_sup).

start_link() ->
    supervisor:start_link({local, ?ROOT_SUP}, ?MODULE, ?ROOT_SUP).

start_consumer_sup() ->
    supervisor:start_link({local, ?CONSUMER_SUP}, ?MODULE, ?CONSUMER_SUP).

start_gc_sup() ->
    supervisor:start_link({local, ?GC_SUP}, ?MODULE, ?GC_SUP).

start_mq_sup() ->
    supervisor:start_link({local, ?MQ_SUP}, ?MODULE, ?MQ_SUP).

start_gc_scheduler() ->
    ensure_child(?MQ_SUP, emqx_mq_gc:child_spec()).

start_metrics() ->
    ensure_child(?MQ_SUP, emqx_mq_metrics:child_spec()).

stop_metrics() ->
    ensure_no_child(?MQ_SUP, emqx_mq_metrics).

stop_gc_scheduler() ->
    ensure_no_child(?MQ_SUP, emqx_mq_gc).

start_consumer(Id, Args) ->
    case supervisor:start_child(?CONSUMER_SUP, emqx_mq_consumer:child_spec(Id, Args)) of
        {ok, Pid} ->
            {ok, Pid};
        {error, {already_started, Pid}} ->
            {ok, Pid};
        {error, {Reason, _ChildSpec}} ->
            {error, Reason};
        {error, Reason} ->
            {error, Reason}
    end.

start_gc() ->
    ensure_child(?GC_SUP, emqx_mq_gc_worker:child_spec()).

%% Internals

init(?ROOT_SUP) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 10
    },
    ChildSpecs = [
        consumer_sup_child_spec(),
        gc_sup_child_spec(),
        mq_sup_child_spec(),
        emqx_mq_controller:child_spec()
    ],
    {ok, {SupFlags, ChildSpecs}};
init(?CONSUMER_SUP) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 10
    },
    ChildSpecs = [],
    {ok, {SupFlags, ChildSpecs}};
init(?GC_SUP) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 10
    },
    ChildSpecs = [],
    {ok, {SupFlags, ChildSpecs}};
init(?MQ_SUP) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 10
    },
    ChildSpecs = [],
    {ok, {SupFlags, ChildSpecs}}.

consumer_sup_child_spec() ->
    #{
        id => ?CONSUMER_SUP,
        start => {?MODULE, start_consumer_sup, []},
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

mq_sup_child_spec() ->
    #{
        id => ?MQ_SUP,
        start => {?MODULE, start_mq_sup, []},
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
