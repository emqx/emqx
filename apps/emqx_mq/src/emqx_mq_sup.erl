%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_sup).

-include("emqx_mq_internal.hrl").

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
    find_queue_sup/1,
    stop_queue_sup/1,
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
    ensure_no_child(?MQ_SUP, ?MQ_METRICS_WORKER).

stop_gc_scheduler() ->
    ensure_no_child(?MQ_SUP, ?MQ_GC_SCHEDULER).

start_consumer(Id, Args) ->
    case supervisor:start_child(?CONSUMER_SUP, emqx_mq_queue_sup:child_spec(Id, Args)) of
        {ok, Sup} ->
            emqx_mq_queue_sup:consumer(Sup);
        {error, {already_started, Sup}} ->
            emqx_mq_queue_sup:consumer(Sup);
        {error, Reason} ->
            case emqx_mq_consumer:find(Id) of
                {ok, Consumer} -> {ok, Consumer};
                not_found -> consumer_start_error(Reason)
            end
    end.

-doc "Find the per-queue supervisor of the queue on this node.".
-spec find_queue_sup(emqx_mq_types:mqid()) -> {ok, pid()} | not_found.
find_queue_sup(Id) ->
    case lists:keyfind(Id, 1, supervisor:which_children(?CONSUMER_SUP)) of
        {Id, Sup, _, _} when is_pid(Sup) -> {ok, Sup};
        _ -> not_found
    end.

-doc """
Stop the per-queue supervisor of the queue on this node and wait until the consumer supervisor
removes it.
""".
-spec stop_queue_sup(emqx_mq_types:mqid()) -> ok.
stop_queue_sup(Id) ->
    case supervisor:terminate_child(?CONSUMER_SUP, Id) of
        ok -> ok;
        {error, not_found} -> ok
    end.

start_gc() ->
    ensure_child(?GC_SUP, emqx_mq_gc_worker:child_spec()).

%% Internals

consumer_start_error({{shutdown, {failed_to_start_child, consumer, Reason}}, _ChildSpec}) ->
    {error, Reason};
consumer_start_error({Reason, _ChildSpec}) ->
    {error, Reason};
consumer_start_error(Reason) ->
    {error, Reason}.

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
