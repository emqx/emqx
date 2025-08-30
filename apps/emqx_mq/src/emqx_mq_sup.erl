%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_sup).

-behaviour(supervisor).

-export([
    start_link/0,
    start_consumer_sup/0,
    start_gc_sup/0,
    start_consumer/2,
    start_gc/0
]).

-export([init/1]).

-define(ROOT_SUP, ?MODULE).
-define(CONSUMER_SUP, emqx_mq_consumer_sup).
-define(GC_SUP, emqx_mq_gc_sup).

start_link() ->
    supervisor:start_link({local, ?ROOT_SUP}, ?MODULE, ?ROOT_SUP).

start_consumer_sup() ->
    supervisor:start_link({local, ?CONSUMER_SUP}, ?MODULE, ?CONSUMER_SUP).

start_gc_sup() ->
    supervisor:start_link({local, ?GC_SUP}, ?MODULE, ?GC_SUP).

start_consumer(Id, Args) ->
    case supervisor:start_child(?CONSUMER_SUP, emqx_mq_consumer:child_spec(Id, Args)) of
        {ok, Pid} ->
            {ok, Pid};
        {error, {already_started, Pid}} ->
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

start_gc() ->
    case supervisor:start_child(?GC_SUP, emqx_mq_gc_worker:child_spec()) of
        {ok, _Pid} ->
            ok;
        {error, {already_started, _Pid}} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

init(?ROOT_SUP) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 10
    },
    ChildSpecs = [
        emqx_mq_metrics:child_spec(),
        consumer_sup_child_spec(),
        gc_sup_child_spec(),
        emqx_mq_gc:child_spec()
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
    {ok, {SupFlags, ChildSpecs}}.

consumer_sup_child_spec() ->
    #{
        id => ?CONSUMER_SUP,
        start => {?MODULE, start_consumer_sup, []},
        restart => permanent,
        shutdown => 5000,
        type => supervisor,
        modules => [?MODULE]
    }.

gc_sup_child_spec() ->
    #{
        id => ?GC_SUP,
        start => {?MODULE, start_gc_sup, []},
        restart => permanent,
        shutdown => 5000,
        type => supervisor,
        modules => [?MODULE]
    }.
