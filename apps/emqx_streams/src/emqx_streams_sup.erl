%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_sup).

-export([
    start_link/0,
    start_post_starter/1,
    start_metrics/0,
    start_gc_scheduler/0,
    start_gc_sup/0,
    start_gc/0
]).

-behaviour(supervisor).
-export([init/1]).

-define(ROOT_SUP, ?MODULE).
-define(GC_SUP, emqx_streams_gc_sup).

%%

start_link() ->
    supervisor:start_link({local, ?ROOT_SUP}, ?MODULE, ?ROOT_SUP).

start_gc_sup() ->
    supervisor:start_link({local, ?GC_SUP}, ?MODULE, ?GC_SUP).

start_post_starter(MFA) ->
    supervisor:start_child(?ROOT_SUP, post_start_child_spec(MFA)).

start_gc_scheduler() ->
    ensure_child(?ROOT_SUP, emqx_streams_gc:child_spec()).

start_metrics() ->
    ensure_child(?ROOT_SUP, emqx_streams_metrics:child_spec()).

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
        gc_sup_child_spec()
    ],
    {ok, {SupFlags, ChildSpecs}};
init(?GC_SUP) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 10
    },
    ChildSpecs = [],
    {ok, {SupFlags, ChildSpecs}}.

post_start_child_spec(MFA) ->
    #{
        id => post_start,
        start => MFA,
        restart => transient,
        type => worker,
        shutdown => brutal_kill
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
