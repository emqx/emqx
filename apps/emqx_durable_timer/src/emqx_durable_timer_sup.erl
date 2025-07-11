%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_durable_timer_sup).

-behavior(supervisor).

%% API:
-export([start_link/0, start_worker_sup/0, stop_worker_sup/0, start_worker/5]).

%% behavior callbacks:
-export([init/1]).

%% internal exports:
-export([start_link_workers_sup/0]).

-export_type([]).

%%================================================================================
%% Type declarations
%%================================================================================

-define(TOP, ?MODULE).
-define(WORKERS_SUP, emqx_durable_timer_workers).

%%================================================================================
%% API functions
%%================================================================================

-spec start_link() -> supervisor:startlink_ret().
start_link() ->
    supervisor:start_link({local, ?TOP}, ?MODULE, ?TOP).

-spec start_worker_sup() -> ok.
start_worker_sup() ->
    case supervisor:restart_child(?TOP, types_sup) of
        {ok, _} ->
            ok;
        {ok, _, _} ->
            ok;
        {error, running} ->
            ok
    end.

-spec stop_worker_sup() -> ok.
stop_worker_sup() ->
    ok = supervisor:terminate_child(?TOP, types_sup).

-spec start_worker(
    emqx_durable_timer:type(),
    emqx_durable_timer:epoch(),
    emqx_ds:shard(),
    module(),
    emqx_durable_timer_worker:type()
) -> ok.
start_worker(Type, Epoch, Shard, CBM, Active) ->
    start_simple_child(?WORKERS_SUP, [Type, Epoch, Shard, CBM, Active]).

%%================================================================================
%% Internal exports
%%================================================================================

-spec start_link_workers_sup() -> supervisor:startlink_ret().
start_link_workers_sup() ->
    supervisor:start_link({local, ?WORKERS_SUP}, ?MODULE, ?WORKERS_SUP).

%%================================================================================
%% behavior callbacks
%%================================================================================

init(?TOP) ->
    Children = [
        #{
            id => types_sup,
            start => {?MODULE, start_link_workers_sup, []},
            shutdown => infinity,
            restart => permanent,
            type => supervisor
        },
        #{
            id => main,
            start => {emqx_durable_timer, start_link, []},
            shutdown => 5_000,
            restart => permanent,
            type => worker
        }
    ],
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 10
    },
    {ok, {SupFlags, Children}};
init(?WORKERS_SUP) ->
    Children = [
        #{
            id => worker,
            start => {emqx_durable_timer_worker, start_link, []},
            shutdown => 5_000,
            type => worker
        }
    ],
    SupFlags = #{
        strategy => simple_one_for_one,
        intensity => 10,
        period => 10
    },
    {ok, {SupFlags, Children}}.

%%================================================================================
%% Internal functions
%%================================================================================

start_simple_child(Sup, Args) ->
    case supervisor:start_child(Sup, Args) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            ok;
        Err ->
            Err
    end.
