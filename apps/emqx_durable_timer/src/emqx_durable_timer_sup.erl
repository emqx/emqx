%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_durable_timer_sup).
-moduledoc """
```
       worker worker ...
          \     |    /
   pg     ?WORKERS_SUP  coordinator
     \         |        /
       ?SYSTEM(one_for_all)        % started on demand
               |
              ?TOP                 % root application supervisor
```
""".

-behaviour(supervisor).

%% API:
-export([ensure_running/0, start_top/0, stop_all_workers/0, start_worker/4]).

%% behavior callbacks:
-export([init/1]).

%% internal exports:
-export([start_link_system/0, start_link_workers_sup/0]).

-export_type([]).

-include("internals.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(TOP, ?MODULE).
-define(SYSTEM, emqx_durable_timer_system_sup).
-define(WORKERS_SUP, emqx_durable_timer_workers).

%%================================================================================
%% API functions
%%================================================================================

-spec start_top() -> supervisor:startlink_ret().
start_top() ->
    supervisor:start_link({local, ?TOP}, ?MODULE, ?TOP).

-spec stop_all_workers() -> ok.
stop_all_workers() ->
    Children = supervisor:which_children(?WORKERS_SUP),
    lists:foreach(
        fun({_, Child, _, _}) -> supervisor:terminate_child(?WORKERS_SUP, Child) end,
        Children
    ).

-spec start_worker(
    emqx_durable_timer_worker:kind(),
    emqx_durable_timer:type(),
    emqx_durable_timer:epoch(),
    emqx_ds:shard()
) -> ok.
start_worker(Kind, Type, Epoch, Shard) ->
    case supervisor:start_child(?WORKERS_SUP, [Kind, Type, Epoch, Shard]) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            ok;
        {error, normal} ->
            %% Happens when there's no data to replay:
            ok;
        Err ->
            Err
    end.

-spec ensure_running() -> ok.
ensure_running() ->
    ChildSpec = #{
        id => ?SYSTEM,
        start => {?MODULE, start_link_system, []},
        type => supervisor,
        shutdown => infinity
    },
    case supervisor:start_child(?TOP, ChildSpec) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            ok;
        {error, already_present} ->
            ok;
        Error ->
            Error
    end.

%%================================================================================
%% Internal exports
%%================================================================================

-spec start_link_system() -> supervisor:startlink_ret().
start_link_system() ->
    supervisor:start_link({local, ?SYSTEM}, ?MODULE, ?SYSTEM).

-spec start_link_workers_sup() -> supervisor:startlink_ret().
start_link_workers_sup() ->
    supervisor:start_link({local, ?WORKERS_SUP}, ?MODULE, ?WORKERS_SUP).

%%================================================================================
%% behavior callbacks
%%================================================================================

init(?TOP) ->
    _ = ets:new(?regs_tab, [public, named_table, set, {keypos, 1}]),
    %% Start dormant:
    Children = [],
    SupFlags = #{
        strategy => one_for_one,
        intensity => 100,
        period => 100
    },
    {ok, {SupFlags, Children}};
init(?SYSTEM) ->
    Children = [
        #{
            id => pg,
            start => {pg, start_link, [?workers_pg]},
            type => worker,
            shutdown => 5_000,
            restart => permanent
        },
        #{
            id => ?WORKERS_SUP,
            start => {?MODULE, start_link_workers_sup, []},
            shutdown => infinity,
            restart => permanent,
            type => supervisor
        },
        #{
            id => coordinator,
            start => {emqx_durable_timer, start_link, []},
            shutdown => 5_000,
            restart => permanent,
            type => worker
        }
    ],
    SupFlags = #{
        strategy => one_for_all,
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
            type => worker,
            restart => transient
        }
    ],
    SupFlags = #{
        strategy => simple_one_for_one,
        intensity => 100,
        period => 1
    },
    {ok, {SupFlags, Children}}.

%%================================================================================
%% Internal functions
%%================================================================================
