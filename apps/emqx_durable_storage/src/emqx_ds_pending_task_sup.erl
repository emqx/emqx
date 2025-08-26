%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_pending_task_sup).
-moduledoc """
A supervisor for pending schema migration tasks.
""".

-behaviour(supervisor).

%% API:
-export([start_link/0, spawn_task/3, terminate_task/2]).

%% behavior callbacks:
-export([init/1]).

%% internal exports:
-export([]).

-export_type([t/0]).

-include_lib("snabbkaffe/include/trace.hrl").
-include("emqx_dsch.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-type t() :: #{emqx_dsch:pending_id() => pid()}.

%%================================================================================
%% API functions
%%================================================================================

-define(SUP, ?MODULE).

-spec start_link() -> supervisor:startlink_ret().
start_link() ->
    supervisor:start_link({local, ?SUP}, ?MODULE, []).

-spec spawn_task(emqx_dsch:pending_id(), emqx_dsch:pending(), t()) -> t().
spawn_task(Id, TaskDefn, Pending) ->
    case supervisor:start_child(?SUP, [Id, TaskDefn]) of
        {ok, Pid} ->
            Pending#{Id => {running, Pid}};
        Other ->
            ?tp(warning, ?tp_pending_spawn_fail, #{
                id => Id, task => TaskDefn, reason => Other
            }),
            Pending#{Id => {failed, Other}}
    end.

-spec terminate_task(emqx_dsch:pending_id(), t()) -> t().
terminate_task(Id, Pending0) ->
    case maps:take(Id, Pending0) of
        {Task, Pending} ->
            case Task of
                {running, Pid} ->
                    _ = supervisor:terminate_child(?SUP, Pid),
                    Pending;
                {failed, _} ->
                    Pending0
            end;
        error ->
            Pending0
    end.

%%================================================================================
%% behavior callbacks
%%================================================================================

init([]) ->
    Children = [
        #{
            id => pending,
            start => {emqx_dsch, start_link_pending, []},
            shutdown => 15_000,
            restart => temporary,
            type => worker,
            significant => false
        }
    ],
    SupFlags = #{
        strategy => simple_one_for_one,
        intensity => 1000,
        period => 1,
        auto_shutdown => never
    },
    {ok, {SupFlags, Children}}.

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================
