%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_rabbitmq_sup).

-feature(maybe_expr, enable).
-behaviour(supervisor).

-export([ensure_started/2]).
-export([ensure_deleted/1]).
-export([start_link/0]).
-export([init/1]).

-define(BRIDGE_SUP, ?MODULE).

ensure_started(SuperId, Config) ->
    {ok, SuperPid} = ensure_supervisor_started(SuperId),
    case supervisor:start_child(SuperPid, [Config]) of
        {ok, WorkPid} ->
            {ok, WorkPid};
        {error, {already_started, WorkPid}} ->
            {ok, WorkPid};
        {error, Error} ->
            {error, Error}
    end.

ensure_deleted(SuperId) ->
    maybe
        Pid = erlang:whereis(?BRIDGE_SUP),
        true ?= Pid =/= undefined,
        ok ?= supervisor:terminate_child(Pid, SuperId),
        ok ?= supervisor:delete_child(Pid, SuperId)
    else
        false -> ok;
        {error, not_found} -> ok;
        Error -> Error
    end.

ensure_supervisor_started(Id) ->
    SupervisorSpec =
        #{
            id => Id,
            start => {emqx_bridge_rabbitmq_source_sup, start_link, []},
            restart => permanent,
            type => supervisor
        },
    case supervisor:start_child(?BRIDGE_SUP, SupervisorSpec) of
        {ok, Pid} ->
            {ok, Pid};
        {error, {already_started, Pid}} ->
            {ok, Pid}
    end.

start_link() ->
    supervisor:start_link({local, ?BRIDGE_SUP}, ?MODULE, []).

init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 50,
        period => 10
    },
    ChildSpecs = [],
    {ok, {SupFlags, ChildSpecs}}.
