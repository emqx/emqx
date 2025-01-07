%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
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
