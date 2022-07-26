%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_resource_sup).

-include("emqx_resource.hrl").

-behaviour(supervisor).

-export([start_link/0, start_workers/2, stop_workers/2]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => one_for_one, intensity => 10, period => 10},
    Metrics = emqx_metrics_worker:child_spec(?RES_METRICS),

    ResourceManager =
        #{
            id => emqx_resource_manager_sup,
            start => {emqx_resource_manager_sup, start_link, []},
            restart => permanent,
            shutdown => infinity,
            type => supervisor,
            modules => [emqx_resource_manager_sup]
        },
    {ok, {SupFlags, [Metrics, ResourceManager]}}.

start_workers(ResId, Opts) ->
    PoolSize = pool_size(Opts),
    _ = ensure_worker_pool(ResId, hash, [{size, PoolSize}]),
    lists:foreach(
        fun(Idx) ->
            _ = ensure_worker_added(ResId, {ResId, Idx}, Idx),
            ok = ensure_worker_started(ResId, Idx, Opts)
        end,
        lists:seq(1, PoolSize)
    ).

stop_workers(ResId, Opts) ->
    PoolSize = pool_size(Opts),
    lists:foreach(
        fun(Idx) ->
            ok = ensure_worker_stopped(ResId, Idx),
            ok = ensure_worker_removed(ResId, {ResId, Idx})
        end,
        lists:seq(1, PoolSize)
    ),
    _ = gproc_pool:delete(ResId),
    ok.

pool_size(Opts) ->
    maps:get(worker_pool_size, Opts, erlang:system_info(schedulers_online)).

ensure_worker_pool(Pool, Type, Opts) ->
    try
        gproc_pool:new(Pool, Type, Opts)
    catch
        error:exists -> ok
    end,
    ok.

ensure_worker_added(Pool, Name, Slot) ->
    try
        gproc_pool:add_worker(Pool, Name, Slot)
    catch
        error:exists -> ok
    end,
    ok.

ensure_worker_removed(Pool, Name) ->
    _ = gproc_pool:remove_worker(Pool, Name),
    ok.

-define(CHILD_ID(MOD, RESID, INDEX), {MOD, RESID, INDEX}).
ensure_worker_started(ResId, Idx, Opts) ->
    Mod = emqx_resource_worker,
    Spec = #{
        id => ?CHILD_ID(Mod, ResId, Idx),
        start => {Mod, start_link, [ResId, Idx, Opts]},
        restart => transient,
        shutdown => 5000,
        type => worker,
        modules => [Mod]
    },
    case supervisor:start_child(emqx_resource_sup, Spec) of
        {ok, _Pid} -> ok;
        {error, {already_started, _}} -> ok;
        {error, already_present} -> ok;
        {error, _} = Err -> Err
    end.

ensure_worker_stopped(ResId, Idx) ->
    ChildId = ?CHILD_ID(emqx_resource_worker, ResId, Idx),
    case supervisor:terminate_child(emqx_resource_sup, ChildId) of
        ok ->
            supervisor:delete_child(emqx_resource_sup, ChildId);
        {error, not_found} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.
