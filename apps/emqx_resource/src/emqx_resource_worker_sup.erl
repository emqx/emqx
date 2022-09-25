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
-module(emqx_resource_worker_sup).
-behaviour(supervisor).

%%%=============================================================================
%%% Exports and Definitions
%%%=============================================================================

%% External API
-export([start_link/0]).

-export([start_workers/2, stop_workers/2]).

%% Callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%%=============================================================================
%%% API
%%%=============================================================================

-spec start_link() -> supervisor:startlink_ret().
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%%=============================================================================
%%% Callbacks
%%%=============================================================================

-spec init(list()) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}} | ignore.
init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 100,
        period => 30
    },
    ChildSpecs = [],
    {ok, {SupFlags, ChildSpecs}}.

start_workers(ResId, Opts) ->
    WorkerPoolSize = worker_pool_size(Opts),
    _ = ensure_worker_pool(ResId, hash, [{size, WorkerPoolSize}]),
    lists:foreach(
        fun(Idx) ->
            _ = ensure_worker_added(ResId, Idx),
            ok = ensure_worker_started(ResId, Idx, Opts)
        end,
        lists:seq(1, WorkerPoolSize)
    ).

stop_workers(ResId, Opts) ->
    WorkerPoolSize = worker_pool_size(Opts),
    lists:foreach(
        fun(Idx) ->
            ensure_worker_removed(ResId, Idx)
        end,
        lists:seq(1, WorkerPoolSize)
    ),
    ensure_worker_pool_removed(ResId),
    ok.

%%%=============================================================================
%%% Internal
%%%=============================================================================
worker_pool_size(Opts) ->
    maps:get(worker_pool_size, Opts, erlang:system_info(schedulers_online)).

ensure_worker_pool(ResId, Type, Opts) ->
    try
        gproc_pool:new(ResId, Type, Opts)
    catch
        error:exists -> ok
    end,
    ok.

ensure_worker_added(ResId, Idx) ->
    try
        gproc_pool:add_worker(ResId, {ResId, Idx}, Idx)
    catch
        error:exists -> ok
    end,
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
    case supervisor:start_child(?SERVER, Spec) of
        {ok, _Pid} -> ok;
        {error, {already_started, _}} -> ok;
        {error, already_present} -> ok;
        {error, _} = Err -> Err
    end.

ensure_worker_removed(ResId, Idx) ->
    ChildId = ?CHILD_ID(emqx_resource_worker, ResId, Idx),
    case supervisor:terminate_child(?SERVER, ChildId) of
        ok ->
            Res = supervisor:delete_child(?SERVER, ChildId),
            _ = gproc_pool:remove_worker(ResId, {ResId, Idx}),
            Res;
        {error, not_found} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

ensure_worker_pool_removed(ResId) ->
    try
        gproc_pool:delete(ResId)
    catch
        error:badarg -> ok
    end,
    ok.
