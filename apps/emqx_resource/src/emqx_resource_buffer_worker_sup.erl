%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_resource_buffer_worker_sup).
-behaviour(supervisor).

%%%=============================================================================
%%% Exports and Definitions
%%%=============================================================================

%% External API
-export([start_link/0]).

-export([start_workers/2, stop_workers/2, worker_pids/1]).

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

-spec start_workers(emqx_resource:resource_id(), _Opts :: #{atom() => _}) -> ok.
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

-spec stop_workers(emqx_resource:resource_id(), _Opts :: #{atom() => _}) -> ok.
stop_workers(ResId, Opts) ->
    WorkerPoolSize = worker_pool_size(Opts),
    lists:foreach(
        fun(Idx) ->
            _ = ensure_worker_removed(ResId, Idx),
            ensure_disk_queue_dir_absent(ResId, Idx)
        end,
        lists:seq(1, WorkerPoolSize)
    ),
    ensure_worker_pool_removed(ResId),
    ok.

-spec worker_pids(emqx_resource:resource_id()) -> [pid()].
worker_pids(ResId) ->
    lists:map(
        fun({_Name, Pid}) ->
            Pid
        end,
        gproc_pool:active_workers(ResId)
    ).

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
    Mod = emqx_resource_buffer_worker,
    Spec = #{
        id => ?CHILD_ID(Mod, ResId, Idx),
        start => {Mod, start_link, [ResId, Idx, Opts]},
        restart => transient,
        %% if we delay shutdown, when the pool is big, it will take a long time
        shutdown => brutal_kill,
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
    ChildId = ?CHILD_ID(emqx_resource_buffer_worker, ResId, Idx),
    case supervisor:terminate_child(?SERVER, ChildId) of
        ok ->
            _ = supervisor:delete_child(?SERVER, ChildId),
            %% no need to remove worker from the pool,
            %% because the entire pool will be force deleted later
            ok;
        {error, not_found} ->
            ok
    end.

ensure_disk_queue_dir_absent(ResourceId, Index) ->
    ok = emqx_resource_buffer_worker:clear_disk_queue_dir(ResourceId, Index),
    ok.

ensure_worker_pool_removed(ResId) ->
    gproc_pool:force_delete(ResId),
    ok.
