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
-module(emqx_ds_beamformer_sup).

-behaviour(supervisor).

%% API:
-export([start_link/3, pool/1, cbm/1]).

%% behavior callbacks:
-export([init/1]).

%% internal exports:
-export([start_workers/3, init_pool_owner/3]).

-export_type([]).

%%================================================================================
%% Type declarations
%%================================================================================

-define(SUP(SHARD), {n, l, {?MODULE, SHARD}}).

-define(cbm(DB), {?MODULE, DB}).

%%================================================================================
%% API functions
%%================================================================================

-spec cbm(_Shard) -> module().
cbm(DB) ->
    persistent_term:get(?cbm(DB)).

pool(Shard) ->
    {?MODULE, Shard}.

-spec start_link(module(), _Shard, emqx_ds_beamformer:opts()) -> supervisor:startlink_ret().
start_link(CBM, ShardId, Opts) ->
    supervisor:start_link(
        {via, gproc, ?SUP(ShardId)}, ?MODULE, {top, CBM, ShardId, Opts}
    ).

%%================================================================================
%% behavior callbacks
%%================================================================================

init({top, Module, ShardId, Opts}) ->
    Children = [
        #{
            id => pool_owner,
            type => worker,
            start => {proc_lib, start_link, [?MODULE, init_pool_owner, [self(), ShardId, Module]]}
        },
        #{
            id => workers,
            type => supervisor,
            shutdown => infinity,
            start => {?MODULE, start_workers, [Module, ShardId, Opts]}
        }
    ],
    SupFlags = #{
        strategy => one_for_all,
        intensity => 1,
        period => 1
    },
    {ok, {SupFlags, Children}};
init({workers, Module, ShardId, Opts}) ->
    #{n_workers := InitialNWorkers} = Opts,
    Children = [
        #{
            id => I,
            type => worker,
            shutdown => 5000,
            start => {emqx_ds_beamformer, start_link, [Module, ShardId, I, Opts]}
        }
     || I <- lists:seq(1, InitialNWorkers)
    ],
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 10
    },
    {ok, {SupFlags, Children}}.

%%================================================================================
%% Internal exports
%%================================================================================

start_workers(Module, ShardId, InitialNWorkers) ->
    supervisor:start_link(?MODULE, {workers, Module, ShardId, InitialNWorkers}).

%% Helper process that automatically destroys gproc pool when
%% supervisor is stopped:
-spec init_pool_owner(pid(), _Shard, module()) -> no_return().
init_pool_owner(Parent, ShardId, Module) ->
    process_flag(trap_exit, true),
    gproc_pool:new(pool(ShardId), hash, [{auto_size, true}]),
    persistent_term:put(?cbm(ShardId), Module),
    proc_lib:init_ack(Parent, {ok, self()}),
    %% Automatic cleanup:
    receive
        {'EXIT', _Pid, Reason} ->
            gproc_pool:force_delete(pool(ShardId)),
            persistent_term:erase(?cbm(ShardId)),
            exit(Reason)
    end.

%%================================================================================
%% Internal functions
%%================================================================================
