%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_ds_pollers).

-behavior(supervisor).

%% API:
-export([start_link/2, pool/1]).

%% behavior callbacks:
-export([init/1]).

%% internal exports:
-export([start_workers/2, init_pool_owner/2]).

-export_type([]).

%%================================================================================
%% Type declarations
%%================================================================================

-define(SUP(SHARD), {n, l, {?MODULE, SHARD}}).

%%================================================================================
%% API functions
%%================================================================================

pool(Shard) ->
    {?MODULE, Shard}.

-spec start_link(emqx_ds_storage_layer:shard_id(), non_neg_integer()) -> supervisor:startlink_ret().
start_link(ShardId, InitialNWorkers) ->
    supervisor:start_link({via, gproc, ?SUP(ShardId)}, ?MODULE, {top, ShardId, InitialNWorkers}).

%%================================================================================
%% behavior callbacks
%%================================================================================

init({top, ShardId, InitialNWorkers}) ->
    Children = [
        #{
            id => pool_owner,
            type => worker,
            start => {proc_lib, start_link, [?MODULE, init_pool_owner, [self(), ShardId]]}
        },
        #{
            id => workers,
            type => supervisor,
            shutdown => infinity,
            start => {?MODULE, start_workers, [ShardId, InitialNWorkers]}
        }
    ],
    SupFlags = #{
        strategy => one_for_all,
        intensity => 1,
        period => 1
    },
    {ok, {SupFlags, Children}};
init({workers, ShardId, InitialNWorkers}) ->
    Children = [
        #{
            id => I,
            type => worker,
            shutdown => 1000,
            start => {emqx_ds_beamformer, start_link, [ShardId, I]}
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

start_workers(ShardId, InitialNWorkers) ->
    supervisor:start_link(?MODULE, {workers, ShardId, InitialNWorkers}).

%% Helper process that automatically destroys gproc pool when
%% supervisor is stopped:
init_pool_owner(Parent, ShardId) ->
    process_flag(trap_exit, true),
    gproc_pool:new(pool(ShardId), hash, [{auto_size, true}]),
    logger:warning("Started poll workers for ~p", [ShardId]),
    proc_lib:init_ack(Parent, {ok, self()}),
    %% Automatic cleanup:
    receive
        {'EXIT', _Pid, Reason} ->
            gproc_pool:delete(pool(ShardId)),
            logger:warning("Stopped poll workers for ~p", [ShardId]),
            exit(Reason)
    end.

%%================================================================================
%% Internal functions
%%================================================================================
