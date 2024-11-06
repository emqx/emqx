%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-export([start_link/3]).

%% behavior callbacks:
-export([init/1]).

%% internal exports:
-export([start_workers/3]).

-export_type([]).

%%================================================================================
%% Type declarations
%%================================================================================

-define(SUP(SHARD), {n, l, {?MODULE, SHARD}}).

%%================================================================================
%% API functions
%%================================================================================

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
            id => subscription_manager,
            type => worker,
            start => {emqx_ds_beamformer, start_link, [Module, ShardId, Opts]}
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
            id => {Type, I},
            type => worker,
            shutdown => 5000,
            start => {Type, start_link, [Module, ShardId, I, Opts]}
        }
     || I <- lists:seq(1, InitialNWorkers),
        Type <- [emqx_ds_beamformer_rt, emqx_ds_beamformer_catchup]
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

%%================================================================================
%% Internal functions
%%================================================================================
