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
-export([start_link/3, cbm/1, info/1]).

%% behavior callbacks:
-export([init/1]).

%% internal exports:
-export([start_workers/3]).

-export_type([]).

-include("emqx_ds_beamformer.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(SUP(SHARD), {n, l, {?MODULE, SHARD}}).

%%================================================================================
%% API functions
%%================================================================================

-spec cbm(_Shard) -> module().
cbm(DB) ->
    #{cbm := CBM} = persistent_term:get(?pt_gvar(DB)),
    CBM.

-spec start_link(module(), _Shard, emqx_ds_beamformer:opts()) -> supervisor:startlink_ret().
start_link(CBM, ShardId, Opts) ->
    supervisor:start_link(
        {via, gproc, ?SUP(ShardId)}, ?MODULE, {top, CBM, ShardId, Opts}
    ).

info(DBShard = {_, Shard}) ->
    case gproc:where(?SUP(DBShard)) of
        undefined ->
            {DBShard, down};
        Pid ->
            child_status({Shard, Pid, supervisor, [?MODULE]})
    end.

%%================================================================================
%% behavior callbacks
%%================================================================================

init({top, Module, ShardId, Opts}) ->
    Children = [
        #{
            id => leader,
            type => worker,
            start => {emqx_ds_beamformer, start_link, [ShardId, Module]}
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
        intensity => 10,
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
        Type <- [
            emqx_ds_beamformer_catchup,
            emqx_ds_beamformer_rt
        ]
    ],
    %% FIXME: currently we want crash in one worker to immediately
    %% escalate to the leader, since currently there's no mechanism to
    %% re-enqueue requests owned by crashed worker.
    SupFlags = #{
        strategy => one_for_all,
        intensity => 5,
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

child_status({Name, Pid, worker, _}) ->
    ProcessInfo = erlang:process_info(Pid, [message_queue_len, current_function]),
    {
        Name,
        {Pid, maps:from_list(ProcessInfo)}
    };
child_status({Name, Pid, supervisor, _}) ->
    ChildrenInfo = [child_status(Child) || Child <- supervisor:which_children(Pid)],
    {
        Name,
        {Pid, maps:from_list(ChildrenInfo)}
    }.
