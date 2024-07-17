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
-module(emqx_ds_beamformer).

-behavior(gen_server).

%% API:
-export([]).

%% behavior callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([start_link/2]).

-export_type([]).

%%================================================================================
%% Type declarations
%%================================================================================

%%================================================================================
%% API functions
%%================================================================================

-spec start_link(emqx_ds_storage_layer:shard_id(), integer()) -> {ok, pid()}.
start_link(ShardId, Name) ->
    gen_server:start_link(?MODULE, [ShardId, Name], []).

-spec poll() ->

%%================================================================================
%% behavior callbacks
%%================================================================================

-record(s, {
    shard,
    name
}).

init([ShardId, Name]) ->
    process_flag(trap_exit, true),
    S = #s{shard = ShardId, name = N},
    Pool = emqx_ds_pollers:pool(ShardId),
    gproc_pool:add_worker(Pool, Name),
    gproc_pool:connect_worker(Pool, Name),
    {ok, S}.

handle_call(_Call, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Cast, S) ->
    {noreply, S}.

handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, #s{shard = ShardId, name = Name}) ->
    Pool = emqx_ds_pollers:pool(ShardId),
    gproc_pool:disconnect_worker(Pool, Name),
    gproc_pool:remove_worker(Pool, Name),
    ok.

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================
