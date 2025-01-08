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
-module(emqx_ds_builtin_local_batch_serializer).

-include_lib("emqx_durable_storage/include/emqx_ds.hrl").

%% API
-export([
    start_link/3,

    store_batch_atomic/4
]).

%% `gen_server' API
-export([
    init/1,
    handle_call/3,
    handle_cast/2
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(name(DB, SHARD), {n, l, {?MODULE, DB, SHARD}}).
-define(via(DB, SHARD), {via, gproc, ?name(DB, SHARD)}).

-record(store_batch_atomic, {batch :: emqx_ds:batch(), opts :: emqx_ds:message_store_opts()}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

start_link(DB, Shard, _Opts) ->
    gen_server:start_link(?via(DB, Shard), ?MODULE, [DB, Shard], []).

store_batch_atomic(DB, Shard, Batch, Opts) ->
    gen_server:call(?via(DB, Shard), #store_batch_atomic{batch = Batch, opts = Opts}, infinity).

%%------------------------------------------------------------------------------
%% `gen_server' API
%%------------------------------------------------------------------------------

init([DB, Shard]) ->
    process_flag(message_queue_data, off_heap),
    State = #{
        db => DB,
        shard => Shard
    },
    {ok, State}.

handle_call(#store_batch_atomic{batch = Batch, opts = StoreOpts}, _From, State) ->
    ShardId = shard_id(State),
    DBOpts = db_config(State),
    Result = do_store_batch_atomic(ShardId, Batch, DBOpts, StoreOpts),
    {reply, Result, State};
handle_call(Call, _From, State) ->
    {reply, {error, {unknown_call, Call}}, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

shard_id(#{db := DB, shard := Shard}) ->
    {DB, Shard}.

db_config(#{db := DB}) ->
    emqx_ds_builtin_local_meta:db_config(DB).

-spec do_store_batch_atomic(
    emqx_ds_storage_layer:shard_id(),
    emqx_ds:dsbatch(),
    emqx_ds_builtin_local:db_opts(),
    emqx_ds:message_store_opts()
) ->
    emqx_ds:store_batch_result().
do_store_batch_atomic(ShardId, #dsbatch{} = Batch, DBOpts, StoreOpts) ->
    #dsbatch{
        operations = Operations0,
        preconditions = Preconditions
    } = Batch,
    case emqx_ds_precondition:verify(emqx_ds_storage_layer, ShardId, Preconditions) of
        ok ->
            do_store_operations(ShardId, Operations0, DBOpts, StoreOpts);
        {precondition_failed, _} = PreconditionFailed ->
            {error, unrecoverable, PreconditionFailed};
        Error ->
            Error
    end;
do_store_batch_atomic(ShardId, Operations, DBOpts, StoreOpts) ->
    do_store_operations(ShardId, Operations, DBOpts, StoreOpts).

do_store_operations(ShardId, Operations0, DBOpts, _StoreOpts) ->
    ForceMonotonic = maps:get(force_monotonic_timestamps, DBOpts),
    {Latest, Operations} =
        emqx_ds_builtin_local:make_batch(
            ForceMonotonic,
            current_timestamp(ShardId),
            Operations0
        ),
    Result = emqx_ds_storage_layer:store_batch(ShardId, Operations, _Options = #{}),
    emqx_ds_builtin_local_meta:set_current_timestamp(ShardId, Latest),
    Result.

current_timestamp(ShardId) ->
    emqx_ds_builtin_local_meta:current_timestamp(ShardId).
