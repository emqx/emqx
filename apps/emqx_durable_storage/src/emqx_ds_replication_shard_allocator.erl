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

-module(emqx_ds_replication_shard_allocator).

-export([start_link/1]).

-export([n_shards/1]).
-export([shard_meta/2]).

-behaviour(gen_server).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-define(db_meta(DB), {?MODULE, DB}).
-define(shard_meta(DB, SHARD), {?MODULE, DB, SHARD}).

%%

start_link(DB) ->
    gen_server:start_link(?MODULE, DB, []).

n_shards(DB) ->
    Meta = persistent_term:get(?db_meta(DB)),
    maps:get(n_shards, Meta).

shard_meta(DB, Shard) ->
    persistent_term:get(?shard_meta(DB, Shard)).

%%

-define(ALLOCATE_RETRY_TIMEOUT, 1_000).

init(DB) ->
    _ = erlang:process_flag(trap_exit, true),
    _ = logger:set_process_metadata(#{db => DB, domain => [ds, db, shard_allocator]}),
    State = #{db => DB, status => allocating},
    handle_allocate_shards(State, ok).

handle_call(_Call, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(timeout, State) ->
    handle_allocate_shards(State, noreply);
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #{db := DB, shards := Shards}) ->
    erase_db_meta(DB),
    erase_shards_meta(DB, Shards);
terminate(_Reason, #{}) ->
    ok.

%%

handle_allocate_shards(State, Ret) ->
    case allocate_shards(State) of
        {ok, NState} ->
            {Ret, NState};
        {error, Data} ->
            _ = logger:notice(
                Data#{
                    msg => "Shard allocation still in progress",
                    retry_in => ?ALLOCATE_RETRY_TIMEOUT
                }
            ),
            {Ret, State, ?ALLOCATE_RETRY_TIMEOUT}
    end.

%%

allocate_shards(State = #{db := DB}) ->
    case emqx_ds_replication_layer_meta:allocate_shards(DB) of
        {ok, Shards} ->
            logger:notice(#{msg => "Shards allocated", shards => Shards}),
            ok = start_shards(DB, emqx_ds_replication_layer_meta:my_shards(DB)),
            ok = start_egresses(DB, Shards),
            ok = save_db_meta(DB, Shards),
            ok = save_shards_meta(DB, Shards),
            {ok, State#{shards => Shards, status := ready}};
        {error, Reason} ->
            {error, Reason}
    end.

start_shards(DB, Shards) ->
    ok = lists:foreach(
        fun(Shard) ->
            ok = emqx_ds_builtin_db_sup:ensure_shard({DB, Shard})
        end,
        Shards
    ),
    ok = logger:info(#{msg => "Shards started", shards => Shards}),
    ok.

start_egresses(DB, Shards) ->
    ok = lists:foreach(
        fun(Shard) ->
            ok = emqx_ds_builtin_db_sup:ensure_egress({DB, Shard})
        end,
        Shards
    ),
    logger:info(#{msg => "Egresses started", shards => Shards}),
    ok.

save_db_meta(DB, Shards) ->
    persistent_term:put(?db_meta(DB), #{
        shards => Shards,
        n_shards => length(Shards)
    }).

save_shards_meta(DB, Shards) ->
    lists:foreach(fun(Shard) -> save_shard_meta(DB, Shard) end, Shards).

save_shard_meta(DB, Shard) ->
    Servers = emqx_ds_replication_layer_shard:shard_servers(DB, Shard),
    persistent_term:put(?shard_meta(DB, Shard), #{
        servers => Servers
    }).

erase_db_meta(DB) ->
    persistent_term:erase(?db_meta(DB)).

erase_shards_meta(DB, Shards) ->
    lists:foreach(fun(Shard) -> erase_shard_meta(DB, Shard) end, Shards).

erase_shard_meta(DB, Shard) ->
    persistent_term:erase(?shard_meta(DB, Shard)).
