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
-module(emqx_ds_builtin_local_meta_worker).

-behaviour(gen_server).

%% API:
-export([start_link/1]).

%% behavior callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([]).

-export_type([]).

%%================================================================================
%% Type declarations
%%================================================================================

%%================================================================================
%% API functions
%%================================================================================

-define(SERVER, ?MODULE).

-spec start_link(emqx_ds:db()) -> {ok, pid()}.
start_link(DB) ->
    gen_server:start_link(?MODULE, [DB], []).

%%================================================================================
%% behavior callbacks
%%================================================================================

-record(s, {db :: emqx_ds:db()}).

init([DB]) ->
    process_flag(trap_exit, true),
    S = #s{db = DB},
    self() ! {?MODULE, tick},
    {ok, S}.

handle_call(_Call, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Cast, S) ->
    {noreply, S}.

handle_info({?MODULE, tick}, S = #s{db = DB}) ->
    Shards = emqx_ds_builtin_local_meta:shards(DB),
    [tick(DB, Shard) || Shard <- Shards],
    erlang:send_after(100, self(), tick),
    {noreply, S};
handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, _S) ->
    ok.

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

tick(DB, Shard) ->
    ShardId = {DB, Shard},
    Now = max(
        erlang:system_time(microsecond), emqx_ds_builtin_local_meta:current_timestamp(ShardId) + 1
    ),
    emqx_ds_builtin_local_meta:set_current_timestamp(ShardId, Now),
    %% @TODO ????
    Events = emqx_ds_storage_layer:handle_event(ShardId, Now, {?MODULE, tick}),
    handle_events(ShardId, Now, Events).

handle_events(_ShardId, _Now, []) ->
    ok;
handle_events(ShardId, Now, [Event | Rest]) ->
    handle_events(ShardId, Now, emqx_ds_storage_layer:handle_event(ShardId, Now, Event)),
    handle_events(ShardId, Now, Rest).
