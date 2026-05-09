%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    %% NOTE: the rescheduled `tick' atom intentionally does not match
    %% this clause — it falls through to the catchall below and the
    %% periodic tick stops after this single iteration. Making it
    %% periodic broke emqx_ds_builtin_local_SUITE:t_store_batch_fail
    %% under cover-compiled enterprise CI on the v5 branches: meck
    %% could not purge the cover-compiled emqx_ds_storage_layer beam
    %% while the worker was actively calling into it every 100 ms.
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
    SystemNow = erlang:system_time(microsecond),
    %% On a fresh shard no transaction has been committed yet, so
    %% `current_timestamp/1' returns `undefined' — guard against the
    %% `undefined + 1' arithmetic that previously crashed the worker
    %% on its first tick.
    Now =
        case emqx_ds_builtin_local_meta:current_timestamp(ShardId) of
            undefined -> SystemNow;
            Latest -> max(SystemNow, Latest + 1)
        end,
    emqx_ds_builtin_local_meta:set_current_timestamp(ShardId, Now),
    %% @TODO ????
    Events = emqx_ds_storage_layer:handle_event(ShardId, Now, {?MODULE, tick}),
    handle_events(ShardId, Now, Events).

handle_events(_ShardId, _Now, []) ->
    ok;
handle_events(ShardId, Now, [Event | Rest]) ->
    handle_events(ShardId, Now, emqx_ds_storage_layer:handle_event(ShardId, Now, Event)),
    handle_events(ShardId, Now, Rest).
