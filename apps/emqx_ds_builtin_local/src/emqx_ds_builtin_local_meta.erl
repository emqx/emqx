%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_builtin_local_meta).

-behaviour(gen_server).

%% API:
-export([
    start_link/0,
    n_shards/1,
    shards/1,
    db_config/1,

    current_timestamp/1,
    set_current_timestamp/2,
    ensure_monotonic_timestamp/1
]).

%% behavior callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([]).

-export_type([]).

-include_lib("stdlib/include/ms_transform.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

%% We save timestamp of the last written message to a mnesia table.
%% The saved value is restored when the node restarts. This is needed
%% to create a timestamp that is truly monotonic even in presence of
%% node restarts.
-define(TS_TAB, emqx_ds_builtin_local_timestamp_tab).
-record(?TS_TAB, {
    id :: emqx_ds_storage_layer:dbshard(),
    latest :: integer()
}).

%%================================================================================
%% API functions
%%================================================================================

-define(SERVER, ?MODULE).

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec n_shards(emqx_ds:db()) -> pos_integer().
n_shards(DB) ->
    #{n_shards := NShards} = emqx_dsch:get_db_schema(DB),
    NShards.

-spec shards(emqx_ds:db()) -> [emqx_ds:shard()].
shards(DB) ->
    NShards = n_shards(DB),
    [integer_to_binary(Shard) || Shard <- lists:seq(0, NShards - 1)].

-spec db_config(emqx_ds:db()) -> emqx_ds_builtin_local:db_opts().
db_config(DB) ->
    #{runtime := RTC} = emqx_dsch:get_db_runtime(DB),
    Schema = emqx_dsch:get_db_schema(DB),
    maps:merge(Schema, RTC).

-spec set_current_timestamp(emqx_ds_storage_layer:dbshard(), emqx_ds:time()) -> ok.
set_current_timestamp(ShardId, Time) ->
    mria:dirty_write(?TS_TAB, #?TS_TAB{id = ShardId, latest = Time}).

-spec current_timestamp(emqx_ds_storage_layer:dbshard()) -> emqx_ds:time() | undefined.
current_timestamp(ShardId) ->
    case mnesia:dirty_read(?TS_TAB, ShardId) of
        [#?TS_TAB{latest = Latest}] ->
            Latest;
        [] ->
            undefined
    end.

-spec ensure_monotonic_timestamp(emqx_ds_storage_layer:dbshard()) -> emqx_ds:time().
ensure_monotonic_timestamp(ShardId) ->
    mria:dirty_update_counter({?TS_TAB, ShardId}, 1).

%%================================================================================
%% behavior callbacks
%%================================================================================

-record(s, {}).

init([]) ->
    process_flag(trap_exit, true),
    ensure_tables(),
    S = #s{},
    {ok, S}.

handle_call(_Call, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Cast, S) ->
    {noreply, S}.

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

ensure_tables() ->
    ok = mria:create_table(?TS_TAB, [
        {local_content, true},
        {type, set},
        {storage, disc_copies},
        {record_name, ?TS_TAB},
        {attributes, record_info(fields, ?TS_TAB)},
        {storage_properties, [{ets, [{read_concurrency, true}]}]}
    ]).
