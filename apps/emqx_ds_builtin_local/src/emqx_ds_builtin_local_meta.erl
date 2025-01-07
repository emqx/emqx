%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_ds_builtin_local_meta).

-behaviour(gen_server).

%% API:
-export([
    start_link/0,
    open_db/2,
    drop_db/1,
    n_shards/1,
    shards/1,
    db_config/1,
    update_db_config/2,

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

-define(META_TAB, emqx_ds_builtin_local_metadata_tab).
-record(?META_TAB, {
    db :: emqx_ds:db(),
    db_props :: emqx_ds_builtin_local:db_opts()
}).

%% We save timestamp of the last written message to a mnesia table.
%% The saved value is restored when the node restarts. This is needed
%% to create a timestamp that is truly monotonic even in presence of
%% node restarts.
-define(TS_TAB, emqx_ds_builtin_local_timestamp_tab).
-record(?TS_TAB, {
    id :: emqx_ds_storage_layer:shard_id(),
    latest :: integer()
}).

%%================================================================================
%% API functions
%%================================================================================

-define(SERVER, ?MODULE).

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec open_db(emqx_ds:db(), emqx_ds_builtin_local:db_opts()) ->
    emqx_ds_builtin_local:db_opts().
open_db(DB, CreateOpts = #{backend := builtin_local, storage := _, n_shards := _}) ->
    transaction(
        fun() ->
            case mnesia:wread({?META_TAB, DB}) of
                [] ->
                    mnesia:write(#?META_TAB{db = DB, db_props = CreateOpts}),
                    CreateOpts;
                [#?META_TAB{db_props = Opts}] ->
                    Opts
            end
        end
    ).

-spec drop_db(emqx_ds:db()) -> ok.
drop_db(DB) ->
    transaction(
        fun() ->
            MS = ets:fun2ms(fun(#?TS_TAB{id = ID}) when element(1, ID) =:= DB ->
                ID
            end),
            Timestamps = mnesia:select(?TS_TAB, MS, write),
            [mnesia:delete({?TS_TAB, I}) || I <- Timestamps],
            mnesia:delete({?META_TAB, DB})
        end
    ).

-spec update_db_config(emqx_ds:db(), emqx_ds_builtin_local:db_opts()) ->
    emqx_ds_builtin_local:db_opts().
update_db_config(DB, Opts) ->
    transaction(
        fun() ->
            mnesia:write(#?META_TAB{db = DB, db_props = Opts}),
            Opts
        end
    ).

-spec n_shards(emqx_ds:db()) -> pos_integer().
n_shards(DB) ->
    #{n_shards := NShards} = db_config(DB),
    NShards.

-spec shards(emqx_ds:db()) -> [emqx_ds_builtin_local:shard()].
shards(DB) ->
    NShards = n_shards(DB),
    [integer_to_binary(Shard) || Shard <- lists:seq(0, NShards - 1)].

-spec db_config(emqx_ds:db()) -> emqx_ds_builtin_local:db_opts().
db_config(DB) ->
    case mnesia:dirty_read(?META_TAB, DB) of
        [#?META_TAB{db_props = Props}] ->
            Props;
        [] ->
            error({no_such_db, DB})
    end.

-spec set_current_timestamp(emqx_ds_storage_layer:shard_id(), emqx_ds:time()) -> ok.
set_current_timestamp(ShardId, Time) ->
    mria:dirty_write(?TS_TAB, #?TS_TAB{id = ShardId, latest = Time}).

-spec current_timestamp(emqx_ds_storage_layer:shard_id()) -> emqx_ds:time() | undefined.
current_timestamp(ShardId) ->
    case mnesia:dirty_read(?TS_TAB, ShardId) of
        [#?TS_TAB{latest = Latest}] ->
            Latest;
        [] ->
            undefined
    end.

-spec ensure_monotonic_timestamp(emqx_ds_storage_layer:shard_id()) -> emqx_ds:time().
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
    ok = mria:create_table(?META_TAB, [
        {local_content, true},
        {type, ordered_set},
        {storage, disc_copies},
        {record_name, ?META_TAB},
        {attributes, record_info(fields, ?META_TAB)}
    ]),
    ok = mria:create_table(?TS_TAB, [
        {local_content, true},
        {type, set},
        {storage, disc_copies},
        {record_name, ?TS_TAB},
        {attributes, record_info(fields, ?TS_TAB)}
    ]).

transaction(Fun) ->
    case mria:transaction(mria:local_content_shard(), Fun) of
        {atomic, Result} ->
            Result;
        {aborted, Reason} ->
            {error, Reason}
    end.
