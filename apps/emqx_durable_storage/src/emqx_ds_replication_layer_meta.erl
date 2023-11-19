%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc Metadata storage for the builtin sharded database.
%%
%% Currently metadata is stored in mria; that's not ideal, but
%% eventually we'll replace it, so it's important not to leak
%% implementation details from this module.
-module(emqx_ds_replication_layer_meta).

%% API:
-export([init/0, shards/1, replica_set/2, sites/0, open_db/2, drop_db/1]).

%% internal exports:
-export([open_db_trans/2, drop_db_trans/1, claim_site/2]).

-export_type([site/0]).

-include_lib("stdlib/include/qlc.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(SHARD, emqx_ds_builtin_metadata_shard).
%% DS database metadata:
-define(META_TAB, emqx_ds_builtin_metadata_tab).
%% Mapping from Site to the actual Erlang node:
-define(NODE_TAB, emqx_ds_builtin_node_tab).
%% Shard metadata:
-define(SHARD_TAB, emqx_ds_builtin_shard_tab).

-record(?META_TAB, {
    db :: emqx_ds:db(),
    db_props :: emqx_ds_replication_layer:builtin_db_opts()
}).

-record(?NODE_TAB, {
    site :: site(),
    node :: node(),
    misc = #{} :: map()
}).

-record(?SHARD_TAB, {
    shard :: {emqx_ds:db(), emqx_ds_replication_layer:shard_id()},
    replica_set :: [site()],
    leader :: node() | undefined,
    misc = #{} :: map()
}).

%% Persistent ID of the node (independent from the IP/FQDN):
-type site() :: binary().

%%================================================================================
%% API funcions
%%================================================================================

-spec init() -> ok.
init() ->
    ensure_tables(),
    ensure_site().

-spec shards(emqx_ds:db()) -> [emqx_ds_replication_layer:shard_id()].
shards(DB) ->
    eval_qlc(
        qlc:q([Shard || #?SHARD_TAB{shard = {D, Shard}} <- mnesia:table(?SHARD_TAB), D =:= DB])
    ).

-spec replica_set(emqx_ds:db(), emqx_ds_replication_layer:shard_id()) ->
    {ok, [site()]} | {error, _}.
replica_set(DB, Shard) ->
    case mnesia:dirty_read(?SHARD_TAB, {DB, Shard}) of
        [#?SHARD_TAB{replica_set = ReplicaSet}] ->
            {ok, ReplicaSet};
        [] ->
            {error, no_shard}
    end.

-spec sites() -> [site()].
sites() ->
    eval_qlc(qlc:q([Site || #?NODE_TAB{site = Site} <- mnesia:table(?NODE_TAB)])).

-spec open_db(emqx_ds:db(), emqx_ds_replication_layer:builtin_db_opts()) ->
    emqx_ds_replication_layer:builtin_db_opts().
open_db(DB, DefaultOpts) ->
    {atomic, Opts} = mria:transaction(?SHARD, fun ?MODULE:open_db_trans/2, [DB, DefaultOpts]),
    Opts.

-spec drop_db(emqx_ds:db()) -> ok.
drop_db(DB) ->
    _ = mria:transaction(?SHARD, fun ?MODULE:drop_db_trans/1, [DB]),
    ok.

%%================================================================================
%% behavior callbacks
%%================================================================================

%%================================================================================
%% Internal exports
%%================================================================================

-spec open_db_trans(emqx_ds:db(), emqx_ds_replication_layer:builtin_db_opts()) ->
    emqx_ds_replication_layer:builtin_db_opts().
open_db_trans(DB, CreateOpts) ->
    case mnesia:wread({?META_TAB, DB}) of
        [] ->
            NShards = maps:get(n_shards, CreateOpts),
            ReplicationFactor = maps:get(replication_factor, CreateOpts),
            mnesia:write(#?META_TAB{db = DB, db_props = CreateOpts}),
            create_shards(DB, NShards, ReplicationFactor),
            CreateOpts;
        [#?META_TAB{db_props = Opts}] ->
            Opts
    end.

-spec drop_db_trans(emqx_ds:db()) -> ok.
drop_db_trans(DB) ->
    mnesia:delete({?META_TAB, DB}),
    [mnesia:delete({?SHARD_TAB, Shard}) || Shard <- shards(DB)],
    ok.

-spec claim_site(site(), node()) -> ok.
claim_site(Site, Node) ->
    mnesia:write(#?NODE_TAB{site = Site, node = Node}).

%%================================================================================
%% Internal functions
%%================================================================================

ensure_tables() ->
    %% TODO: seems like it may introduce flakiness
    Majority = false,
    ok = mria:create_table(?META_TAB, [
        {rlog_shard, ?SHARD},
        {majority, Majority},
        {type, ordered_set},
        {storage, rocksdb_copies},
        {record_name, ?META_TAB},
        {attributes, record_info(fields, ?META_TAB)}
    ]),
    ok = mria:create_table(?NODE_TAB, [
        {rlog_shard, ?SHARD},
        {majority, Majority},
        {type, ordered_set},
        {storage, rocksdb_copies},
        {record_name, ?NODE_TAB},
        {attributes, record_info(fields, ?NODE_TAB)}
    ]),
    ok = mria:create_table(?SHARD_TAB, [
        {rlog_shard, ?SHARD},
        {majority, Majority},
        {type, ordered_set},
        {storage, ram_copies},
        {record_name, ?SHARD_TAB},
        {attributes, record_info(fields, ?SHARD_TAB)}
    ]),
    ok = mria:wait_for_tables([?META_TAB, ?NODE_TAB, ?SHARD_TAB]).

ensure_site() ->
    Filename = filename:join(emqx:data_dir(), "emqx_ds_builtin_site.eterm"),
    case file:consult(Filename) of
        {ok, [Site]} ->
            ok;
        _ ->
            Site = crypto:strong_rand_bytes(8),
            ok = filelib:ensure_dir(Filename),
            {ok, FD} = file:open(Filename, [write]),
            io:format(FD, "~p.", [Site]),
            file:close(FD)
    end,
    {atomic, ok} = mria:transaction(?SHARD, fun ?MODULE:claim_site/2, [Site, node()]),
    ok.

-spec create_shards(emqx_ds:db(), pos_integer(), pos_integer()) -> ok.
create_shards(DB, NShards, ReplicationFactor) ->
    Shards = [integer_to_binary(I) || I <- lists:seq(0, NShards - 1)],
    Sites = sites(),
    lists:foreach(
        fun(Shard) ->
            Hashes0 = [{hash(Shard, Site), Site} || Site <- Sites],
            Hashes = lists:sort(Hashes0),
            {_, Sites} = lists:unzip(Hashes),
            ReplicaSet = lists:sublist(Sites, 1, ReplicationFactor),
            Record = #?SHARD_TAB{
                shard = {DB, Shard},
                replica_set = ReplicaSet
            },
            mnesia:write(Record)
        end,
        Shards
    ).

-spec hash(emqx_ds_replication_layer:shard_id(), site()) -> any().
hash(Shard, Site) ->
    erlang:phash2({Shard, Site}).

eval_qlc(Q) ->
    case mnesia:is_transaction() of
        true ->
            qlc:eval(Q);
        false ->
            {atomic, Result} = mria:ro_transaction(?SHARD, fun() -> qlc:eval(Q) end),
            Result
    end.
