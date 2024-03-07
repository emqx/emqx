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

%% @doc Metadata storage for the builtin sharded database.
%%
%% Currently metadata is stored in mria; that's not ideal, but
%% eventually we'll replace it, so it's important not to leak
%% implementation details from this module.
-module(emqx_ds_replication_layer_meta).

-compile(inline).

-behaviour(gen_server).

%% API:
-export([
    shards/1,
    my_shards/1,
    allocate_shards/2,
    replica_set/2,
    sites/0,
    node/1,
    open_db/2,
    get_options/1,
    update_db_config/2,
    drop_db/1,
    this_site/0,
    print_status/0
]).

%% gen_server
-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([
    open_db_trans/2,
    allocate_shards_trans/2,
    update_db_config_trans/2,
    drop_db_trans/1,
    claim_site/2,
    n_shards/1
]).

-export_type([site/0]).

-include_lib("stdlib/include/qlc.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(SERVER, ?MODULE).

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
    %% Sites that should contain the data when the cluster is in the
    %% stable state (no nodes are being added or removed from it):
    replica_set :: [site()],
    misc = #{} :: map()
}).

%% Persistent ID of the node (independent from the IP/FQDN):
-type site() :: binary().

%% Peristent term key:
-define(emqx_ds_builtin_site, emqx_ds_builtin_site).

%% Make Dialyzer happy
-define(NODE_PAT(),
    %% Equivalent of `#?NODE_TAB{_ = '_'}`:
    erlang:make_tuple(record_info(size, ?NODE_TAB), '_')
).

-define(SHARD_PAT(SHARD),
    %% Equivalent of `#?SHARD_TAB{shard = SHARD, _ = '_'}`
    erlang:make_tuple(record_info(size, ?SHARD_TAB), '_', [{#?SHARD_TAB.shard, SHARD}])
).

%%================================================================================
%% API funcions
%%================================================================================

-spec print_status() -> ok.
print_status() ->
    io:format("THIS SITE:~n~s~n", [this_site()]),
    io:format("~nSITES:~n", []),
    Nodes = [node() | nodes()],
    lists:foreach(
        fun(#?NODE_TAB{site = Site, node = Node}) ->
            Status =
                case lists:member(Node, Nodes) of
                    true -> up;
                    false -> down
                end,
            io:format("~s    ~p    ~p~n", [Site, Node, Status])
        end,
        eval_qlc(mnesia:table(?NODE_TAB))
    ),
    io:format(
        "~nSHARDS:~nId                             Replicas~n", []
    ),
    lists:foreach(
        fun(#?SHARD_TAB{shard = {DB, Shard}, replica_set = RS}) ->
            ShardStr = string:pad(io_lib:format("~p/~s", [DB, Shard]), 30),
            ReplicasStr = string:pad(io_lib:format("~p", [RS]), 40),
            io:format("~s ~s~n", [ShardStr, ReplicasStr])
        end,
        eval_qlc(mnesia:table(?SHARD_TAB))
    ).

-spec this_site() -> site().
this_site() ->
    persistent_term:get(?emqx_ds_builtin_site).

-spec n_shards(emqx_ds:db()) -> pos_integer().
n_shards(DB) ->
    [#?META_TAB{db_props = #{n_shards := NShards}}] = mnesia:dirty_read(?META_TAB, DB),
    NShards.

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec shards(emqx_ds:db()) -> [emqx_ds_replication_layer:shard_id()].
shards(DB) ->
    filter_shards(DB).

-spec my_shards(emqx_ds:db()) -> [emqx_ds_replication_layer:shard_id()].
my_shards(DB) ->
    Site = this_site(),
    filter_shards(DB, fun(#?SHARD_TAB{replica_set = ReplicaSet}) ->
        lists:member(Site, ReplicaSet)
    end).

allocate_shards(DB, Opts) ->
    case mria:transaction(?SHARD, fun ?MODULE:allocate_shards_trans/2, [DB, Opts]) of
        {atomic, Shards} ->
            {ok, Shards};
        {aborted, {shards_already_allocated, Shards}} ->
            {ok, Shards};
        {aborted, {insufficient_sites_online, Needed, Sites}} ->
            {error, #{reason => insufficient_sites_online, needed => Needed, sites => Sites}}
    end.

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

-spec node(site()) -> node() | undefined.
node(Site) ->
    case mnesia:dirty_read(?NODE_TAB, Site) of
        [#?NODE_TAB{node = Node}] ->
            Node;
        [] ->
            undefined
    end.

-spec get_options(emqx_ds:db()) -> emqx_ds_replication_layer:builtin_db_opts().
get_options(DB) ->
    case mnesia:dirty_read(?META_TAB, DB) of
        [#?META_TAB{db_props = Opts}] ->
            Opts;
        [] ->
            #{}
    end.

-spec open_db(emqx_ds:db(), emqx_ds_replication_layer:builtin_db_opts()) ->
    emqx_ds_replication_layer:builtin_db_opts().
open_db(DB, DefaultOpts) ->
    {atomic, Opts} = mria:transaction(?SHARD, fun ?MODULE:open_db_trans/2, [DB, DefaultOpts]),
    Opts.

-spec update_db_config(emqx_ds:db(), emqx_ds_replication_layer:builtin_db_opts()) ->
    ok | {error, _}.
update_db_config(DB, DefaultOpts) ->
    {atomic, Opts} = mria:transaction(?SHARD, fun ?MODULE:update_db_config_trans/2, [
        DB, DefaultOpts
    ]),
    Opts.

-spec drop_db(emqx_ds:db()) -> ok.
drop_db(DB) ->
    _ = mria:transaction(?SHARD, fun ?MODULE:drop_db_trans/1, [DB]),
    ok.

%%================================================================================
%% behavior callbacks
%%================================================================================

-record(s, {}).

init([]) ->
    process_flag(trap_exit, true),
    logger:set_process_metadata(#{domain => [ds, meta]}),
    ensure_tables(),
    ensure_site(),
    S = #s{},
    {ok, S}.

handle_call(_Call, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Cast, S) ->
    {noreply, S}.

handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, #s{}) ->
    persistent_term:erase(?emqx_ds_builtin_site),
    ok.

%%================================================================================
%% Internal exports
%%================================================================================

-spec open_db_trans(emqx_ds:db(), emqx_ds_replication_layer:builtin_db_opts()) ->
    emqx_ds_replication_layer:builtin_db_opts().
open_db_trans(DB, CreateOpts) ->
    case mnesia:wread({?META_TAB, DB}) of
        [] ->
            mnesia:write(#?META_TAB{db = DB, db_props = CreateOpts}),
            CreateOpts;
        [#?META_TAB{db_props = Opts}] ->
            Opts
    end.

-spec allocate_shards_trans(emqx_ds:db(), emqx_ds_replication_layer:builtin_db_opts()) -> [_Shard].
allocate_shards_trans(DB, Opts) ->
    NShards = maps:get(n_shards, Opts),
    NSites = maps:get(n_sites, Opts),
    ReplicationFactor = maps:get(replication_factor, Opts),
    NReplicas = min(NSites, ReplicationFactor),
    Shards = [integer_to_binary(I) || I <- lists:seq(0, NShards - 1)],
    AllSites = mnesia:match_object(?NODE_TAB, ?NODE_PAT(), read),
    case length(AllSites) of
        N when N >= NSites ->
            ok;
        _ ->
            mnesia:abort({insufficient_sites_online, NSites, AllSites})
    end,
    case mnesia:match_object(?SHARD_TAB, ?SHARD_PAT({DB, '_'}), write) of
        [] ->
            ok;
        Records ->
            ShardsAllocated = [Shard || #?SHARD_TAB{shard = {_DB, Shard}} <- Records],
            mnesia:abort({shards_already_allocated, ShardsAllocated})
    end,
    {Allocation, _} = lists:mapfoldl(
        fun(Shard, SSites) ->
            {Sites, _} = emqx_utils_stream:consume(NReplicas, SSites),
            {_, SRest} = emqx_utils_stream:consume(1, SSites),
            {{Shard, Sites}, SRest}
        end,
        emqx_utils_stream:repeat(emqx_utils_stream:list(AllSites)),
        Shards
    ),
    lists:map(
        fun({Shard, Sites}) ->
            ReplicaSet = [Site || #?NODE_TAB{site = Site} <- Sites],
            Record = #?SHARD_TAB{
                shard = {DB, Shard},
                replica_set = ReplicaSet
            },
            ok = mnesia:write(Record),
            Shard
        end,
        Allocation
    ).

-spec update_db_config_trans(emqx_ds:db(), emqx_ds_replication_layer:builtin_db_opts()) ->
    ok | {error, database}.
update_db_config_trans(DB, CreateOpts) ->
    case mnesia:wread({?META_TAB, DB}) of
        [#?META_TAB{db_props = Opts}] ->
            %% Since this is an update and not a reopen,
            %% we should keep the shard number and replication factor
            %% and not create a new shard server
            #{
                n_shards := NShards,
                replication_factor := ReplicationFactor
            } = Opts,

            mnesia:write(#?META_TAB{
                db = DB,
                db_props = CreateOpts#{
                    n_shards := NShards,
                    replication_factor := ReplicationFactor
                }
            }),
            ok;
        [] ->
            {error, no_database}
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
    ok = mria:create_table(?META_TAB, [
        {rlog_shard, ?SHARD},
        {type, ordered_set},
        {storage, disc_copies},
        {record_name, ?META_TAB},
        {attributes, record_info(fields, ?META_TAB)}
    ]),
    ok = mria:create_table(?NODE_TAB, [
        {rlog_shard, ?SHARD},
        {type, ordered_set},
        {storage, disc_copies},
        {record_name, ?NODE_TAB},
        {attributes, record_info(fields, ?NODE_TAB)}
    ]),
    ok = mria:create_table(?SHARD_TAB, [
        {rlog_shard, ?SHARD},
        {type, ordered_set},
        {storage, disc_copies},
        {record_name, ?SHARD_TAB},
        {attributes, record_info(fields, ?SHARD_TAB)}
    ]),
    ok = mria:wait_for_tables([?META_TAB, ?NODE_TAB, ?SHARD_TAB]).

ensure_site() ->
    Filename = filename:join(emqx_ds:base_dir(), "emqx_ds_builtin_site.eterm"),
    case file:consult(Filename) of
        {ok, [Site]} ->
            ok;
        _ ->
            Site = binary:encode_hex(crypto:strong_rand_bytes(8)),
            logger:notice("Creating a new site with ID=~s", [Site]),
            ok = filelib:ensure_dir(Filename),
            {ok, FD} = file:open(Filename, [write]),
            io:format(FD, "~p.", [Site]),
            file:close(FD)
    end,
    {atomic, ok} = mria:transaction(?SHARD, fun ?MODULE:claim_site/2, [Site, node()]),
    persistent_term:put(?emqx_ds_builtin_site, Site),
    ok.

eval_qlc(Q) ->
    case mnesia:is_transaction() of
        true ->
            qlc:eval(Q);
        false ->
            {atomic, Result} = mria:ro_transaction(?SHARD, fun() -> qlc:eval(Q) end),
            Result
    end.

filter_shards(DB) ->
    filter_shards(DB, const(true)).

-spec filter_shards(emqx_ds:db(), fun((_) -> boolean())) ->
    [emqx_ds_replication_layer:shard_id()].
filter_shards(DB, Predicate) ->
    filter_shards(DB, Predicate, fun(#?SHARD_TAB{shard = {_, ShardId}}) ->
        ShardId
    end).

filter_shards(DB, Predicate, Mapper) ->
    eval_qlc(
        qlc:q([
            Mapper(Shard)
         || #?SHARD_TAB{shard = {D, _}} = Shard <- mnesia:table(
                ?SHARD_TAB
            ),
            D =:= DB,
            Predicate(Shard)
        ])
    ).

const(Result) ->
    fun(_) ->
        Result
    end.
