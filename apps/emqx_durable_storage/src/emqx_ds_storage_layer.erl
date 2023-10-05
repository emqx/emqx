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
-module(emqx_ds_storage_layer).

-behaviour(gen_server).

%% Replication layer API:
-export([open_shard/2, drop_shard/1, store_batch/3, get_streams/3, make_iterator/3, next/3]).

%% gen_server
-export([start_link/2, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export_type([gen_id/0, generation/0, cf_refs/0, stream/0, iterator/0]).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-type shard_id() :: emqx_ds_replication_layer:shard_id().

-type cf_refs() :: [{string(), rocksdb:cf_handle()}].

-type gen_id() :: 0..16#ffff.

%% Note: this record might be stored permanently on a remote node.
-record(stream, {
    generation :: gen_id(),
    enc :: _EncapsultatedData,
    misc = #{} :: map()
}).

-opaque stream() :: #stream{}.

%% Note: this record might be stored permanently on a remote node.
-record(it, {
    generation :: gen_id(),
    enc :: _EncapsultatedData,
    misc = #{} :: map()
}).

-opaque iterator() :: #it{}.

%%%% Generation:

-type generation(Data) :: #{
    %% Module that handles data for the generation:
    module := module(),
    %% Module-specific data defined at generation creation time:
    data := Data,
    %% When should this generation become active?
    %% This generation should only contain messages timestamped no earlier than that.
    %% The very first generation will have `since` equal 0.
    since := emqx_ds:time(),
    until := emqx_ds:time() | undefined
}.

%% Schema for a generation. Persistent term.
-type generation_schema() :: generation(term()).

%% Runtime view of generation:
-type generation() :: generation(term()).

%%%% Shard:

-type shard(GenData) :: #{
    %% ID of the current generation (where the new data is written:)
    current_generation := gen_id(),
    %% This data is used to create new generation:
    prototype := {module(), term()},
    %% Generations:
    {generation, gen_id()} => GenData
}.

%% Shard schema (persistent):
-type shard_schema() :: shard(generation_schema()).

%% Shard (runtime):
-type shard() :: shard(generation()).

%%================================================================================
%% Generation callbacks
%%================================================================================

%% Create the new schema given generation id and the options.
%% Create rocksdb column families.
-callback create(shard_id(), rocksdb:db_handle(), gen_id(), _Options) ->
    {_Schema, cf_refs()}.

%% Open the existing schema
-callback open(shard_id(), rocsdb:db_handle(), gen_id(), cf_refs(), _Schema) ->
    _Data.

-callback store_batch(shard_id(), _Data, [emqx_types:message()], emqx_ds:message_store_opts()) ->
    ok.

-callback get_streams(shard_id(), _Data, emqx_ds:topic_filter(), emqx_ds:time()) ->
    [_Stream].

-callback make_iterator(shard_id(), _Data, _Stream, emqx_ds:time()) ->
    emqx_ds:make_iterator_result(_Iterator).

-callback next(shard_id(), _Data, Iter, pos_integer()) ->
    {ok, Iter, [emqx_types:message()]} | {error, _}.

%%================================================================================
%% API for the replication layer
%%================================================================================

-spec open_shard(shard_id(), emqx_ds:create_db_opts()) -> ok.
open_shard(Shard, Options) ->
    emqx_ds_storage_layer_sup:ensure_shard(Shard, Options).

-spec drop_shard(shard_id()) -> ok.
drop_shard(Shard) ->
    emqx_ds_storage_layer_sup:stop_shard(Shard),
    ok = rocksdb:destroy(db_dir(Shard), []).

-spec store_batch(shard_id(), [emqx_types:message()], emqx_ds:message_store_opts()) ->
    emqx_ds:store_batch_result().
store_batch(Shard, Messages, Options) ->
    %% We always store messages in the current generation:
    GenId = generation_current(Shard),
    #{module := Mod, data := GenData} = generation_get(Shard, GenId),
    Mod:store_batch(Shard, GenData, Messages, Options).

-spec get_streams(shard_id(), emqx_ds:topic_filter(), emqx_ds:time()) ->
    [{integer(), stream()}].
get_streams(Shard, TopicFilter, StartTime) ->
    Gens = generations_since(Shard, StartTime),
    lists:flatmap(
        fun(GenId) ->
            #{module := Mod, data := GenData} = generation_get(Shard, GenId),
            Streams = Mod:get_streams(Shard, GenData, TopicFilter, StartTime),
            [
                {GenId, #stream{
                    generation = GenId,
                    enc = Stream
                }}
             || Stream <- Streams
            ]
        end,
        Gens
    ).

-spec make_iterator(shard_id(), stream(), emqx_ds:time()) ->
    emqx_ds:make_iterator_result(iterator()).
make_iterator(Shard, #stream{generation = GenId, enc = Stream}, StartTime) ->
    #{module := Mod, data := GenData} = generation_get(Shard, GenId),
    case Mod:make_iterator(Shard, GenData, Stream, StartTime) of
        {ok, Iter} ->
            {ok, #it{
                generation = GenId,
                enc = Iter
            }};
        {error, _} = Err ->
            Err
    end.

-spec next(shard_id(), iterator(), pos_integer()) ->
    emqx_ds:next_result(iterator()).
next(Shard, Iter = #it{generation = GenId, enc = GenIter0}, BatchSize) ->
    #{module := Mod, data := GenData} = generation_get(Shard, GenId),
    Current = generation_current(Shard),
    case Mod:next(Shard, GenData, GenIter0, BatchSize) of
        {ok, _GenIter, []} when GenId < Current ->
            %% This is a past generation. Storage layer won't write
            %% any more messages here. The iterator reached the end:
            %% the stream has been fully replayed.
            {ok, end_of_stream};
        {ok, GenIter, Batch} ->
            {ok, Iter#it{enc = GenIter}, Batch};
        Error = {error, _} ->
            Error
    end.

%%================================================================================
%% gen_server for the shard
%%================================================================================

-define(REF(ShardId), {via, gproc, {n, l, {?MODULE, ShardId}}}).

-spec start_link(shard_id(), emqx_ds:create_db_opts()) ->
    {ok, pid()}.
start_link(Shard, Options) ->
    gen_server:start_link(?REF(Shard), ?MODULE, {Shard, Options}, []).

-record(s, {
    shard_id :: emqx_ds:shard_id(),
    db :: rocksdb:db_handle(),
    cf_refs :: cf_refs(),
    schema :: shard_schema(),
    shard :: shard()
}).

%% Note: we specify gen_server requests as records to make use of Dialyzer:
-record(call_create_generation, {since :: emqx_ds:time()}).

-type server_state() :: #s{}.

-define(DEFAULT_CF, "default").
-define(DEFAULT_CF_OPTS, []).

init({ShardId, Options}) ->
    process_flag(trap_exit, true),
    logger:set_process_metadata(#{shard_id => ShardId, domain => [ds, storage_layer, shard]}),
    erase_schema_runtime(ShardId),
    {ok, DB, CFRefs0} = rocksdb_open(ShardId, Options),
    {Schema, CFRefs} =
        case get_schema_persistent(DB) of
            not_found ->
                create_new_shard_schema(ShardId, DB, CFRefs0, Options);
            Scm ->
                {Scm, CFRefs0}
        end,
    Shard = open_shard(ShardId, DB, CFRefs, Schema),
    S = #s{
        shard_id = ShardId,
        db = DB,
        cf_refs = CFRefs,
        schema = Schema,
        shard = Shard
    },
    commit_metadata(S),
    {ok, S}.

handle_call(#call_create_generation{since = Since}, _From, S0) ->
    S = add_generation(S0, Since),
    commit_metadata(S),
    {reply, ok, S};
handle_call(_Call, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Cast, S) ->
    {noreply, S}.

handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, #s{db = DB, shard_id = ShardId}) ->
    erase_schema_runtime(ShardId),
    ok = rocksdb:close(DB).

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

-spec open_shard(shard_id(), rocksdb:db_handle(), cf_refs(), shard_schema()) ->
    shard().
open_shard(ShardId, DB, CFRefs, ShardSchema) ->
    %% Transform generation schemas to generation runtime data:
    maps:map(
        fun
            ({generation, GenId}, GenSchema) ->
                open_generation(ShardId, DB, CFRefs, GenId, GenSchema);
            (_K, Val) ->
                Val
        end,
        ShardSchema
    ).

-spec add_generation(server_state(), emqx_ds:time()) -> server_state().
add_generation(S0, Since) ->
    #s{shard_id = ShardId, db = DB, schema = Schema0, shard = Shard0, cf_refs = CFRefs0} = S0,
    {GenId, Schema, NewCFRefs} = new_generation(ShardId, DB, Schema0, Since),
    CFRefs = NewCFRefs ++ CFRefs0,
    Key = {generation, GenId},
    Generation = open_generation(ShardId, DB, CFRefs, GenId, maps:get(Key, Schema)),
    Shard = Shard0#{Key => Generation},
    S0#s{
        cf_refs = CFRefs,
        schema = Schema,
        shard = Shard
    }.

-spec open_generation(shard_id(), rocksdb:db_handle(), cf_refs(), gen_id(), generation_schema()) ->
    generation().
open_generation(ShardId, DB, CFRefs, GenId, GenSchema) ->
    ?tp(debug, ds_open_generation, #{gen_id => GenId, schema => GenSchema}),
    #{module := Mod, data := Schema} = GenSchema,
    RuntimeData = Mod:open(ShardId, DB, GenId, CFRefs, Schema),
    GenSchema#{data => RuntimeData}.

-spec create_new_shard_schema(shard_id(), rocksdb:db_handle(), cf_refs(), _Options) ->
    {shard_schema(), cf_refs()}.
create_new_shard_schema(ShardId, DB, CFRefs, Options) ->
    ?tp(notice, ds_create_new_shard_schema, #{shard => ShardId, options => Options}),
    %% TODO: read prototype from options/config
    Schema0 = #{
        current_generation => 0,
        prototype => {emqx_ds_storage_reference, #{}}
    },
    {_NewGenId, Schema, NewCFRefs} = new_generation(ShardId, DB, Schema0, _Since = 0),
    {Schema, NewCFRefs ++ CFRefs}.

-spec new_generation(shard_id(), rocksdb:db_handle(), shard_schema(), emqx_ds:time()) ->
    {gen_id(), shard_schema(), cf_refs()}.
new_generation(ShardId, DB, Schema0, Since) ->
    #{current_generation := PrevGenId, prototype := {Mod, ModConf}} = Schema0,
    GenId = PrevGenId + 1,
    {GenData, NewCFRefs} = Mod:create(ShardId, DB, GenId, ModConf),
    GenSchema = #{module => Mod, data => GenData, since => Since, until => undefined},
    Schema = Schema0#{
        current_generation => GenId,
        {generation, GenId} => GenSchema
    },
    {GenId, Schema, NewCFRefs}.

%% @doc Commit current state of the server to both rocksdb and the persistent term
-spec commit_metadata(server_state()) -> ok.
commit_metadata(#s{shard_id = ShardId, schema = Schema, shard = Runtime, db = DB}) ->
    ok = put_schema_persistent(DB, Schema),
    put_schema_runtime(ShardId, Runtime).

-spec rocksdb_open(shard_id(), emqx_ds:create_db_opts()) ->
    {ok, rocksdb:db_handle(), cf_refs()} | {error, _TODO}.
rocksdb_open(Shard, Options) ->
    DBOptions = [
        {create_if_missing, true},
        {create_missing_column_families, true}
        | maps:get(db_options, Options, [])
    ],
    DBDir = db_dir(Shard),
    _ = filelib:ensure_dir(DBDir),
    ExistingCFs =
        case rocksdb:list_column_families(DBDir, DBOptions) of
            {ok, CFs} ->
                [{Name, []} || Name <- CFs, Name /= ?DEFAULT_CF];
            % DB is not present. First start
            {error, {db_open, _}} ->
                []
        end,
    ColumnFamilies = [
        {?DEFAULT_CF, ?DEFAULT_CF_OPTS}
        | ExistingCFs
    ],
    case rocksdb:open(DBDir, DBOptions, ColumnFamilies) of
        {ok, DBHandle, [_CFDefault | CFRefs]} ->
            {CFNames, _} = lists:unzip(ExistingCFs),
            {ok, DBHandle, lists:zip(CFNames, CFRefs)};
        Error ->
            Error
    end.

-spec db_dir(shard_id()) -> file:filename().
db_dir({DB, ShardId}) ->
    lists:flatten([atom_to_list(DB), $:, atom_to_list(ShardId)]).

%%--------------------------------------------------------------------------------
%% Schema access
%%--------------------------------------------------------------------------------

-spec generation_current(shard_id()) -> gen_id().
generation_current(Shard) ->
    #{current_generation := Current} = get_schema_runtime(Shard),
    Current.

-spec generation_get(shard_id(), gen_id()) -> generation().
generation_get(Shard, GenId) ->
    #{{generation, GenId} := GenData} = get_schema_runtime(Shard),
    GenData.

-spec generations_since(shard_id(), emqx_ds:time()) -> [gen_id()].
generations_since(Shard, Since) ->
    Schema = get_schema_runtime(Shard),
    maps:fold(
        fun
            ({generation, GenId}, #{until := Until}, Acc) when Until >= Since ->
                [GenId | Acc];
            (_K, _V, Acc) ->
                Acc
        end,
        [],
        Schema
    ).

-define(PERSISTENT_TERM(SHARD), {emqx_ds_storage_layer, SHARD}).

-spec get_schema_runtime(shard_id()) -> shard().
get_schema_runtime(Shard) ->
    persistent_term:get(?PERSISTENT_TERM(Shard)).

-spec put_schema_runtime(shard_id(), shard()) -> ok.
put_schema_runtime(Shard, RuntimeSchema) ->
    persistent_term:put(?PERSISTENT_TERM(Shard), RuntimeSchema),
    ok.

-spec erase_schema_runtime(shard_id()) -> ok.
erase_schema_runtime(Shard) ->
    persistent_term:erase(?PERSISTENT_TERM(Shard)),
    ok.

-undef(PERSISTENT_TERM).

-define(ROCKSDB_SCHEMA_KEY, <<"schema_v1">>).

-spec get_schema_persistent(rocksdb:db_handle()) -> shard_schema() | not_found.
get_schema_persistent(DB) ->
    case rocksdb:get(DB, ?ROCKSDB_SCHEMA_KEY, []) of
        {ok, Blob} ->
            Schema = binary_to_term(Blob),
            %% Sanity check:
            #{current_generation := _, prototype := _} = Schema,
            Schema;
        not_found ->
            not_found
    end.

-spec put_schema_persistent(rocksdb:db_handle(), shard_schema()) -> ok.
put_schema_persistent(DB, Schema) ->
    Blob = term_to_binary(Schema),
    rocksdb:put(DB, ?ROCKSDB_SCHEMA_KEY, Blob, []).

-undef(ROCKSDB_SCHEMA_KEY).
