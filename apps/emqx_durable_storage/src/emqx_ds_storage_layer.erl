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
-module(emqx_ds_storage_layer).

-behaviour(gen_server).

%% Replication layer API:
-export([
    open_shard/2,
    drop_shard/1,
    store_batch/3,
    get_streams/3,
    make_iterator/4,
    update_iterator/3,
    next/3,
    update_config/2,
    add_generation/1,
    list_generations_with_lifetimes/1,
    drop_generation/2
]).

%% gen_server
-export([start_link/2, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([db_dir/1]).

-export_type([
    gen_id/0,
    generation/0,
    cf_refs/0,
    stream/0,
    iterator/0,
    shard_id/0,
    options/0,
    prototype/0,
    post_creation_context/0
]).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(REF(ShardId), {via, gproc, {n, l, {?MODULE, ShardId}}}).

%%================================================================================
%% Type declarations
%%================================================================================

%% # "Record" integer keys.  We use maps with integer keys to avoid persisting and sending
%% records over the wire.

%% tags:
-define(STREAM, 1).
-define(IT, 2).

%% keys:
-define(tag, 1).
-define(generation, 2).
-define(enc, 3).

-type prototype() ::
    {emqx_ds_storage_reference, emqx_ds_storage_reference:options()}
    | {emqx_ds_storage_bitfield_lts, emqx_ds_storage_bitfield_lts:options()}.

-type shard_id() :: {emqx_ds:db(), emqx_ds_replication_layer:shard_id()}.

-type cf_refs() :: [{string(), rocksdb:cf_handle()}].

-type gen_id() :: 0..16#ffff.

%% Note: this might be stored permanently on a remote node.
-opaque stream() ::
    #{
        ?tag := ?STREAM,
        ?generation := gen_id(),
        ?enc := term()
    }.

%% Note: this might be stred permanently on a remote node.
-opaque iterator() ::
    #{
        ?tag := ?IT,
        ?generation := gen_id(),
        ?enc := term()
    }.

%%%% Generation:

-define(GEN_KEY(GEN_ID), {generation, GEN_ID}).

-type generation(Data) :: #{
    %% Module that handles data for the generation:
    module := module(),
    %% Module-specific data defined at generation creation time:
    data := Data,
    %% Column families used by this generation
    cf_refs := cf_refs(),
    %% Time at which this was created.  Might differ from `since', in particular for the
    %% first generation.
    created_at := emqx_ds:time(),
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
    %% ID of the current generation (where the new data is written):
    current_generation := gen_id(),
    %% This data is used to create new generation:
    prototype := prototype(),
    %% Generations:
    ?GEN_KEY(gen_id()) => GenData
}.

%% Shard schema (persistent):
-type shard_schema() :: shard(generation_schema()).

%% Shard (runtime):
-type shard() :: shard(generation()).

-type options() :: map().

-type post_creation_context() ::
    #{
        shard_id := emqx_ds_storage_layer:shard_id(),
        db := rocksdb:db_handle(),
        new_gen_id := emqx_ds_storage_layer:gen_id(),
        old_gen_id := emqx_ds_storage_layer:gen_id(),
        new_cf_refs := cf_refs(),
        old_cf_refs := cf_refs(),
        new_gen_runtime_data := _NewData,
        old_gen_runtime_data := _OldData
    }.

%%================================================================================
%% Generation callbacks
%%================================================================================

%% Create the new schema given generation id and the options.
%% Create rocksdb column families.
-callback create(shard_id(), rocksdb:db_handle(), gen_id(), Options :: map()) ->
    {_Schema, cf_refs()}.

%% Open the existing schema
-callback open(shard_id(), rocksdb:db_handle(), gen_id(), cf_refs(), _Schema) ->
    _Data.

-callback drop(shard_id(), rocksdb:db_handle(), gen_id(), cf_refs(), _RuntimeData) ->
    ok | {error, _Reason}.

-callback store_batch(shard_id(), _Data, [emqx_types:message()], emqx_ds:message_store_opts()) ->
    emqx_ds:store_batch_result().

-callback get_streams(shard_id(), _Data, emqx_ds:topic_filter(), emqx_ds:time()) ->
    [_Stream].

-callback make_iterator(shard_id(), _Data, _Stream, emqx_ds:topic_filter(), emqx_ds:time()) ->
    emqx_ds:make_iterator_result(_Iterator).

-callback next(shard_id(), _Data, Iter, pos_integer()) ->
    {ok, Iter, [emqx_types:message()]} | {error, _}.

-callback post_creation_actions(post_creation_context()) -> _Data.

-optional_callbacks([post_creation_actions/1]).

%%================================================================================
%% API for the replication layer
%%================================================================================

-record(call_list_generations_with_lifetimes, {}).
-record(call_drop_generation, {gen_id :: gen_id()}).

-spec open_shard(shard_id(), options()) -> ok.
open_shard(Shard, Options) ->
    emqx_ds_storage_layer_sup:ensure_shard(Shard, Options).

-spec drop_shard(shard_id()) -> ok.
drop_shard(Shard) ->
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
    ?tp(get_streams_all_gens, #{gens => Gens}),
    lists:flatmap(
        fun(GenId) ->
            ?tp(get_streams_get_gen, #{gen_id => GenId}),
            case generation_get_safe(Shard, GenId) of
                {ok, #{module := Mod, data := GenData}} ->
                    Streams = Mod:get_streams(Shard, GenData, TopicFilter, StartTime),
                    [
                        {GenId, #{
                            ?tag => ?STREAM,
                            ?generation => GenId,
                            ?enc => Stream
                        }}
                     || Stream <- Streams
                    ];
                {error, not_found} ->
                    %% race condition: generation was dropped before getting its streams?
                    []
            end
        end,
        Gens
    ).

-spec make_iterator(shard_id(), stream(), emqx_ds:topic_filter(), emqx_ds:time()) ->
    emqx_ds:make_iterator_result(iterator()).
make_iterator(
    Shard, #{?tag := ?STREAM, ?generation := GenId, ?enc := Stream}, TopicFilter, StartTime
) ->
    case generation_get_safe(Shard, GenId) of
        {ok, #{module := Mod, data := GenData}} ->
            case Mod:make_iterator(Shard, GenData, Stream, TopicFilter, StartTime) of
                {ok, Iter} ->
                    {ok, #{
                        ?tag => ?IT,
                        ?generation => GenId,
                        ?enc => Iter
                    }};
                {error, _} = Err ->
                    Err
            end;
        {error, not_found} ->
            {error, end_of_stream}
    end.

-spec update_iterator(
    shard_id(), iterator(), emqx_ds:message_key()
) ->
    emqx_ds:make_iterator_result(iterator()).
update_iterator(
    Shard,
    #{?tag := ?IT, ?generation := GenId, ?enc := OldIter},
    DSKey
) ->
    case generation_get_safe(Shard, GenId) of
        {ok, #{module := Mod, data := GenData}} ->
            case Mod:update_iterator(Shard, GenData, OldIter, DSKey) of
                {ok, Iter} ->
                    {ok, #{
                        ?tag => ?IT,
                        ?generation => GenId,
                        ?enc => Iter
                    }};
                {error, _} = Err ->
                    Err
            end;
        {error, not_found} ->
            {error, end_of_stream}
    end.

-spec next(shard_id(), iterator(), pos_integer()) ->
    emqx_ds:next_result(iterator()).
next(Shard, Iter = #{?tag := ?IT, ?generation := GenId, ?enc := GenIter0}, BatchSize) ->
    case generation_get_safe(Shard, GenId) of
        {ok, #{module := Mod, data := GenData}} ->
            Current = generation_current(Shard),
            case Mod:next(Shard, GenData, GenIter0, BatchSize) of
                {ok, _GenIter, []} when GenId < Current ->
                    %% This is a past generation. Storage layer won't write
                    %% any more messages here. The iterator reached the end:
                    %% the stream has been fully replayed.
                    {ok, end_of_stream};
                {ok, GenIter, Batch} ->
                    {ok, Iter#{?enc := GenIter}, Batch};
                Error = {error, _} ->
                    Error
            end;
        {error, not_found} ->
            %% generation was possibly dropped by GC
            {ok, end_of_stream}
    end.

-spec update_config(shard_id(), emqx_ds:create_db_opts()) -> ok.
update_config(ShardId, Options) ->
    gen_server:call(?REF(ShardId), {?FUNCTION_NAME, Options}, infinity).

-spec add_generation(shard_id()) -> ok.
add_generation(ShardId) ->
    gen_server:call(?REF(ShardId), add_generation, infinity).

-spec list_generations_with_lifetimes(shard_id()) ->
    #{
        gen_id() => #{
            created_at := emqx_ds:time(),
            since := emqx_ds:time(),
            until := undefined | emqx_ds:time()
        }
    }.
list_generations_with_lifetimes(ShardId) ->
    gen_server:call(?REF(ShardId), #call_list_generations_with_lifetimes{}, infinity).

-spec drop_generation(shard_id(), gen_id()) -> ok.
drop_generation(ShardId, GenId) ->
    gen_server:call(?REF(ShardId), #call_drop_generation{gen_id = GenId}, infinity).

%%================================================================================
%% gen_server for the shard
%%================================================================================

-spec start_link(shard_id(), emqx_ds:create_db_opts()) ->
    {ok, pid()}.
start_link(Shard = {_, _}, Options) ->
    gen_server:start_link(?REF(Shard), ?MODULE, {Shard, Options}, []).

-record(s, {
    shard_id :: shard_id(),
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
                Prototype = maps:get(storage, Options),
                create_new_shard_schema(ShardId, DB, CFRefs0, Prototype);
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

handle_call({update_config, Options}, _From, #s{schema = Schema} = S0) ->
    Prototype = maps:get(storage, Options),
    S1 = S0#s{schema = Schema#{prototype := Prototype}},
    Since = emqx_message:timestamp_now(),
    S = add_generation(S1, Since),
    commit_metadata(S),
    {reply, ok, S};
handle_call(add_generation, _From, S0) ->
    Since = emqx_message:timestamp_now(),
    S = add_generation(S0, Since),
    commit_metadata(S),
    {reply, ok, S};
handle_call(#call_list_generations_with_lifetimes{}, _From, S) ->
    Generations = handle_list_generations_with_lifetimes(S),
    {reply, Generations, S};
handle_call(#call_drop_generation{gen_id = GenId}, _From, S0) ->
    {Reply, S} = handle_drop_generation(S0, GenId),
    commit_metadata(S),
    {reply, Reply, S};
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
            (?GEN_KEY(GenId), GenSchema) ->
                open_generation(ShardId, DB, CFRefs, GenId, GenSchema);
            (_K, Val) ->
                Val
        end,
        ShardSchema
    ).

-spec add_generation(server_state(), emqx_ds:time()) -> server_state().
add_generation(S0, Since) ->
    #s{shard_id = ShardId, db = DB, schema = Schema0, shard = Shard0, cf_refs = CFRefs0} = S0,
    Schema1 = update_last_until(Schema0, Since),
    Shard1 = update_last_until(Shard0, Since),

    #{current_generation := OldGenId, prototype := {CurrentMod, _ModConf}} = Schema0,
    OldKey = ?GEN_KEY(OldGenId),
    #{OldKey := OldGenSchema} = Schema0,
    #{cf_refs := OldCFRefs} = OldGenSchema,
    #{OldKey := #{module := OldMod, data := OldGenData}} = Shard0,

    {GenId, Schema, NewCFRefs} = new_generation(ShardId, DB, Schema1, Since),

    CFRefs = NewCFRefs ++ CFRefs0,
    Key = ?GEN_KEY(GenId),
    Generation0 =
        #{data := NewGenData0} =
        open_generation(ShardId, DB, CFRefs, GenId, maps:get(Key, Schema)),

    %% When the new generation's module is the same as the last one, we might want to
    %% perform actions like inheriting some of the previous (meta)data.
    NewGenData =
        run_post_creation_actions(
            #{
                shard_id => ShardId,
                db => DB,
                new_gen_id => GenId,
                old_gen_id => OldGenId,
                new_cf_refs => NewCFRefs,
                old_cf_refs => OldCFRefs,
                new_gen_runtime_data => NewGenData0,
                old_gen_runtime_data => OldGenData,
                new_module => CurrentMod,
                old_module => OldMod
            }
        ),
    Generation = Generation0#{data := NewGenData},

    Shard = Shard1#{current_generation := GenId, Key => Generation},
    S0#s{
        cf_refs = CFRefs,
        schema = Schema,
        shard = Shard
    }.

-spec handle_list_generations_with_lifetimes(server_state()) -> #{gen_id() => map()}.
handle_list_generations_with_lifetimes(#s{schema = ShardSchema}) ->
    maps:fold(
        fun
            (?GEN_KEY(GenId), GenSchema, Acc) ->
                Acc#{GenId => export_generation(GenSchema)};
            (_Key, _Value, Acc) ->
                Acc
        end,
        #{},
        ShardSchema
    ).

-spec export_generation(generation_schema()) -> map().
export_generation(GenSchema) ->
    maps:with([created_at, since, until], GenSchema).

-spec handle_drop_generation(server_state(), gen_id()) ->
    {ok | {error, current_generation}, server_state()}.
handle_drop_generation(#s{schema = #{current_generation := GenId}} = S0, GenId) ->
    {{error, current_generation}, S0};
handle_drop_generation(#s{schema = Schema} = S0, GenId) when
    not is_map_key(?GEN_KEY(GenId), Schema)
->
    {{error, not_found}, S0};
handle_drop_generation(S0, GenId) ->
    #s{
        shard_id = ShardId,
        db = DB,
        schema = #{?GEN_KEY(GenId) := GenSchema} = OldSchema,
        shard = OldShard,
        cf_refs = OldCFRefs
    } = S0,
    #{module := Mod, cf_refs := GenCFRefs} = GenSchema,
    #{?GEN_KEY(GenId) := #{data := RuntimeData}} = OldShard,
    case Mod:drop(ShardId, DB, GenId, GenCFRefs, RuntimeData) of
        ok ->
            CFRefs = OldCFRefs -- GenCFRefs,
            Shard = maps:remove(?GEN_KEY(GenId), OldShard),
            Schema = maps:remove(?GEN_KEY(GenId), OldSchema),
            S = S0#s{
                cf_refs = CFRefs,
                shard = Shard,
                schema = Schema
            },
            {ok, S}
    end.

-spec open_generation(shard_id(), rocksdb:db_handle(), cf_refs(), gen_id(), generation_schema()) ->
    generation().
open_generation(ShardId, DB, CFRefs, GenId, GenSchema) ->
    ?tp(debug, ds_open_generation, #{gen_id => GenId, schema => GenSchema}),
    #{module := Mod, data := Schema} = GenSchema,
    RuntimeData = Mod:open(ShardId, DB, GenId, CFRefs, Schema),
    GenSchema#{data => RuntimeData}.

-spec create_new_shard_schema(shard_id(), rocksdb:db_handle(), cf_refs(), prototype()) ->
    {shard_schema(), cf_refs()}.
create_new_shard_schema(ShardId, DB, CFRefs, Prototype) ->
    ?tp(notice, ds_create_new_shard_schema, #{shard => ShardId, prototype => Prototype}),
    %% TODO: read prototype from options/config
    Schema0 = #{
        current_generation => 0,
        prototype => Prototype
    },
    {_NewGenId, Schema, NewCFRefs} = new_generation(ShardId, DB, Schema0, _Since = 0),
    {Schema, NewCFRefs ++ CFRefs}.

-spec new_generation(shard_id(), rocksdb:db_handle(), shard_schema(), emqx_ds:time()) ->
    {gen_id(), shard_schema(), cf_refs()}.
new_generation(ShardId, DB, Schema0, Since) ->
    #{current_generation := PrevGenId, prototype := {Mod, ModConf}} = Schema0,
    GenId = PrevGenId + 1,
    {GenData, NewCFRefs} = Mod:create(ShardId, DB, GenId, ModConf),
    GenSchema = #{
        module => Mod,
        data => GenData,
        cf_refs => NewCFRefs,
        created_at => emqx_message:timestamp_now(),
        since => Since,
        until => undefined
    },
    Schema = Schema0#{
        current_generation => GenId,
        ?GEN_KEY(GenId) => GenSchema
    },
    {GenId, Schema, NewCFRefs}.

%% @doc Commit current state of the server to both rocksdb and the persistent term
-spec commit_metadata(server_state()) -> ok.
commit_metadata(#s{shard_id = ShardId, schema = Schema, shard = Runtime, db = DB}) ->
    ok = put_schema_persistent(DB, Schema),
    put_schema_runtime(ShardId, Runtime).

-spec rocksdb_open(shard_id(), options()) ->
    {ok, rocksdb:db_handle(), cf_refs()} | {error, _TODO}.
rocksdb_open(Shard, Options) ->
    DBOptions = [
        {create_if_missing, true},
        {create_missing_column_families, true},
        {enable_write_thread_adaptive_yield, false}
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
    filename:join([emqx_ds:base_dir(), atom_to_list(DB), binary_to_list(ShardId)]).

-spec update_last_until(Schema, emqx_ds:time()) -> Schema when Schema :: shard_schema() | shard().
update_last_until(Schema, Until) ->
    #{current_generation := GenId} = Schema,
    GenData0 = maps:get(?GEN_KEY(GenId), Schema),
    GenData = GenData0#{until := Until},
    Schema#{?GEN_KEY(GenId) := GenData}.

run_post_creation_actions(
    #{
        new_module := Mod,
        old_module := Mod,
        new_gen_runtime_data := NewGenData
    } = Context
) ->
    case erlang:function_exported(Mod, post_creation_actions, 1) of
        true ->
            Mod:post_creation_actions(Context);
        false ->
            NewGenData
    end;
run_post_creation_actions(#{new_gen_runtime_data := NewGenData}) ->
    %% Different implementation modules
    NewGenData.

%%--------------------------------------------------------------------------------
%% Schema access
%%--------------------------------------------------------------------------------

-spec generation_current(shard_id()) -> gen_id().
generation_current(Shard) ->
    #{current_generation := Current} = get_schema_runtime(Shard),
    Current.

-spec generation_get(shard_id(), gen_id()) -> generation().
generation_get(Shard, GenId) ->
    {ok, GenData} = generation_get_safe(Shard, GenId),
    GenData.

-spec generation_get_safe(shard_id(), gen_id()) -> {ok, generation()} | {error, not_found}.
generation_get_safe(Shard, GenId) ->
    case get_schema_runtime(Shard) of
        #{?GEN_KEY(GenId) := GenData} ->
            {ok, GenData};
        #{} ->
            {error, not_found}
    end.

-spec generations_since(shard_id(), emqx_ds:time()) -> [gen_id()].
generations_since(Shard, Since) ->
    Schema = get_schema_runtime(Shard),
    maps:fold(
        fun
            (?GEN_KEY(GenId), #{until := Until}, Acc) when Until >= Since ->
                [GenId | Acc];
            (_K, _V, Acc) ->
                Acc
        end,
        [],
        Schema
    ).

-define(PERSISTENT_TERM(SHARD), {emqx_ds_storage_layer, SHARD}).

-spec get_schema_runtime(shard_id()) -> shard().
get_schema_runtime(Shard = {_, _}) ->
    persistent_term:get(?PERSISTENT_TERM(Shard)).

-spec put_schema_runtime(shard_id(), shard()) -> ok.
put_schema_runtime(Shard = {_, _}, RuntimeSchema) ->
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
