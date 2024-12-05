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
    %% Lifecycle
    start_link/2,
    drop_shard/1,
    shard_info/2,

    %% Data
    store_batch/3,
    store_batch/4,
    prepare_batch/3,
    commit_batch/3,
    dispatch_events/3,

    get_streams/3,
    get_delete_streams/3,
    make_iterator/4,
    make_delete_iterator/4,
    update_iterator/3,
    next/4,

    generation/1,
    unpack_iterator/2,
    scan_stream/6,

    delete_next/5,

    %% Preconditions
    lookup_message/2,

    %% Generations
    update_config/3,
    add_generation/2,
    list_generations_with_lifetimes/1,
    drop_generation/2,
    find_generation/2,

    %% Globals
    store_global/3,
    fetch_global/2,

    %% Snapshotting
    flush/1,
    take_snapshot/1,
    accept_snapshot/1,

    %% Custom events
    handle_event/3
]).

%% gen_server
-export([
    init/1,
    format_status/1,
    handle_continue/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

%% internal exports:
-export([db_dir/1, base_dir/0]).

-export_type([
    gen_id/0,
    generation/0,
    batch/0,
    cf_refs/0,
    stream/0,
    delete_stream/0,
    stream_v1/0,
    iterator/0,
    delete_iterator/0,
    shard_id/0,
    options/0,
    prototype/0,
    cooked_batch/0,
    batch_store_opts/0,
    poll_iterators/0,
    event_dispatch_f/0
]).

-include("emqx_ds.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(REF(ShardId), {via, gproc, {n, l, {?MODULE, ShardId}}}).

-define(stream_v2(GENERATION, INNER), [GENERATION | INNER]).
-define(delete_stream(GENERATION, INNER), [GENERATION | INNER]).

%% Wrappers for the storage events:
-define(storage_event(GEN_ID, PAYLOAD), #{0 := 3333, 1 := GEN_ID, 2 := PAYLOAD}).
-define(mk_storage_event(GEN_ID, PAYLOAD), #{0 => 3333, 1 => GEN_ID, 2 => PAYLOAD}).

%%================================================================================
%% Type declarations
%%================================================================================

-define(APP, emqx_durable_storage).

%% # "Record" integer keys.  We use maps with integer keys to avoid persisting and sending
%% records over the wire.
%% tags:
-define(STREAM, 1).
-define(IT, 2).
-define(DELETE_IT, 3).
-define(COOKED_BATCH, 4).

%% keys:
-define(tag, 1).
-define(generation, 2).
-define(enc, 3).

-type prototype() ::
    {emqx_ds_storage_reference, emqx_ds_storage_reference:options()}
    | {emqx_ds_storage_bitfield_lts, emqx_ds_storage_bitfield_lts:options()}.

-type shard_id() :: {emqx_ds:db(), binary()}.

-type cf_ref() :: {string(), rocksdb:cf_handle()}.
-type cf_refs() :: [cf_ref()].

-type gen_id() :: 0..16#ffff.
-type gen_info() :: #{
    created_at := emqx_ds:time(),
    since := emqx_ds:time(),
    until := undefined | emqx_ds:time(),
    _ => _
}.

-type batch() :: [
    {emqx_ds:time(), emqx_types:message()}
    | emqx_ds:deletion()
].

%% Options affecting how batches should be stored.
%% See also: `emqx_ds:message_store_opts()'.
-type batch_store_opts() ::
    #{
        %% Whether the whole batch given to `store_batch' should be inserted atomically as
        %% a unit. Default: `false'.
        atomic => boolean(),
        %% Should the storage make sure that the batch is written durably? Non-durable
        %% writes are in general unsafe but require much less resources, i.e. with RocksDB
        %% non-durable (WAL-less) writes do not usually involve _any_ disk I/O.
        %% Default: `true'.
        durable => boolean()
    }.

%% Options affecting how batches should be prepared.
-type batch_prepare_opts() :: #{}.

%% TODO: kept for BPAPI compatibility. Remove me on EMQX v5.6
-opaque stream_v1() ::
    #{
        ?tag := ?STREAM,
        ?generation := gen_id(),
        ?enc := term()
    }.

%% Note: this might be stored permanently on a remote node.
-opaque stream() :: nonempty_maybe_improper_list(gen_id(), term()).

%% Note: this might be stored permanently on a remote node.
-opaque delete_stream() :: stream().

%% Note: this might be stored permanently on a remote node.
-opaque iterator() ::
    #{
        ?tag := ?IT,
        ?generation := gen_id(),
        ?enc := term()
    }.

%% Note: this might be stored permanently on a remote node.
-opaque delete_iterator() ::
    #{
        ?tag := ?DELETE_IT,
        ?generation := gen_id(),
        ?enc := term()
    }.

-opaque cooked_batch() ::
    #{
        ?tag := ?COOKED_BATCH,
        ?generation := gen_id(),
        ?enc := term()
    }.

-type event_dispatch_f() :: fun(([stream()]) -> ok).

%%%% Generation:

-define(GEN_KEY(GEN_ID), {generation, GEN_ID}).

-type generation(Data) :: #{
    %% Module that handles data for the generation:
    module := module(),
    %% Module-specific data defined at generation creation time:
    data := Data,
    %% Column families used by this generation
    cf_names := [string()],
    %% Time at which this was created.  Might differ from `since', in particular for the
    %% first generation.
    created_at := emqx_message:timestamp(),
    %% When should this generation become active?
    %% This generation should only contain messages timestamped no earlier than that.
    %% The very first generation will have `since` equal 0.
    since := emqx_ds:time(),
    until := emqx_ds:time() | undefined
}.

%% Module-specific runtime data, as instantiated by `Mod:open/5` callback function.
-type generation_data() :: term().

%% Schema for a generation. Persistent term.
-type generation_schema() :: generation(term()).

%% Runtime view of generation:
-type generation() :: generation(generation_data()).

%%%% Shard:

-type shard(GenData) :: #{
    %% ID of the current generation (where the new data is written):
    current_generation := gen_id(),
    %% This data is used to create new generation:
    prototype := prototype(),
    %% Generations:
    ?GEN_KEY(gen_id()) => GenData,
    %% DB handle (runtime only).
    db => rocksdb:db_handle()
}.

%% Shard schema (persistent):
-type shard_schema() :: shard(generation_schema()).

%% Shard (runtime):
-type shard() :: shard(generation()).

%% Which DB options to provide to the storage layout module:
%% See `emqx_ds:db_opts()`.
-define(STORAGE_LAYOUT_DB_OPTS, [
    append_only,
    atomic_batches
]).

-define(GLOBAL(K), <<"G/", K/binary>>).

-type options() :: map().

-type poll_iterators() :: [{_UserData, iterator()}].

-define(ERR_GEN_GONE, {error, unrecoverable, generation_not_found}).
-define(ERR_BUFF_FULL, {error, recoverable, reached_max}).

%%================================================================================
%% Generation callbacks
%%================================================================================

%% Create the new schema given generation id and the options.
%% Create rocksdb column families.
-callback create(
    shard_id(),
    rocksdb:db_handle(),
    gen_id(),
    Options :: map(),
    generation_data() | undefined,
    emqx_ds:db_opts()
) ->
    {_Schema, cf_refs()}.

%% Open the existing schema
-callback open(shard_id(), rocksdb:db_handle(), gen_id(), cf_refs(), _Schema) ->
    generation_data().

%% Delete the schema and data
-callback drop(shard_id(), rocksdb:db_handle(), gen_id(), cf_refs(), generation_data()) ->
    ok | {error, _Reason}.

-callback prepare_batch(
    shard_id(),
    generation_data(),
    [{emqx_ds:time(), emqx_types:message()}, ...],
    batch_store_opts()
) ->
    {ok, term()} | emqx_ds:error(_).

-callback commit_batch(
    shard_id(),
    generation_data(),
    _CookedBatch,
    batch_store_opts()
) -> ok | emqx_ds:error(_).

-callback get_streams(
    shard_id(), generation_data(), emqx_ds:topic_filter(), emqx_ds:time()
) ->
    [_Stream].

-callback get_delete_streams(
    shard_id(), generation_data(), emqx_ds:topic_filter(), emqx_ds:time()
) ->
    [_Stream].

-callback make_iterator(
    shard_id(), generation_data(), _Stream, emqx_ds:topic_filter(), emqx_ds:time()
) ->
    emqx_ds:make_iterator_result(_Iterator).

-callback make_delete_iterator(
    shard_id(), generation_data(), _DeleteStream, emqx_ds:topic_filter(), emqx_ds:time()
) ->
    emqx_ds:make_delete_iterator_result(_Iterator).

-callback next(
    shard_id(), generation_data(), Iter, pos_integer(), emqx_ds:time(), _IsCurrent :: boolean()
) ->
    {ok, Iter, [emqx_types:message()]} | {ok, end_of_stream} | {error, _}.

-callback delete_next(
    shard_id(),
    generation_data(),
    DeleteIterator,
    emqx_ds:delete_selector(),
    pos_integer(),
    emqx_ds:time(),
    _IsCurrentGeneration :: boolean()
) ->
    {ok, DeleteIterator, _NDeleted :: non_neg_integer(), _IteratedOver :: non_neg_integer()}
    | {ok, end_of_stream}
    | emqx_ds:error(_).

%% Lookup a single message, for preconditions to work.
-callback lookup_message(shard_id(), generation_data(), emqx_ds_precondition:matcher()) ->
    emqx_types:message() | not_found | emqx_ds:error(_).

-callback handle_event(shard_id(), generation_data(), emqx_ds:time(), CustomEvent | tick) ->
    [CustomEvent].

%% Stream event API:

-callback batch_events(
    shard_id(),
    generation_data(),
    _CookedBatch
) -> [_Stream].

-optional_callbacks([
    handle_event/4,
    %% FIXME: should be mandatory:
    batch_events/3
]).

%%================================================================================
%% API for the replication layer
%%================================================================================

%% Note: we specify gen_server requests as records to make use of Dialyzer:
-record(call_add_generation, {since :: emqx_ds:time()}).
-record(call_update_config, {options :: emqx_ds:create_db_opts(), since :: emqx_ds:time()}).
-record(call_list_generations_with_lifetimes, {}).
-record(call_drop_generation, {gen_id :: gen_id()}).
-record(call_flush, {}).
-record(call_take_snapshot, {}).

-spec drop_shard(shard_id()) -> ok.
drop_shard(Shard) ->
    ok = rocksdb:destroy(db_dir(Shard), []).

%% @doc This is a convenicence wrapper that combines `prepare' and
%% `commit' operations.
-spec store_batch(shard_id(), batch(), batch_store_opts()) ->
    emqx_ds:store_batch_result().
store_batch(Shard, Messages, Options) ->
    DispatchF = fun(_) -> ok end,
    store_batch(Shard, Messages, Options, DispatchF).

%% @doc This is a convenicence wrapper that combines `prepare',
%% `commit' and `dispatch_events' operations.
-spec store_batch(
    shard_id(),
    batch(),
    batch_store_opts(),
    event_dispatch_f()
) ->
    emqx_ds:store_batch_result().
store_batch(Shard, Batch, Options, DispatchF) ->
    case prepare_batch(Shard, Batch, #{}) of
        {ok, CookedBatch} ->
            Result = commit_batch(Shard, CookedBatch, Options),
            dispatch_events(Shard, CookedBatch, DispatchF),
            Result;
        ignore ->
            ok;
        Error = {error, _, _} ->
            Error
    end.

%% @doc Transform a batch of messages into a "cooked batch" that can
%% be stored in the transaction log or transfered over the network.
%%
%% Important: the caller MUST ensure that timestamps within the shard
%% form a strictly increasing monotonic sequence through out the whole
%% lifetime of the shard.
%%
%% The underlying storage layout MAY use timestamp as a unique message
%% ID.
-spec prepare_batch(shard_id(), batch(), batch_prepare_opts()) ->
    {ok, cooked_batch()} | ignore | emqx_ds:error(_).
prepare_batch(Shard, Batch, Options) ->
    %% NOTE
    %% We assume that batches do not span generations. Callers should enforce this.
    ?tp(emqx_ds_storage_layer_prepare_batch, #{
        shard => Shard, batch => Batch, options => Options
    }),
    %% FIXME: always store messages in the current generation
    Time = batch_starts_at(Batch),
    case is_integer(Time) andalso generation_at(Shard, Time) of
        {GenId, #{module := Mod, data := GenData}} ->
            T0 = erlang:monotonic_time(microsecond),
            Result =
                case Mod:prepare_batch(Shard, GenData, Batch, Options) of
                    {ok, CookedBatch} ->
                        {ok, #{?tag => ?COOKED_BATCH, ?generation => GenId, ?enc => CookedBatch}};
                    Error = {error, _, _} ->
                        Error
                end,
            T1 = erlang:monotonic_time(microsecond),
            %% TODO store->prepare
            emqx_ds_builtin_metrics:observe_store_batch_time(Shard, T1 - T0),
            Result;
        false ->
            %% No write operations in this batch.
            ignore;
        not_found ->
            %% Generation is likely already GCed.
            ignore
    end.

-spec batch_starts_at(batch()) -> emqx_ds:time() | undefined.
batch_starts_at([{Time, _Message} | _]) when is_integer(Time) ->
    Time;
batch_starts_at([{delete, #message_matcher{timestamp = Time}} | _]) ->
    Time;
batch_starts_at([]) ->
    undefined.

%% @doc Commit cooked batch to the storage.
%%
%% The underlying storage layout must guarantee that this operation is
%% idempotent.
-spec commit_batch(
    shard_id(),
    cooked_batch(),
    batch_store_opts()
) -> emqx_ds:store_batch_result().
commit_batch(Shard, #{?tag := ?COOKED_BATCH, ?generation := GenId, ?enc := CookedBatch}, Options) ->
    #{?GEN_KEY(GenId) := #{module := Mod, data := GenData}} = get_schema_runtime(Shard),
    T0 = erlang:monotonic_time(microsecond),
    Result = Mod:commit_batch(Shard, GenData, CookedBatch, Options),
    T1 = erlang:monotonic_time(microsecond),
    emqx_ds_builtin_metrics:observe_store_batch_time(Shard, T1 - T0),
    Result.

-spec dispatch_events(
    shard_id(),
    cooked_batch(),
    event_dispatch_f()
) -> ok.
dispatch_events(
    Shard, #{?tag := ?COOKED_BATCH, ?generation := GenId, ?enc := CookedBatch}, DispatchF
) ->
    #{?GEN_KEY(GenId) := #{module := Mod, data := GenData}} = get_schema_runtime(Shard),
    Events = Mod:batch_events(Shard, GenData, CookedBatch),
    DispatchF([{?stream_v2(GenId, InnerStream), Topic} || {InnerStream, Topic} <- Events]).

-spec get_streams(shard_id(), emqx_ds:topic_filter(), emqx_ds:time()) ->
    [{integer(), stream()}].
get_streams(Shard, TopicFilter, StartTime) ->
    Gens = generations_since(Shard, StartTime),
    ?tp(get_streams_all_gens, #{gens => Gens}),
    lists:flatmap(
        fun(GenId) ->
            ?tp(get_streams_get_gen, #{gen_id => GenId}),
            case generation_get(Shard, GenId) of
                #{module := Mod, data := GenData} ->
                    Streams = Mod:get_streams(Shard, GenData, TopicFilter, StartTime),
                    [
                        {GenId, ?stream_v2(GenId, InnerStream)}
                     || InnerStream <- Streams
                    ];
                not_found ->
                    %% race condition: generation was dropped before getting its streams?
                    []
            end
        end,
        Gens
    ).

-spec get_delete_streams(shard_id(), emqx_ds:topic_filter(), emqx_ds:time()) ->
    [delete_stream()].
get_delete_streams(Shard, TopicFilter, StartTime) ->
    Gens = generations_since(Shard, StartTime),
    ?tp(get_streams_all_gens, #{gens => Gens}),
    lists:flatmap(
        fun(GenId) ->
            ?tp(get_streams_get_gen, #{gen_id => GenId}),
            case generation_get(Shard, GenId) of
                #{module := Mod, data := GenData} ->
                    Streams = Mod:get_delete_streams(Shard, GenData, TopicFilter, StartTime),
                    [
                        ?delete_stream(GenId, InnerStream)
                     || InnerStream <- Streams
                    ];
                not_found ->
                    %% race condition: generation was dropped before getting its streams?
                    []
            end
        end,
        Gens
    ).

-spec make_iterator(shard_id(), stream(), emqx_ds:topic_filter(), emqx_ds:time()) ->
    emqx_ds:make_iterator_result(iterator()).
make_iterator(
    Shard, ?stream_v2(GenId, Stream), TopicFilter, StartTime
) ->
    case generation_get(Shard, GenId) of
        #{module := Mod, data := GenData} ->
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
        not_found ->
            {error, unrecoverable, generation_not_found}
    end.

-spec make_delete_iterator(shard_id(), delete_stream(), emqx_ds:topic_filter(), emqx_ds:time()) ->
    emqx_ds:make_delete_iterator_result(delete_iterator()).
make_delete_iterator(
    Shard, ?delete_stream(GenId, Stream), TopicFilter, StartTime
) ->
    case generation_get(Shard, GenId) of
        #{module := Mod, data := GenData} ->
            case Mod:make_delete_iterator(Shard, GenData, Stream, TopicFilter, StartTime) of
                {ok, Iter} ->
                    {ok, #{
                        ?tag => ?DELETE_IT,
                        ?generation => GenId,
                        ?enc => Iter
                    }};
                {error, Err} ->
                    {error, unrecoverable, Err}
            end;
        not_found ->
            {error, unrecoverable, generation_not_found}
    end.

-spec update_iterator(shard_id(), iterator(), emqx_ds:message_key()) ->
    emqx_ds:make_iterator_result(iterator()).
update_iterator(
    Shard,
    #{?tag := ?IT, ?generation := GenId, ?enc := OldIter},
    DSKey
) ->
    case generation_get(Shard, GenId) of
        #{module := Mod, data := GenData} ->
            case Mod:update_iterator(Shard, GenData, OldIter, DSKey) of
                {ok, Iter} ->
                    {ok, #{
                        ?tag => ?IT,
                        ?generation => GenId,
                        ?enc => Iter
                    }};
                {error, Err} ->
                    {error, unrecoverable, Err}
            end;
        not_found ->
            ?ERR_GEN_GONE
    end.

-spec generation(iterator()) -> gen_id().
generation(#{?tag := ?IT, ?generation := GenId}) ->
    GenId.

-spec next(shard_id(), iterator(), pos_integer(), emqx_ds:time()) ->
    emqx_ds:next_result(iterator()).
next(Shard, Iter = #{?tag := ?IT, ?generation := GenId, ?enc := GenIter0}, BatchSize, Now) ->
    case generation_get(Shard, GenId) of
        #{module := Mod, data := GenData} ->
            IsCurrent = GenId =:= generation_current(Shard),
            case Mod:next(Shard, GenData, GenIter0, BatchSize, Now, IsCurrent) of
                {ok, GenIter, Batch} ->
                    {ok, Iter#{?enc := GenIter}, Batch};
                {ok, end_of_stream} ->
                    {ok, end_of_stream};
                Error = {error, _, _} ->
                    Error
            end;
        not_found ->
            %% generation was possibly dropped by GC
            ?ERR_GEN_GONE
    end.

%% Internal API for fetching data with multiple iterators in one
%% sweep. This API does not suppose precise batch size.

%%    When doing multi-next, we group iterators by stream:
%% @TODO we need add it to the callback
unpack_iterator(Shard, #{?tag := ?IT, ?generation := GenId, ?enc := Inner}) ->
    case generation_get(Shard, GenId) of
        #{module := Mod, data := GenData} ->
            {InnerStream, TopicFilter, Key, TS} = Mod:unpack_iterator(Shard, GenData, Inner),
            #{
                stream => ?stream_v2(GenId, InnerStream),
                topic_filter => TopicFilter,
                last_seen_key => Key,
                timestamp => TS,
                message_matcher => Mod:message_matcher(Shard, GenData, Inner)
            };
        not_found ->
            %% generation was possibly dropped by GC
            ?ERR_GEN_GONE
    end.

%% @doc This callback is similar in nature to `next'. It is used by
%% the beamformer module, and it allows to fetch data for multiple
%% iterators at once.
scan_stream(
    Shard, ?stream_v2(GenId, Inner), TopicFilter, Now, StartMsg, BatchSize
) ->
    case generation_get(Shard, GenId) of
        #{module := Mod, data := GenData} ->
            IsCurrent = GenId =:= generation_current(Shard),
            Mod:scan_stream(
                Shard, GenData, Inner, TopicFilter, StartMsg, BatchSize, Now, IsCurrent
            );
        not_found ->
            ?ERR_GEN_GONE
    end.

-spec delete_next(
    shard_id(), delete_iterator(), emqx_ds:delete_selector(), pos_integer(), emqx_ds:time()
) ->
    emqx_ds:delete_next_result(delete_iterator()).
delete_next(
    Shard,
    Iter = #{?tag := ?DELETE_IT, ?generation := GenId, ?enc := GenIter0},
    Selector,
    BatchSize,
    Now
) ->
    case generation_get(Shard, GenId) of
        #{module := Mod, data := GenData} ->
            IsCurrent = GenId =:= generation_current(Shard),
            case Mod:delete_next(Shard, GenData, GenIter0, Selector, BatchSize, Now, IsCurrent) of
                {ok, GenIter, NumDeleted, _IteratedOver} ->
                    {ok, Iter#{?enc := GenIter}, NumDeleted};
                EOS = {ok, end_of_stream} ->
                    EOS;
                Error = {error, _} ->
                    Error
            end;
        not_found ->
            %% generation was possibly dropped by GC
            {ok, end_of_stream}
    end.

%% @doc Persist a bunch of key/value pairs in the storage globally, in the "outside
%% of specific generation" sense. Once persisted, values can be read back by calling
%% `fetch_global/2`.
%%
%% Adding or dropping generations won't affect persisted key/value pairs, hence the
%% purpose: keep state that needs to be tied to the shard itself and outlive any
%% generation.
%%
%% This operation is idempotent, previous values associated with existing keys are
%% overwritten. While atomicity of the operation can be specifically requested through
%% `atomic` option, it is atomic by default: either all of pairs are persisted, or none
%% at all. Writes are durable by default, but this is optional, see `batch_store_opts()`
%% for details.
-spec store_global(shard_id(), _KVs :: #{binary() => binary()}, batch_store_opts()) ->
    ok | emqx_ds:error(_).
store_global(ShardId, KVs, Options) ->
    #{db := DB} = get_schema_runtime(ShardId),
    {ok, Batch} = rocksdb:batch(),
    try
        ok = maps:foreach(fun(K, V) -> rocksdb:batch_put(Batch, ?GLOBAL(K), V) end, KVs),
        WriteOpts = [{disable_wal, not maps:get(durable, Options, true)}],
        Result = rocksdb:write_batch(DB, Batch, WriteOpts),
        case Result of
            ok ->
                ok;
            {error, {error, Reason}} ->
                {error, unrecoverable, {rocksdb, Reason}}
        end
    after
        rocksdb:release_batch(Batch)
    end.

%% @doc Retrieve a value for a single key from the storage written there previously by
%% `store_global/3`.
-spec fetch_global(shard_id(), _Key :: binary()) ->
    {ok, _Value :: binary()} | not_found | emqx_ds:error(_).
fetch_global(ShardId, K) ->
    #{db := DB} = get_schema_runtime(ShardId),
    Result = rocksdb:get(DB, ?GLOBAL(K), _ReadOpts = []),
    case Result of
        {ok, _} ->
            Result;
        not_found ->
            Result;
        {error, Reason} ->
            {error, unrecoverable, {rocksdb, Reason}}
    end.

-spec update_config(shard_id(), emqx_ds:time(), emqx_ds:create_db_opts()) ->
    ok | {error, overlaps_existing_generations}.
update_config(ShardId, Since, Options) ->
    Call = #call_update_config{since = Since, options = Options},
    gen_server:call(?REF(ShardId), Call, infinity).

-spec add_generation(shard_id(), emqx_ds:time()) ->
    ok | {error, overlaps_existing_generations}.
add_generation(ShardId, Since) ->
    gen_server:call(?REF(ShardId), #call_add_generation{since = Since}, infinity).

-spec lookup_message(shard_id(), emqx_ds_precondition:matcher()) ->
    emqx_types:message() | not_found | emqx_ds:error(_).
lookup_message(ShardId, Matcher = #message_matcher{timestamp = Time}) ->
    case generation_at(ShardId, Time) of
        {_GenId, #{module := Mod, data := GenData}} ->
            Mod:lookup_message(ShardId, GenData, Matcher);
        not_found ->
            not_found
    end.

-spec list_generations_with_lifetimes(shard_id()) ->
    #{gen_id() => gen_info()}.
list_generations_with_lifetimes(ShardId) ->
    gen_server:call(?REF(ShardId), #call_list_generations_with_lifetimes{}, infinity).

-spec drop_generation(shard_id(), gen_id()) -> ok | {error, _}.
drop_generation(ShardId, GenId) ->
    gen_server:call(?REF(ShardId), #call_drop_generation{gen_id = GenId}, infinity).

-spec find_generation(shard_id(), current | _At :: emqx_ds:time()) ->
    {gen_id(), gen_info()} | not_found.
find_generation(ShardId, current) ->
    GenId = generation_current(ShardId),
    GenData = #{} = generation_get(ShardId, GenId),
    {GenId, GenData};
find_generation(ShardId, AtTime) ->
    generation_at(ShardId, AtTime).

-spec shard_info(shard_id(), status) -> running | down.
shard_info(ShardId, status) ->
    try get_schema_runtime(ShardId) of
        #{} -> running
    catch
        error:badarg -> down
    end.

-spec flush(shard_id()) -> ok | {error, _}.
flush(ShardId) ->
    gen_server:call(?REF(ShardId), #call_flush{}, infinity).

-spec take_snapshot(shard_id()) -> {ok, emqx_ds_storage_snapshot:reader()} | {error, _Reason}.
take_snapshot(ShardId) ->
    case gen_server:call(?REF(ShardId), #call_take_snapshot{}, infinity) of
        {ok, Dir} ->
            emqx_ds_storage_snapshot:new_reader(Dir);
        Error ->
            Error
    end.

-spec accept_snapshot(shard_id()) -> {ok, emqx_ds_storage_snapshot:writer()} | {error, _Reason}.
accept_snapshot(ShardId) ->
    ok = drop_shard(ShardId),
    handle_accept_snapshot(ShardId).

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
    db_opts :: emqx_ds:db_opts(),
    cf_refs :: cf_refs(),
    cf_need_flush :: gen_id(),
    schema :: shard_schema(),
    shard :: shard()
}).

-type server_state() :: #s{}.

-define(DEFAULT_CF, "default").
-define(DEFAULT_CF_OPTS, []).

init({ShardId, Options}) ->
    process_flag(trap_exit, true),
    ?tp(info, ds_storage_init, #{shard => ShardId}),
    logger:set_process_metadata(#{shard_id => ShardId, domain => [ds, storage_layer, shard]}),
    erase_schema_runtime(ShardId),
    clear_all_checkpoints(ShardId),
    {ok, DB, CFRefs0} = rocksdb_open(ShardId, Options),
    {Schema, CFRefs} =
        case get_schema_persistent(DB) of
            not_found ->
                create_new_shard_schema(ShardId, DB, CFRefs0, Options);
            Scm ->
                {Scm, CFRefs0}
        end,
    Shard = open_shard(ShardId, DB, CFRefs, Schema),
    CurrentGenId = maps:get(current_generation, Schema),
    S = #s{
        shard_id = ShardId,
        db = DB,
        db_opts = filter_layout_db_opts(Options),
        cf_refs = CFRefs,
        cf_need_flush = CurrentGenId,
        schema = Schema,
        shard = Shard
    },
    commit_metadata(S),
    ?tp(debug, ds_storage_init_state, #{shard => ShardId, s => S}),
    {ok, S, {continue, clean_orphans}}.

handle_continue(
    clean_orphans,
    S = #s{shard_id = ShardId, db = DB, cf_refs = CFRefs, schema = Schema}
) ->
    %% Add / drop generation are not transactional.
    %% This means that the storage may contain "orphaned" column families, i.e.
    %% column families that do not belong to a live generation. We need to clean
    %% them, because an attempt to create existing column family is an error,
    %% therefore `add_generation/2` is not idempotent. Cleaning seems to be safe:
    %% either it's unfinished `handle_add_generation/2` meaning CFs are empty, or
    %% it's unfinished `handle_drop_generation/2` meaning CFs was meant to be
    %% dropped anyway.
    CFNames = maps:fold(
        fun
            (?GEN_KEY(_), #{cf_names := GenCFNames}, Acc) ->
                GenCFNames ++ Acc;
            (_Prop, _, Acc) ->
                Acc
        end,
        [],
        Schema
    ),
    OrphanedCFRefs = lists:foldl(fun proplists:delete/2, CFRefs, CFNames),
    case OrphanedCFRefs of
        [] ->
            {noreply, S};
        [_ | _] ->
            lists:foreach(
                fun({CFName, CFHandle}) ->
                    Result = rocksdb:drop_column_family(DB, CFHandle),
                    ?tp(
                        warning,
                        ds_storage_layer_dropped_orphaned_column_family,
                        #{
                            shard => ShardId,
                            orphan => CFName,
                            result => Result,
                            s => format_state(S)
                        }
                    )
                end,
                OrphanedCFRefs
            ),
            {noreply, S#s{cf_refs = CFRefs -- OrphanedCFRefs}}
    end.

format_status(Status) ->
    maps:map(
        fun
            (state, State) ->
                format_state(State);
            (_, Val) ->
                Val
        end,
        Status
    ).

handle_call(#call_update_config{since = Since, options = Options}, _From, S0) ->
    case handle_update_config(S0, Since, Options) of
        S = #s{} ->
            commit_metadata(S),
            {reply, ok, S};
        Error = {error, _} ->
            {reply, Error, S0}
    end;
handle_call(#call_add_generation{since = Since}, _From, S0) ->
    case handle_add_generation(S0, Since) of
        S = #s{} ->
            commit_metadata(S),
            {reply, ok, S};
        Error = {error, _} ->
            {reply, Error, S0}
    end;
handle_call(#call_list_generations_with_lifetimes{}, _From, S) ->
    Generations = handle_list_generations_with_lifetimes(S),
    {reply, Generations, S};
handle_call(#call_drop_generation{gen_id = GenId}, _From, S0) ->
    {Reply, S} = handle_drop_generation(S0, GenId),
    {reply, Reply, S};
handle_call(#call_flush{}, _From, S0) ->
    {Reply, S} = handle_flush(S0),
    {reply, Reply, S};
handle_call(#call_take_snapshot{}, _From, S) ->
    Snapshot = handle_take_snapshot(S),
    {reply, Snapshot, S};
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

-spec clear_all_checkpoints(shard_id()) -> ok.
clear_all_checkpoints(ShardId) ->
    CheckpointBaseDir = checkpoints_dir(ShardId),
    ok = filelib:ensure_path(CheckpointBaseDir),
    {ok, AllFiles} = file:list_dir(CheckpointBaseDir),
    CheckpointDirs = [Dir || Dir <- AllFiles, filelib:is_dir(Dir)],
    lists:foreach(
        fun(Dir) ->
            logger:debug(#{
                msg => "ds_storage_deleting_previous_checkpoint",
                dir => Dir
            }),
            ok = file:del_dir_r(Dir)
        end,
        CheckpointDirs
    ).

-spec open_shard(shard_id(), rocksdb:db_handle(), cf_refs(), shard_schema()) ->
    shard().
open_shard(ShardId, DB, CFRefs, ShardSchema) ->
    %% Transform generation schemas to generation runtime data:
    Shard = maps:map(
        fun
            (?GEN_KEY(GenId), GenSchema) ->
                open_generation(ShardId, DB, CFRefs, GenId, GenSchema);
            (_K, Val) ->
                Val
        end,
        ShardSchema
    ),
    Shard#{db => DB}.

-spec handle_add_generation(server_state(), emqx_ds:time()) ->
    server_state() | {error, overlaps_existing_generations}.
handle_add_generation(
    S0 = #s{
        shard_id = ShardId,
        db = DB,
        db_opts = DBOpts,
        schema = Schema0,
        shard = Shard0,
        cf_refs = CFRefs0
    },
    Since
) ->
    Schema1 = update_last_until(Schema0, Since),
    Shard1 = update_last_until(Shard0, Since),
    case Schema1 of
        _Updated = #{} ->
            {GenId, Schema, NewCFRefs} =
                new_generation(ShardId, DB, Schema1, Shard0, Since, DBOpts),
            CFRefs = NewCFRefs ++ CFRefs0,
            Key = ?GEN_KEY(GenId),
            Generation = open_generation(ShardId, DB, CFRefs, GenId, maps:get(Key, Schema)),
            Shard = Shard1#{current_generation := GenId, Key => Generation},
            S0#s{
                cf_refs = CFRefs,
                schema = Schema,
                shard = Shard
            };
        {error, exists} ->
            S0;
        {error, Reason} ->
            {error, Reason}
    end.

-spec handle_update_config(server_state(), emqx_ds:time(), emqx_ds:create_db_opts()) ->
    server_state() | {error, overlaps_existing_generations}.
handle_update_config(S0 = #s{schema = Schema}, Since, Options) ->
    Prototype = maps:get(storage, Options),
    S = S0#s{schema = Schema#{prototype := Prototype}},
    handle_add_generation(S, Since).

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
        schema = #{?GEN_KEY(GenId) := GenSchema} = Schema0,
        shard = #{?GEN_KEY(GenId) := #{data := RuntimeData}} = Shard0,
        cf_refs = CFRefs0
    } = S0,
    %% 1. Commit the metadata first, so other functions are less
    %% likely to see stale data, and replicas don't end up
    %% inconsistent state, where generation's column families are
    %% absent, but its metadata is still present.
    %%
    %% Note: in theory, this operation may be interrupted in the
    %% middle. This will leave column families hanging.
    Shard = maps:remove(?GEN_KEY(GenId), Shard0),
    Schema = maps:remove(?GEN_KEY(GenId), Schema0),
    S1 = S0#s{
        shard = Shard,
        schema = Schema
    },
    commit_metadata(S1),
    %% 2. Now, actually drop the data from RocksDB:
    #{module := Mod, cf_names := GenCFNames} = GenSchema,
    GenCFRefs = [cf_ref(Name, CFRefs0) || Name <- GenCFNames],
    try
        Mod:drop(ShardId, DB, GenId, GenCFRefs, RuntimeData)
    catch
        EC:Err:Stack ->
            ?tp(
                error,
                ds_storage_layer_failed_to_drop_generation,
                #{
                    shard => ShardId,
                    EC => Err,
                    stacktrace => Stack,
                    generation => GenId,
                    s => format_state(S0)
                }
            )
    end,
    CFRefs = CFRefs0 -- GenCFRefs,
    S = S1#s{cf_refs = CFRefs},
    {ok, S}.

-spec open_generation(shard_id(), rocksdb:db_handle(), cf_refs(), gen_id(), generation_schema()) ->
    generation().
open_generation(ShardId, DB, CFRefs, GenId, GenSchema) ->
    ?tp(debug, ds_open_generation, #{gen_id => GenId, schema => GenSchema}),
    #{module := Mod, data := Schema} = GenSchema,
    RuntimeData = Mod:open(ShardId, DB, GenId, CFRefs, Schema),
    GenSchema#{data => RuntimeData}.

-spec create_new_shard_schema(shard_id(), rocksdb:db_handle(), cf_refs(), emqx_ds:create_db_opts()) ->
    {shard_schema(), cf_refs()}.
create_new_shard_schema(ShardId, DB, CFRefs, Options = #{storage := Prototype}) ->
    ?tp(notice, ds_create_new_shard_schema, #{shard => ShardId, prototype => Prototype}),
    %% TODO: read prototype from options/config
    Schema0 = #{
        current_generation => 0,
        prototype => Prototype
    },
    DBOpts = filter_layout_db_opts(Options),
    {_NewGenId, Schema, NewCFRefs} =
        new_generation(ShardId, DB, Schema0, undefined, _Since = 0, DBOpts),
    {Schema, NewCFRefs ++ CFRefs}.

-spec new_generation(
    shard_id(),
    rocksdb:db_handle(),
    shard_schema(),
    shard() | undefined,
    emqx_ds:time(),
    emqx_ds:db_opts()
) ->
    {gen_id(), shard_schema(), cf_refs()}.
new_generation(ShardId, DB, Schema0, Shard0, Since, DBOpts) ->
    #{current_generation := PrevGenId, prototype := {Mod, ModConf}} = Schema0,
    case Shard0 of
        #{?GEN_KEY(PrevGenId) := #{module := Mod} = PrevGen} ->
            %% When the new generation's module is the same as the last one, we might want
            %% to perform actions like inheriting some of the previous (meta)data.
            PrevRuntimeData = maps:get(data, PrevGen);
        _ ->
            PrevRuntimeData = undefined
    end,
    %% Provide a small subset of DB options to the storage layout module.
    GenId = next_generation_id(PrevGenId),
    {GenData, NewCFRefs} = Mod:create(ShardId, DB, GenId, ModConf, PrevRuntimeData, DBOpts),
    GenSchema = #{
        module => Mod,
        data => GenData,
        cf_names => cf_names(NewCFRefs),
        created_at => erlang:system_time(millisecond),
        since => Since,
        until => undefined
    },
    Schema = Schema0#{
        current_generation => GenId,
        ?GEN_KEY(GenId) => GenSchema
    },
    {GenId, Schema, NewCFRefs}.

-spec next_generation_id(gen_id()) -> gen_id().
next_generation_id(GenId) ->
    GenId + 1.

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
        %% NOTE
        %% With WAL-less writes, it's important to have CFs flushed atomically.
        %% For example, bitfield-lts backend needs data + trie CFs to be consistent.
        {atomic_flush, true},
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

-spec base_dir() -> file:filename().
base_dir() ->
    application:get_env(?APP, db_data_dir, emqx:data_dir()).

-spec db_dir(shard_id()) -> file:filename().
db_dir({DB, ShardId}) ->
    filename:join([base_dir(), DB, binary_to_list(ShardId)]).

-spec checkpoints_dir(shard_id()) -> file:filename().
checkpoints_dir({DB, ShardId}) ->
    filename:join([base_dir(), DB, checkpoints, binary_to_list(ShardId)]).

-spec checkpoint_dir(shard_id(), _Name :: file:name()) -> file:filename().
checkpoint_dir(ShardId, Name) ->
    filename:join([checkpoints_dir(ShardId), Name]).

-spec update_last_until(Schema, emqx_ds:time()) ->
    Schema | {error, exists | overlaps_existing_generations}
when
    Schema :: shard_schema() | shard().
update_last_until(Schema = #{current_generation := GenId}, Until) ->
    case maps:get(?GEN_KEY(GenId), Schema) of
        GenData = #{since := CurrentSince} when CurrentSince < Until ->
            Schema#{?GEN_KEY(GenId) := GenData#{until := Until}};
        #{since := Until} ->
            {error, exists};
        #{since := CurrentSince} when CurrentSince > Until ->
            {error, overlaps_existing_generations}
    end.

handle_flush(S = #s{db = DB, cf_refs = CFRefs, cf_need_flush = NeedFlushGenId, shard = Shard}) ->
    %% NOTE
    %% There could have been few generations added since the last time `flush/1` was
    %% called. Strictly speaking, we don't need to flush them all at once as part of
    %% a single atomic flush, but the error handling is a bit easier this way.
    CurrentGenId = maps:get(current_generation, Shard),
    GenIds = lists:seq(NeedFlushGenId, CurrentGenId),
    CFHandles = lists:flatmap(
        fun(GenId) ->
            case Shard of
                #{?GEN_KEY(GenId) := #{cf_names := CFNames}} ->
                    [cf_handle(N, CFRefs) || N <- CFNames];
                #{} ->
                    %% Generation was probably dropped.
                    []
            end
        end,
        GenIds
    ),
    case rocksdb:flush(DB, CFHandles, [{wait, true}]) of
        ok ->
            %% Current generation will always need a flush.
            ?tp(ds_storage_flush_complete, #{gens => GenIds, cfs => CFHandles}),
            {ok, S#s{cf_need_flush = CurrentGenId}};
        {error, _} = Error ->
            {Error, S}
    end.

handle_take_snapshot(#s{db = DB, shard_id = ShardId}) ->
    Name = integer_to_list(erlang:system_time(millisecond)),
    Dir = checkpoint_dir(ShardId, Name),
    _ = filelib:ensure_dir(Dir),
    case rocksdb:checkpoint(DB, Dir) of
        ok ->
            {ok, Dir};
        {error, _} = Error ->
            Error
    end.

handle_accept_snapshot(ShardId) ->
    Dir = db_dir(ShardId),
    emqx_ds_storage_snapshot:new_writer(Dir).

-spec handle_event(shard_id(), emqx_ds:time(), Event) -> [Event].
handle_event(Shard, Time, ?storage_event(GenId, Event)) ->
    case generation_get(Shard, GenId) of
        not_found ->
            [];
        #{module := Mod, data := GenData} ->
            case erlang:function_exported(Mod, handle_event, 4) of
                true ->
                    NewEvents = Mod:handle_event(Shard, GenData, Time, Event),
                    [?mk_storage_event(GenId, E) || E <- NewEvents];
                false ->
                    []
            end
    end;
handle_event(Shard, Time, Event) ->
    GenId = generation_current(Shard),
    handle_event(Shard, Time, ?mk_storage_event(GenId, Event)).

filter_layout_db_opts(Options) ->
    maps:with(?STORAGE_LAYOUT_DB_OPTS, Options).

%%--------------------------------------------------------------------------------

-spec cf_names(cf_refs()) -> [string()].
cf_names(CFRefs) ->
    {CFNames, _CFHandles} = lists:unzip(CFRefs),
    CFNames.

-spec cf_ref(_Name :: string(), cf_refs()) -> cf_ref().
cf_ref(Name, CFRefs) ->
    lists:keyfind(Name, 1, CFRefs).

-spec cf_handle(_Name :: string(), cf_refs()) -> rocksdb:cf_handle().
cf_handle(Name, CFRefs) ->
    element(2, cf_ref(Name, CFRefs)).

%%--------------------------------------------------------------------------------
%% Schema access
%%--------------------------------------------------------------------------------

-spec generation_current(shard_id()) -> gen_id().
generation_current(Shard) ->
    #{current_generation := Current} = get_schema_runtime(Shard),
    Current.

%% TODO: remove me
-spec generation_at(shard_id(), emqx_ds:time()) -> {gen_id(), generation()} | not_found.
generation_at(Shard, Time) ->
    Schema = #{current_generation := Current} = get_schema_runtime(Shard),
    generation_at(Time, Current, Schema).

generation_at(Time, GenId, Schema) ->
    case Schema of
        #{?GEN_KEY(GenId) := Gen} ->
            case Gen of
                #{since := Since} when Time < Since andalso GenId > 0 ->
                    generation_at(Time, GenId - 1, Schema);
                _ ->
                    {GenId, Gen}
            end;
        _ ->
            not_found
    end.

-spec generation_get(shard_id(), gen_id()) -> generation() | not_found.
generation_get(Shard, GenId) ->
    case get_schema_runtime(Shard) of
        #{?GEN_KEY(GenId) := GenData} ->
            GenData;
        #{} ->
            not_found
    end.

-spec generations_since(shard_id(), emqx_ds:time()) -> [gen_id()].
generations_since(Shard, Since) ->
    Schema = #{current_generation := Current} = get_schema_runtime(Shard),
    list_generations_since(Schema, Current, Since).

list_generations_since(Schema, GenId, Since) ->
    case Schema of
        #{?GEN_KEY(GenId) := #{until := Until}} when Until > Since ->
            [GenId | list_generations_since(Schema, GenId - 1, Since)];
        #{} ->
            %% No more live generations.
            []
    end.

format_state(#s{shard_id = ShardId, db = DB, cf_refs = CFRefs, schema = Schema, shard = Shard}) ->
    #{
        id => ShardId,
        db => DB,
        cf_refs => CFRefs,
        schema => Schema,
        shard =>
            maps:map(
                fun
                    (?GEN_KEY(_), _Schema) ->
                        '...';
                    (_K, Val) ->
                        Val
                end,
                Shard
            )
    }.

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

-define(ROCKSDB_SCHEMA_KEY(V), <<"schema_", V>>).

-define(ROCKSDB_SCHEMA_KEY, ?ROCKSDB_SCHEMA_KEY("v2")).
-define(ROCKSDB_SCHEMA_KEYS, [
    ?ROCKSDB_SCHEMA_KEY,
    ?ROCKSDB_SCHEMA_KEY("v1")
]).

-spec get_schema_persistent(rocksdb:db_handle()) -> shard_schema() | not_found.
get_schema_persistent(DB) ->
    get_schema_persistent(DB, ?ROCKSDB_SCHEMA_KEYS).

get_schema_persistent(DB, [Key | Rest]) ->
    case rocksdb:get(DB, Key, []) of
        {ok, Blob} ->
            deserialize_schema(Key, Blob);
        not_found ->
            get_schema_persistent(DB, Rest)
    end;
get_schema_persistent(_DB, []) ->
    not_found.

-spec put_schema_persistent(rocksdb:db_handle(), shard_schema()) -> ok.
put_schema_persistent(DB, Schema) ->
    Blob = term_to_binary(Schema),
    rocksdb:put(DB, ?ROCKSDB_SCHEMA_KEY, Blob, []).

-spec deserialize_schema(_SchemaVsn :: binary(), binary()) -> shard_schema().
deserialize_schema(SchemaVsn, Blob) ->
    %% Sanity check:
    Schema = #{current_generation := _, prototype := _} = binary_to_term(Blob),
    decode_schema(SchemaVsn, Schema).

decode_schema(?ROCKSDB_SCHEMA_KEY, Schema) ->
    Schema;
decode_schema(?ROCKSDB_SCHEMA_KEY("v1"), Schema) ->
    maps:map(fun decode_schema_v1/2, Schema).

decode_schema_v1(?GEN_KEY(_), Generation = #{}) ->
    decode_generation_schema_v1(Generation);
decode_schema_v1(_, V) ->
    V.

decode_generation_schema_v1(SchemaV1 = #{cf_refs := CFRefs}) ->
    %% Drop potentially dead CF references from the time generation was created.
    Schema = maps:remove(cf_refs, SchemaV1),
    Schema#{cf_names => cf_names(CFRefs)};
decode_generation_schema_v1(Schema = #{}) ->
    Schema.

%%--------------------------------------------------------------------------------

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

decode_schema_v1_test() ->
    SchemaV1 = #{
        current_generation => 42,
        prototype => {emqx_ds_storage_reference, #{}},
        ?GEN_KEY(41) => #{
            module => emqx_ds_storage_reference,
            data => {schema},
            cf_refs => [{"emqx_ds_storage_reference41", erlang:make_ref()}],
            created_at => 12345,
            since => 0,
            until => 123456
        },
        ?GEN_KEY(42) => #{
            module => emqx_ds_storage_reference,
            data => {schema},
            cf_refs => [{"emqx_ds_storage_reference42", erlang:make_ref()}],
            created_at => 54321,
            since => 123456,
            until => undefined
        }
    },
    ?assertEqual(
        #{
            current_generation => 42,
            prototype => {emqx_ds_storage_reference, #{}},
            ?GEN_KEY(41) => #{
                module => emqx_ds_storage_reference,
                data => {schema},
                cf_names => ["emqx_ds_storage_reference41"],
                created_at => 12345,
                since => 0,
                until => 123456
            },
            ?GEN_KEY(42) => #{
                module => emqx_ds_storage_reference,
                data => {schema},
                cf_names => ["emqx_ds_storage_reference42"],
                created_at => 54321,
                since => 123456,
                until => undefined
            }
        },
        deserialize_schema(?ROCKSDB_SCHEMA_KEY("v1"), term_to_binary(SchemaV1))
    ).

-endif.

-undef(ROCKSDB_SCHEMA_KEY).
