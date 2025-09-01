%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_builtin_local).

%% API:
-export([]).

-behaviour(emqx_ds).
-export([
    default_db_opts/0,
    verify_db_opts/2,
    open_db/4,
    update_db_config/3,
    close_db/1,
    drop_db/1,

    shard_of/2,
    list_shards/1,

    add_generation/1,
    drop_slab/2,
    list_slabs/1,

    dirty_append/2,
    get_streams/4,
    make_iterator/4,
    next/3,

    subscribe/3,
    unsubscribe/2,
    suback/3,
    subscription_info/2,

    stream_to_binary/2,
    binary_to_stream/2,
    iterator_to_binary/2,
    binary_to_iterator/2,

    new_tx/2,
    commit_tx/3,
    tx_commit_outcome/1
]).

-behaviour(emqx_dsch).
-export([
    db_info/1,
    handle_db_config_change/2,
    handle_schema_event/3
]).

-behaviour(emqx_ds_beamformer).
-export([
    beamformer_config/1,
    unpack_iterator/2,
    high_watermark/2,
    scan_stream/5,
    fast_forward/4,
    update_iterator/3,
    message_match_context/4,
    iterator_match_context/2
]).

-behaviour(emqx_ds_optimistic_tx).
-export([
    otx_get_tx_serial/2,
    otx_get_leader/2,
    otx_get_latest_generation/2,
    otx_become_leader/2,
    otx_prepare_tx/5,
    otx_commit_tx_batch/5,
    otx_add_generation/3,
    otx_lookup_ttv/4,
    otx_get_runtime_config/1
]).

%% Internal exports:
-export([do_next/3]).

-ifdef(TEST).
-export([test_applications/1, test_db_config/1]).
-endif.

-export_type([db_opts/0, db_schema/0, db_runtime_config/0, iterator/0]).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds_builtin_tx.hrl").
-include("../../emqx_durable_storage/gen_src/DSBuiltinMetadata.hrl").

-elvis([{elvis_style, atom_naming_convention, disable}]).

%%================================================================================
%% Type declarations
%%================================================================================

-type db_opts() ::
    #{
        backend := builtin_local,
        payload_type := emqx_ds_payload_transform:type(),
        n_shards := pos_integer(),
        storage := emqx_ds_storage_layer:prototype(),
        %% Beamformer config:
        subscriptions => emqx_ds_beamformer:opts(),
        %% Optimistic transaction:
        transactions => emqx_ds_optimistic_tx:runtime_config(),
        rocksdb => emqx_ds_storage_layer:rocksdb_options()
    }.

-type db_schema() :: #{
    backend := builtin_local,
    n_shards := pos_integer(),
    storage := emqx_ds_storage_layer:prototype(),
    payload_type := emqx_ds_payload_transform:type()
}.

-type db_runtime_config() :: #{
    %% Beamformer
    subscriptions := emqx_ds_beamformer:opts(),
    %% Optimistic transaction:
    transactions := emqx_ds_optimistic_tx:runtime_config(),
    %% RocksDB options:
    rocksdb := emqx_ds_storage_layer:rocksdb_options()
}.

-type stream() :: emqx_ds_storage_layer_ttv:stream().

-type iterator() :: emqx_ds_storage_layer_ttv:iterator().

-type tx_context() :: emqx_ds_optimistic_tx:ctx().

%%================================================================================
%% API functions
%%================================================================================

%%================================================================================
%% behavior callbacks
%%================================================================================

-spec default_db_opts() -> map().
default_db_opts() ->
    #{
        backend => builtin_local,
        transactions => #{
            flush_interval => 1_000,
            idle_flush_interval => 1,
            max_items => 1000,
            conflict_window => 5_000
        },
        subscriptions => #{
            n_workers_per_shard => 10,
            batch_size => 1000,
            housekeeping_interval => 1000
        },
        rocksdb => #{}
    }.

-spec verify_db_opts(emqx_ds:db(), db_opts()) ->
    {ok, db_schema(), db_runtime_config()} | emqx_ds:error(_).
verify_db_opts(DB, Opts) ->
    case {emqx_dsch:get_db_schema(DB), emqx_dsch:get_db_runtime(DB)} of
        {OldSchema, RTC} when is_map(OldSchema) ->
            maybe
                case RTC of
                    #{runtime := OldConf} -> ok;
                    undefined -> OldConf = #{}
                end,
                {ok, Merged} ?= merge_config(OldSchema, OldConf, Opts),
                verify_db_opts(Merged)
            end;
        {undefined, undefined} ->
            verify_db_opts(Opts)
    end.

-spec open_db(emqx_ds:db(), boolean(), emqx_dsch:db_schema(), emqx_dsch:db_runtime_config()) ->
    ok | {error, _}.
open_db(DB, Create, Schema, RuntimeConf) ->
    case emqx_ds_builtin_local_sup:start_db(DB, Create, Schema, RuntimeConf) of
        {ok, _} ->
            emqx_ds:set_db_ready(DB, true),
            ok;
        {error, {already_started, _}} ->
            emqx_ds:set_db_ready(DB, true),
            ok;
        {error, Err} ->
            {error, Err}
    end.

-spec close_db(emqx_ds:db()) -> ok.
close_db(DB) ->
    emqx_ds_builtin_local_sup:stop_db(DB).

-spec add_generation(emqx_ds:db()) -> ok | {error, _}.
add_generation(DB) ->
    Shards = emqx_ds_builtin_local_meta:shards(DB),
    lists:foreach(
        fun(Shard) ->
            ok = emqx_ds_optimistic_tx:add_generation(DB, Shard)
        end,
        Shards
    ).

otx_add_generation(DB, Shard, Since) ->
    DBShard = {DB, Shard},
    ok = emqx_ds_storage_layer:add_generation(DBShard, Since),
    emqx_ds_beamformer:generation_event(DBShard).

-spec update_db_config(emqx_ds:db(), emqx_dsch:db_schema(), emqx_dsch:db_runtime_config()) ->
    ok | {error, _}.
update_db_config(DB, NewSchema, NewRTConf) ->
    maybe
        ok ?= emqx_dsch:update_db_schema(DB, NewSchema),
        ok ?= emqx_dsch:update_db_config(DB, NewRTConf)
    end.

-spec list_slabs(emqx_ds:db()) ->
    #{emqx_ds:slab() => emqx_ds:slab_info()}.
list_slabs(DB) ->
    lists:foldl(
        fun(Shard, Acc) ->
            maps:fold(
                fun(GenId, Data, Acc1) ->
                    Acc1#{{Shard, GenId} => Data}
                end,
                Acc,
                emqx_ds_storage_layer:list_slabs({DB, Shard})
            )
        end,
        #{},
        emqx_ds_builtin_local_meta:shards(DB)
    ).

-spec drop_slab(emqx_ds:db(), emqx_ds:slab()) -> ok | {error, _}.
drop_slab(DB, {Shard, GenId}) ->
    emqx_ds_storage_layer:drop_slab({DB, Shard}, GenId).

-spec drop_db(emqx_ds:db()) -> ok | {error, _}.
drop_db(DB) ->
    maybe
        ok ?= close_db(DB),
        lists:foreach(
            fun(Shard) ->
                emqx_ds_storage_layer:drop_shard({DB, Shard})
            end,
            emqx_ds_builtin_local_meta:shards(DB)
        ),
        emqx_dsch:drop_db_schema(DB)
    end.

-spec dirty_append(emqx_ds:dirty_append_opts(), emqx_ds:dirty_append_data()) ->
    reference() | noreply.
dirty_append(#{db := _, shard := _} = Opts, Data) ->
    emqx_ds_optimistic_tx:dirty_append(Opts, Data).

-spec new_tx(emqx_ds:db(), emqx_ds:transaction_opts()) ->
    {ok, tx_context()} | emqx_ds:error(_).
new_tx(DB, Options = #{shard := ShardOpt, generation := Generation}) ->
    case ShardOpt of
        {auto, Owner} ->
            Shard = shard_of(DB, Owner);
        Shard ->
            ok
    end,
    emqx_ds_optimistic_tx:new_kv_tx_ctx(?MODULE, DB, Shard, Generation, Options).

-spec commit_tx(emqx_ds:db(), tx_context(), emqx_ds:tx_ops()) -> reference().
commit_tx(DB, Ctx, Ops) ->
    emqx_ds_optimistic_tx:commit_kv_tx(DB, Ctx, Ops).

tx_commit_outcome(Reply) ->
    emqx_ds_optimistic_tx:tx_commit_outcome(Reply).

list_shards(DB) ->
    emqx_ds_builtin_local_meta:shards(DB).

shard_of(DB, Key) ->
    N = emqx_ds_builtin_local_meta:n_shards(DB),
    Hash = erlang:phash2(Key, N),
    integer_to_binary(Hash).

-spec get_streams(emqx_ds:db(), emqx_ds:topic_filter(), emqx_ds:time(), emqx_ds:get_streams_opts()) ->
    emqx_ds:get_streams_result().
get_streams(DB, TopicFilter, StartTime, Opts) ->
    Shards =
        case Opts of
            #{shard := ReqShard} ->
                [ReqShard];
            _ ->
                emqx_ds_builtin_local_meta:shards(DB)
        end,
    MinGeneration = maps:get(generation_min, Opts, 0),
    Results = lists:flatmap(
        fun(Shard) ->
            Streams = emqx_ds_storage_layer_ttv:get_streams(
                {DB, Shard}, TopicFilter, StartTime, MinGeneration
            ),
            [
                {{Shard, Stream#'Stream'.generation}, Stream}
             || Stream <- Streams
            ]
        end,
        Shards
    ),
    {Results, []}.

-spec make_iterator(
    emqx_ds:db(), emqx_ds:ds_specific_stream(), emqx_ds:topic_filter(), emqx_ds:time()
) ->
    emqx_ds:make_iterator_result(iterator()).
make_iterator(DB, Stream = #'Stream'{}, TopicFilter, StartTime) ->
    emqx_ds_storage_layer_ttv:make_iterator(DB, Stream, TopicFilter, StartTime).

-spec update_iterator(_Shard, emqx_ds:ds_specific_iterator(), emqx_ds:message_key()) ->
    emqx_ds:make_iterator_result(iterator()).
update_iterator(DBShard, Iter0 = #'Iterator'{}, Key) ->
    emqx_ds_storage_layer_ttv:update_iterator(DBShard, Iter0, Key).

-spec next(emqx_ds:db(), iterator(), pos_integer()) -> emqx_ds:next_result(iterator()).
next(DB, Iter, N) ->
    {ok, _, Ref} = emqx_ds_lib:with_worker(?MODULE, do_next, [DB, Iter, N]),
    receive
        {Ref, Result} ->
            Result
    end.

-spec subscribe(emqx_ds:db(), iterator(), emqx_ds:sub_opts()) ->
    {ok, emqx_ds:subscription_handle(), reference()}.
subscribe(DB, It = #'Iterator'{shard = Shard}, SubOpts) ->
    Server = emqx_ds_beamformer:where({DB, Shard}),
    MRef = monitor(process, Server),
    case emqx_ds_beamformer:subscribe(Server, self(), MRef, It, SubOpts) of
        {ok, MRef} ->
            {ok, {Shard, MRef}, MRef};
        Err = {error, _, _} ->
            Err
    end.

-spec subscription_info(emqx_ds:db(), emqx_ds:subscription_handle()) ->
    emqx_ds:sub_info() | undefined.
subscription_info(DB, {Shard, SubRef}) ->
    emqx_ds_beamformer:subscription_info({DB, Shard}, SubRef).

-spec unsubscribe(emqx_ds:db(), emqx_ds:subscription_handle()) -> boolean().
unsubscribe(DB, {Shard, SubId}) ->
    emqx_ds_beamformer:unsubscribe({DB, Shard}, SubId).

-spec suback(emqx_ds:db(), emqx_ds:subscription_handle(), emqx_ds:sub_seqno()) ->
    ok | {error, subscription_not_found}.
suback(DB, {Shard, SubRef}, SeqNo) ->
    emqx_ds_beamformer:suback({DB, Shard}, SubRef, SeqNo).

-spec beamformer_config(emqx_ds:db()) -> emqx_ds_beamformer:opts().
beamformer_config(DB) ->
    #{runtime := #{subscriptions := Conf}} = emqx_dsch:get_db_runtime(DB),
    Conf.

unpack_iterator(DBShard, Iterator = #'Iterator'{}) ->
    emqx_ds_storage_layer_ttv:unpack_iterator(DBShard, Iterator).

high_watermark(DBShard, Stream = #'Stream'{}) ->
    Now = current_timestamp(DBShard),
    emqx_ds_storage_layer_ttv:high_watermark(DBShard, Stream, Now).

fast_forward(DBShard, It = #'Iterator'{}, Key, BatchSize) ->
    Now = current_timestamp(DBShard),
    emqx_ds_storage_layer_ttv:fast_forward(DBShard, It, Key, Now, BatchSize).

message_match_context(DBShard, Stream = #'Stream'{}, MsgKey, TTV) ->
    emqx_ds_storage_layer_ttv:message_match_context(DBShard, Stream, MsgKey, TTV).

iterator_match_context(DBShard, Iterator = #'Iterator'{}) ->
    emqx_ds_storage_layer_ttv:iterator_match_context(DBShard, Iterator).

scan_stream(DBShard, Stream = #'Stream'{}, TopicFilter, StartMsg, BatchSize) ->
    {DB, _} = DBShard,
    Now = current_timestamp(DBShard),
    T0 = erlang:monotonic_time(microsecond),
    Result = emqx_ds_storage_layer_ttv:scan_stream(
        DBShard, Stream, TopicFilter, Now, StartMsg, BatchSize
    ),
    T1 = erlang:monotonic_time(microsecond),
    emqx_ds_builtin_metrics:observe_next_time(DB, T1 - T0),
    Result.

%%================================================================================
%% Internal exports
%%================================================================================

current_timestamp(ShardId) ->
    emqx_ds_builtin_local_meta:current_timestamp(ShardId).

-spec do_next(emqx_ds:db(), iterator(), emqx_ds:next_limit()) -> emqx_ds:next_result(iterator()).
do_next(DB, Iter0 = #'Iterator'{shard = Shard}, NextLimit) ->
    T0 = erlang:monotonic_time(microsecond),
    {BatchSize, TimeLimit} = batch_size_and_time_limit(DB, Shard, NextLimit),
    Result = emqx_ds_storage_layer_ttv:next(DB, Iter0, BatchSize, TimeLimit),
    T1 = erlang:monotonic_time(microsecond),
    emqx_ds_builtin_metrics:observe_next_time(DB, T1 - T0),
    Result.

%%================================================================================
%% Optimistic TX
%%================================================================================

-define(serial_key, <<"emqx_ds_builtin_local_tx_serial">>).

otx_get_tx_serial(DB, Shard) ->
    emqx_ds_storage_layer_ttv:get_read_tx_serial({DB, Shard}).

otx_become_leader(DB, Shard) ->
    {ok, Serial} = get_tx_persistent_serial(DB, Shard),
    case current_timestamp({DB, Shard}) of
        undefined ->
            Timestamp = 0;
        Timestamp when is_integer(Timestamp) ->
            ok
    end,
    {ok, Serial, Timestamp}.

otx_get_leader(DB, Shard) ->
    emqx_ds_optimistic_tx:where(DB, Shard).

otx_get_latest_generation(DB, Shard) ->
    emqx_ds_storage_layer:generation_current({DB, Shard}).

otx_prepare_tx(DBShard, Generation, SerialBin, Ops, Opts) ->
    emqx_ds_storage_layer_ttv:prepare_tx(DBShard, Generation, SerialBin, Ops, Opts).

otx_commit_tx_batch(DBShard = {DB, Shard}, SerCtl, Serial, Timestamp, Batches) ->
    case get_tx_persistent_serial(DB, Shard) of
        {ok, SerCtl} ->
            maybe
                %% First, update the leader serial to avoid duplicated
                %% serials, should the following calls fail:
                ok ?=
                    emqx_ds_storage_layer:store_global(
                        DBShard, #{?serial_key => <<Serial:128>>}, #{}
                    ),
                %% Commit data:
                ok ?= emqx_ds_storage_layer_ttv:commit_batch(DBShard, Batches, #{}),
                %% Finally, update read serial and last timestamp:
                emqx_ds_storage_layer_ttv:set_read_tx_serial(DBShard, Serial),
                emqx_ds_builtin_local_meta:set_current_timestamp(DBShard, Timestamp + 1),
                %% Dispatch events:
                DispatchF = fun(Stream) -> emqx_ds_beamformer:shard_event(DBShard, [Stream]) end,
                emqx_ds_storage_layer_ttv:dispatch_events(DBShard, Batches, DispatchF)
            end;
        Val ->
            ?err_unrec({serial_mismatch, SerCtl, Val})
    end.

otx_lookup_ttv(DBShard, GenId, Topic, Timestamp) ->
    emqx_ds_storage_layer_ttv:lookup(DBShard, GenId, Topic, Timestamp).

otx_get_runtime_config(DB) ->
    #{runtime := #{transactions := Conf}} = emqx_dsch:get_db_runtime(DB),
    Conf.

%% Metadata API:
-spec stream_to_binary(emqx_ds:db(), stream()) -> {ok, binary()} | {error, _}.
stream_to_binary(_DB, Stream = #'Stream'{}) ->
    'DSBuiltinMetadata':encode('Stream', Stream).

-spec binary_to_stream(emqx_ds:db(), binary()) -> {ok, stream()} | {error, _}.
binary_to_stream(_DB, Bin) ->
    'DSBuiltinMetadata':decode('Stream', Bin).

-spec iterator_to_binary(emqx_ds:db(), iterator()) -> {ok, binary()} | {error, _}.
iterator_to_binary(_DB, end_of_stream) ->
    'DSBuiltinMetadata':encode('ReplayPosition', {endOfStream, 'NULL'});
iterator_to_binary(_DB, It = #'Iterator'{}) ->
    'DSBuiltinMetadata':encode('ReplayPosition', {value, It}).

-spec binary_to_iterator(emqx_ds:db(), binary()) -> {ok, iterator()} | {error, _}.
binary_to_iterator(_DB, Bin) ->
    case 'DSBuiltinMetadata':decode('ReplayPosition', Bin) of
        {ok, {endOfStream, 'NULL'}} ->
            {ok, end_of_stream};
        {ok, {value, It}} ->
            {ok, It};
        Err ->
            Err
    end.

-spec batch_size_and_time_limit(emqx_ds:db(), emqx_ds:shard(), emqx_ds:next_limit()) ->
    {pos_integer(), emqx_ds:time()}.
batch_size_and_time_limit(_DB, _Shard, BatchSize) when is_integer(BatchSize) ->
    {BatchSize, infinity};
batch_size_and_time_limit(_DB, _Shard, {time, MaxTS, BatchSize}) ->
    {BatchSize, MaxTS}.

%%================================================================================
%% `emqx_dsch' behavior callbacks
%%================================================================================

db_info(_) ->
    %% FIXME:
    {ok, ""}.

-spec handle_db_config_change(emqx_ds:db(), db_runtime_config()) -> ok.
handle_db_config_change(_, _) ->
    %% FIXME:
    ok.

-spec handle_schema_event(emqx_ds:db(), emqx_dsch:pending_id(), emqx_dsch:pending()) -> ok.
handle_schema_event(_DB, PendingId, _Pending) ->
    emqx_dsch:del_pending(PendingId).

%%================================================================================
%% Internal functions
%%================================================================================

get_tx_persistent_serial(DB, Shard) ->
    case emqx_ds_storage_layer:fetch_global({DB, Shard}, ?serial_key) of
        {ok, <<Val:128>>} ->
            {ok, Val};
        not_found ->
            {ok, 0}
    end.

-spec merge_config(emqx_dsch:db_schema(), emqx_dsch:db_runtime_config(), #{atom() => _}) ->
    {ok, db_opts()} | emqx_ds:error(_).
merge_config(OldSchema, OldConf, Patch) ->
    maybe
        %% Verify that certain immutable fields aren't changed:
        false ?=
            lists:search(
                fun(Field) ->
                    OldVal = maps:get(Field, OldSchema),
                    case Patch of
                        #{Field := NewVal} when NewVal =/= OldVal ->
                            true;
                        _ ->
                            false
                    end
                end,
                [n_shards]
            ),
        Merged = emqx_utils_maps:deep_merge(
            maps:merge(OldSchema, OldConf),
            Patch
        ),
        {ok, Merged}
    else
        {value, Field} ->
            ?err_unrec({unable_to_update_config, Field})
    end.

-spec verify_db_opts(db_opts()) -> {ok, db_schema(), db_runtime_config()} | emqx_ds:error(_).
verify_db_opts(Opts) ->
    maybe
        #{
            backend := builtin_local,
            payload_type := PType,
            n_shards := NShards,
            storage := Storage,
            subscriptions := Subs,
            transactions := Trans,
            rocksdb := RocksDB
        } ?= Opts,
        true ?= is_integer(NShards) andalso NShards > 0,
        true ?= is_map(Subs),
        true ?= is_map(Trans),
        true ?= is_map(RocksDB),
        Schema = #{
            backend => builtin_local,
            n_shards => NShards,
            payload_type => PType,
            storage => Storage
        },
        RTOpts = #{
            subscriptions => Subs,
            transactions => Trans,
            rocksdb => RocksDB
        },
        {ok, Schema, RTOpts}
    else
        What ->
            %% TODO: This reporting is rather insufficient
            ?tp(error, emqx_ds_builtin_local_invalid_conf, #{fail => What, conf => Opts}),
            ?err_unrec(badarg)
    end.

%%================================================================================
%% Common test options
%%================================================================================

-ifdef(TEST).

test_applications(Config) ->
    [
        {App, maps:get(App, Config, #{})}
     || App <- [emqx_durable_storage, emqx_ds_backends]
    ].

test_db_config(_Config) ->
    #{
        backend => builtin_local,
        storage => {emqx_ds_storage_skipstream_lts_v2, #{}},
        n_shards => 1
    }.

-endif.
