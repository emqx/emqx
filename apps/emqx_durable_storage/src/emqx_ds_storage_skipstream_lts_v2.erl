%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc This module implements a storage layout based on the
%% "skipstream" indexing + LTS suitable for storing KV pairs.
%%
%% NOTE: Varying topic levels are used as the stream key.
-module(emqx_ds_storage_skipstream_lts_v2).

-behaviour(emqx_ds_storage_layer_ttv).

%% API:
-export([]).

%% behavior callbacks:
-export([
    create/6,
    open/5,
    drop/5,
    prepare_tx/7,
    commit_batch/4,
    get_streams/4,
    make_iterator/6,
    unpack_iterator/3,
    fast_forward/8,
    next/9,
    scan_stream/9,
    lookup/4,

    message_match_context/4,
    iterator_match_context/3,

    batch_events/4
]).

%% internal exports:

%% inline small functions:
-compile(inline).

-export_type([schema/0, s/0]).

-include_lib("snabbkaffe/include/trace.hrl").
-include("emqx_ds.hrl").
-include("../gen_src/DSBuiltinSLSkipstreamV2.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-elvis([{elvis_style, atom_naming_convention, disable}]).
-elvis([{elvis_style, max_anonymous_function_arity, disable}]).

%% https://github.com/erlang/otp/issues/9841
-dialyzer({nowarn_function, [encode_ext_it_static/2, decode_ext_it_static/1]}).

%%================================================================================
%% Type declarations
%%================================================================================

%% TLOG entry
%%   Keys:
-define(cooked_msg_ops, 6).
-define(cooked_lts_ops, 7).
%%   Payload:
-define(cooked_delete, 100).
-define(cooked_msg_op(STATIC, VARYING, TS, VALUE),
    {STATIC, VARYING, TS, VALUE}
).
%%   Range deletion:
-define(cooked_range_del(STATIC, LEVEL, HASH, FROM, TO),
    {STATIC, LEVEL, HASH, FROM, TO}
).

%% Permanent state:
-type schema() ::
    #{
        timestamp_bytes := non_neg_integer(),
        %% Number of index key bytes allocated for the hash of topic level.
        wildcard_hash_bytes := pos_integer(),
        lts_threshold_spec => emqx_ds_lts:threshold_spec()
    }.

%% Default LTS thresholds: 0th level = 100 entries max, other levels = 10 entries.
-define(DEFAULT_LTS_THRESHOLD, {simple, {100, 10}}).

%% Runtime state:
-record(s, {
    db :: rocksdb:db_handle(),
    trie :: emqx_ds_lts:trie(),
    trie_cf :: rocksdb:cf_handle(),
    ts_bytes :: non_neg_integer(),
    max_ts :: non_neg_integer(),
    gs :: emqx_ds_gen_skipstream_lts:s(),
    threshold_fun :: emqx_ds_lts:threshold_fun()
}).

-type s() :: #s{}.

-define(stream(STATIC), STATIC).

%%================================================================================
%% API functions
%%================================================================================

%%================================================================================
%% behavior callbacks
%%================================================================================

create(_ShardId, DBHandle, GenId, Schema0, SPrev, _DBOpts) ->
    Defaults = #{
        wildcard_hash_bytes => 8,
        timestamp_bytes => 8,
        topic_index_bytes => 8,
        lts_threshold_spec => ?DEFAULT_LTS_THRESHOLD
    },
    Schema = maps:merge(Defaults, Schema0),
    DataCFName = data_cf(GenId),
    TrieCFName = trie_cf(GenId),
    {ok, DataCFHandle} = rocksdb:create_column_family(DBHandle, DataCFName, []),
    {ok, TrieCFHandle} = rocksdb:create_column_family(DBHandle, TrieCFName, []),
    case SPrev of
        #s{trie = TriePrev} ->
            ok = emqx_ds_gen_skipstream_lts:copy_previous_trie(DBHandle, TrieCFHandle, TriePrev),
            ?tp(layout_inherited_lts_trie, #{}),
            ok;
        undefined ->
            ok
    end,
    {Schema, [{DataCFName, DataCFHandle}, {TrieCFName, TrieCFHandle}]}.

open(
    ShardId,
    DBHandle,
    GenId,
    CFRefs,
    Schema = #{
        topic_index_bytes := TIBytes,
        timestamp_bytes := TSB,
        wildcard_hash_bytes := WCBytes
    }
) ->
    {_, DataCF} = lists:keyfind(data_cf(GenId), 1, CFRefs),
    {_, TrieCF} = lists:keyfind(trie_cf(GenId), 1, CFRefs),
    Trie = emqx_ds_gen_skipstream_lts:restore_trie(ShardId, TIBytes, DBHandle, TrieCF),
    ThresholdSpec = maps:get(lts_threshold_spec, Schema, ?DEFAULT_LTS_THRESHOLD),
    GS = emqx_ds_gen_skipstream_lts:open(
        #{
            db_shard => ShardId,
            db_handle => DBHandle,
            data_cf => DataCF,
            trie_cf => TrieCF,
            trie => Trie,
            deserialize => fun(Bin) -> Bin end,
            decompose => fun(StreamKey, Val) ->
                {Varying, TS} = decompose_stream_key(TSB, StreamKey),
                {Varying, TS, Val}
            end,
            wildcard_hash_bytes => WCBytes
        }
    ),
    #s{
        db = DBHandle,
        trie_cf = TrieCF,
        trie = Trie,
        gs = GS,
        ts_bytes = TSB,
        max_ts = 1 bsl (8 * TSB),
        threshold_fun = emqx_ds_lts:threshold_fun(ThresholdSpec)
    }.

drop(_ShardId, _DBHandle, _GenId, _CFRefs, #s{gs = GS}) ->
    emqx_ds_gen_skipstream_lts:drop(GS).

prepare_tx(_DBShard, PTSchema, S, TXID, _Options, TxWrites, TxDeleteTopics) ->
    _ = emqx_ds_gen_skipstream_lts:pop_lts_persist_ops(),
    PTransFun = emqx_ds_payload_transform:ser_fun(PTSchema),
    try
        OperationsCooked = cook_blob_deletes(
            S, TxDeleteTopics, cook_blob_writes(PTransFun, S, TXID, TxWrites, [])
        ),
        {ok, #{
            ?cooked_msg_ops => OperationsCooked,
            ?cooked_lts_ops => emqx_ds_gen_skipstream_lts:pop_lts_persist_ops()
        }}
    catch
        {Type, Err} ->
            {error, Type, Err}
    end.

cook_blob_writes(_, _, _TXID, [], Acc) ->
    lists:reverse(Acc);
cook_blob_writes(PTransFun, S = #s{}, TXID, [{Topic, TS0, Value0} | Rest], Acc) ->
    #s{trie = Trie, threshold_fun = TFun, max_ts = MaxTS} = S,
    %% Get the timestamp:
    case TS0 of
        ?ds_tx_ts_monotonic ->
            TS = emqx_ds_optimistic_tx:get_monotonic_timestamp();
        TS when is_integer(TS) ->
            ok
    end,
    %% Verify that TS fits in the TSB:
    (TS >= 0 andalso TS < MaxTS) orelse
        throw({unrecoverable, {timestamp_is_out_of_range, TS}}),
    {Static, Varying} = emqx_ds_lts:topic_key(Trie, TFun, Topic),
    Value =
        case Value0 of
            ?ds_tx_serial ->
                TXID;
            _ ->
                PTransFun(Value0)
        end,
    cook_blob_writes(PTransFun, S, TXID, Rest, [?cooked_msg_op(Static, Varying, TS, Value) | Acc]).

cook_blob_deletes(S, Topics, Acc0) ->
    lists:foldl(
        fun({TopicFilter, FromTime, ToTime}, Acc) ->
            cook_blob_deletes1(S, TopicFilter, FromTime, ToTime, Acc)
        end,
        Acc0,
        Topics
    ).

cook_blob_deletes1(S, TopicFilter, FromTime, ToTime, Acc0) ->
    lists:foldl(
        fun(Stream, Acc) ->
            {ok, Static, Varying, ItPos} = make_internal_iterator(S, Stream, TopicFilter, FromTime),
            ItLimit = time_limit(S, ToTime),
            cook_blob_deletes2(S#s.gs, Static, Varying, ItPos, ItLimit, Acc)
        end,
        Acc0,
        get_streams(S#s.trie, TopicFilter)
    ).

cook_blob_deletes2(GS, Static, Varying, ItStart, ItLimit, Acc0) ->
    Fun = fun(_TopicStructure, _DSKey, Var, TS, _Val, Acc) ->
        [?cooked_msg_op(Static, Var, TS, ?cooked_delete) | Acc]
    end,
    Interval = {'[', ItStart, ItLimit},
    %% Note: we don't reverse the batch here.
    case emqx_ds_gen_skipstream_lts:fold(GS, Static, Varying, Interval, 100, Acc0, Fun) of
        {ok, NextStreamKey, Acc} when NextStreamKey =:= ItStart ->
            %% End the loop if the key didn't change.
            %% TODO: is this a reliable stopping condition?
            Acc;
        {ok, NextStreamKey, Acc} ->
            cook_blob_deletes2(GS, Static, Varying, NextStreamKey, ItLimit, Acc);
        {error, _, _} = Err ->
            error(Err)
    end.

commit_batch(
    ShardId,
    #s{db = DB, trie_cf = TrieCF, trie = Trie, gs = GS, ts_bytes = TSB},
    Input,
    Options
) ->
    {ok, Batch} = rocksdb:batch(),
    try
        %% Handle LTS updates:
        lists:foreach(
            fun(#{?cooked_lts_ops := LtsOps}) ->
                %% Commit LTS trie to the storage:
                lists:foreach(
                    fun({Key, Val}) ->
                        ok = rocksdb:batch_put(
                            Batch, TrieCF, term_to_binary(Key), term_to_binary(Val)
                        )
                    end,
                    LtsOps
                ),
                %% Apply LTS ops to the memory cache and notify about the new
                %% streams. Note: in case of `builtin_raft' backend
                %% notification is sent _for every replica_ of the database, to
                %% account for possible delays in the replication. Event
                %% deduplication logic of `emqx_ds_new_streams' module should
                %% mitigate the performance impact of repeated events.
                _ = emqx_ds_lts:trie_update(Trie, LtsOps),
                emqx_ds_gen_skipstream_lts:notify_new_streams(ShardId, Trie, LtsOps)
            end,
            Input
        ),
        %% Commit payloads:
        lists:foreach(
            fun(#{?cooked_msg_ops := Operations}) ->
                lists:foreach(
                    fun(?cooked_msg_op(Static, Varying, TS, Op)) ->
                        StreamKey = make_stream_key(TSB, TS, Varying),
                        case Op of
                            Payload when is_binary(Payload) ->
                                emqx_ds_gen_skipstream_lts:batch_put(
                                    GS, Batch, Static, Varying, StreamKey, Payload
                                );
                            ?cooked_delete ->
                                emqx_ds_gen_skipstream_lts:batch_delete(
                                    GS, Batch, Static, Varying, StreamKey
                                )
                        end
                    end,
                    Operations
                )
            end,
            Input
        ),
        Result = rocksdb:write_batch(DB, Batch, [
            {disable_wal, not maps:get(durable, Options, true)}
        ]),
        %% NOTE
        %% Strictly speaking, `{error, incomplete}` is a valid result but should be impossible to
        %% observe until there's `{no_slowdown, true}` in write options.
        case Result of
            ok ->
                ok;
            {error, {error, Reason}} ->
                ?err_unrec({rocksdb, Reason})
        end
    after
        rocksdb:release_batch(Batch)
    end.

batch_events(
    _Shard,
    #s{},
    Inputs,
    Callback
) ->
    do_batch_events(Inputs, Callback, #{}).

do_batch_events([], _Callback, _Streams) ->
    ok;
do_batch_events([#{?cooked_msg_ops := Payloads} | Rest], Callback, Acc0) ->
    Acc = lists:foldl(
        fun
            (?cooked_msg_op(_Static, _Varying, _TS, ?cooked_delete), Acc1) ->
                Acc1;
            (?cooked_msg_op(Static, _Varying, _TS, _ValBlob), Acc1) ->
                case Acc1 of
                    #{Static := _} ->
                        Acc1;
                    #{} ->
                        Callback(Static),
                        Acc1#{Static => []}
                end
        end,
        Acc0,
        Payloads
    ),
    do_batch_events(Rest, Callback, Acc).

get_streams(_Shard, #s{trie = Trie}, TopicFilter, _) ->
    get_streams(Trie, TopicFilter).

make_iterator(_DB, _Shard, S, Stream, TopicFilter, StartPos) ->
    maybe
        {ok, Static, Varying, ItPos} ?=
            make_internal_iterator(S, Stream, TopicFilter, StartPos),
        {ok, encode_ext_it_static(Static, Varying), ItPos}
    end.

unpack_iterator(_DBShard, #s{trie = Trie}, ItStaticBin) ->
    {Static, CompressedTF} = decode_ext_it_static(ItStaticBin),
    maybe
        {ok, TF} ?= decompress_tf(Trie, Static, CompressedTF),
        {ok, Static, TF}
    end.

next(_DB, _Shard, S, ItStaticBin, ItPos0, BatchSize, Now, IsCurrent, PTrans) ->
    {Static, CompressedTF} = decode_ext_it_static(ItStaticBin),
    next_internal(
        next_cb_without_key(PTrans), S, Static, CompressedTF, ItPos0, BatchSize, Now, IsCurrent
    ).

scan_stream(
    _DB, _Shard, S = #s{trie = Trie}, Static, TF, Pos, BatchSize, Now, IsCurrent
) ->
    maybe
        {ok, CompressedTF} ?= compress_tf(Trie, Static, TF),
        next_internal(
            fun next_cb_with_key/6, S, Static, CompressedTF, Pos, BatchSize, Now, IsCurrent
        )
    end.

fast_forward(
    _DB, _Shard, S = #s{ts_bytes = TSB}, ItStaticBin, Pos0, BatchSize, TMax, Target
) ->
    case Target of
        <<TS:(TSB * 8), _/binary>> when TS < TMax ->
            {Static, CompressedTF} = decode_ext_it_static(ItStaticBin),
            case
                next_internal(
                    fun next_cb_with_key/6,
                    S,
                    Static,
                    CompressedTF,
                    Pos0,
                    BatchSize + 1,
                    TMax,
                    true
                )
            of
                Res = {ok, _, Batch} when length(Batch) =< BatchSize ->
                    Res;
                {ok, _, _} ->
                    ?err_rec(cannot_fast_forward);
                Err ->
                    Err
            end;
        _ ->
            ?err_rec(#{msg => <<"Key is too far in the future">>, max => TMax, to => Target})
    end.

lookup(_Shard, #s{trie = Trie, gs = GS, ts_bytes = TSB}, Topic, Time) ->
    maybe
        {ok, {Static, Varying}} ?= emqx_ds_lts:lookup_topic_key(Trie, Topic),
        StreamKey = make_stream_key(TSB, Time, Varying),
        {ok, _DSKey, Value} ?=
            emqx_ds_gen_skipstream_lts:lookup_message(GS, Static, StreamKey),
        {ok, Value}
    end.

message_match_context(#s{trie = Trie}, Stream, _, {Topic, TS, _Value}) ->
    maybe
        {ok, CT} ?= compress_tf(Trie, Stream, Topic),
        {ok, {CT, TS}}
    end.

iterator_match_context(#s{ts_bytes = TSB}, ItStaticBin, ItPos) ->
    {_Static, CompressedTF} = decode_ext_it_static(ItStaticBin),
    <<ItTS:(TSB * 8), _/binary>> = ItPos,
    fun({CompressedTopic, TS}) ->
        TS > ItTS andalso emqx_topic:match(CompressedTopic, CompressedTF)
    end.

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

make_internal_iterator(
    #s{trie = Trie, ts_bytes = TSB},
    ?stream(StaticIdx),
    TopicFilter,
    StartPos
) ->
    ?tp_ignore_side_effects_in_prod(emqx_ds_storage_skipstream_lts_make_blob_iterator, #{
        static_index => StaticIdx, topic_filter => TopicFilter, start_pos => StartPos
    }),
    maybe
        {ok, CompressedTF} ?= compress_tf(Trie, StaticIdx, TopicFilter),
        LastSK = <<StartPos:(TSB * 8)>>,
        {ok, StaticIdx, CompressedTF, LastSK}
    end.

next_internal(
    Fun, S = #s{gs = GS}, Static, CompressedTF, ItPos0, BatchSize, TimeLimit, IsCurrent
) ->
    Interval = {'(', ItPos0, time_limit(S, TimeLimit)},
    case emqx_ds_gen_skipstream_lts:fold(GS, Static, CompressedTF, Interval, BatchSize, [], Fun) of
        {ok, _, []} when not IsCurrent ->
            {ok, end_of_stream};
        {ok, ItPos, Batch} ->
            {ok, ItPos, lists:reverse(Batch)};
        {error, _, _} = Err ->
            Err
    end.

next_cb_without_key(PTSchema) ->
    Fun = emqx_ds_payload_transform:deser_fun(PTSchema),
    fun(TopicStructure, _DSKey, Varying, TS, Val, Acc) ->
        Topic = emqx_ds_lts:decompress_topic(TopicStructure, Varying),
        [Fun({Topic, TS, Val}) | Acc]
    end.

next_cb_with_key(TopicStructure, DSKey, Varying, TS, Val, Acc) ->
    Topic = emqx_ds_lts:decompress_topic(TopicStructure, Varying),
    [{DSKey, {Topic, TS, Val}} | Acc].

get_streams(Trie, TopicFilter) ->
    lists:map(
        fun({Static, _Varying}) ->
            ?stream(Static)
        end,
        emqx_ds_lts:match_topics(Trie, TopicFilter)
    ).

compress_tf(Trie, Static, TF) ->
    case emqx_ds_lts:reverse_lookup(Trie, Static) of
        {ok, TopicStructure} ->
            try
                {ok, emqx_ds_lts:compress_topic(Static, TopicStructure, TF)}
            catch
                {Class, Err} ->
                    {error, Class, Err}
            end;
        undefined ->
            ?err_unrec(unknown_lts_v2_stream)
    end.

decompress_tf(Trie, Static, CompressedTF) ->
    case emqx_ds_lts:reverse_lookup(Trie, Static) of
        {ok, TopicStructure} ->
            {ok, emqx_ds_lts:decompress_topic(TopicStructure, CompressedTF)};
        undefined ->
            ?err_unrec(unknown_lts_v2_stream)
    end.

%%%%%%%% Keys %%%%%%%%%%

%% TODO: https://github.com/erlang/otp/issues/9841
-dialyzer({nowarn_function, [make_stream_key/3, decompose_stream_key/2]}).
make_stream_key(TSB, Time, Varying) ->
    {ok, VaryingBin} = 'DSMetadataCommon':encode('TopicWords', Varying),
    <<Time:(TSB * 8), VaryingBin/binary>>.

decompose_stream_key(TSB, StreamKey) ->
    <<TS:(TSB * 8), VaryingBin/binary>> = StreamKey,
    {ok, Varying} = 'DSMetadataCommon':decode('TopicWords', VaryingBin),
    {Varying, TS}.

time_limit(_, infinity) ->
    infinity;
time_limit(#s{ts_bytes = TSB, max_ts = Max}, Limit) when
    is_integer(Limit), Limit >= 0, Limit < Max
->
    %% Create a stream key prefix corresponding to the key:
    <<Limit:(TSB * 8)>>;
time_limit(#s{max_ts = Max}, Limit) when is_integer(Limit), Limit > Max ->
    infinity.

%%%%%%%% Column families %%%%%%%%%%

%% @doc Generate a column family ID for the MQTT messages
-spec data_cf(emqx_ds_storage_layer:gen_id()) -> [char()].
data_cf(GenId) ->
    "emqx_ds_storage_skipstream_lts_v2_data" ++ integer_to_list(GenId).

%% @doc Generate a column family ID for the trie
-spec trie_cf(emqx_ds_storage_layer:gen_id()) -> [char()].
trie_cf(GenId) ->
    "emqx_ds_storage_skipstream_lts_v2_trie" ++ integer_to_list(GenId).

%%%%%%%% External iterator format %%%%%%%%%%

%% @doc Transform iterator static part (not Pos!) from/to the external
%% serializable representation:
encode_ext_it_static(Static, Varying) ->
    {ok, Bin} = 'DSBuiltinSLSkipstreamV2':encode(
        'Iterator',
        #'Iterator'{
            static = Static,
            topicFilter = emqx_ds_lib:tf_to_asn1(Varying)
        }
    ),
    Bin.

decode_ext_it_static(Bin) ->
    {ok, #'Iterator'{static = Static, topicFilter = Varying}} = 'DSBuiltinSLSkipstreamV2':decode(
        'Iterator', Bin
    ),
    {Static, emqx_ds_lib:asn1_to_tf(Varying)}.

%%================================================================================
%% Tests
%%================================================================================
