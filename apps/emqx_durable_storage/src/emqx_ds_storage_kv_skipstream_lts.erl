%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_ds_storage_kv_skipstream_lts).

-behaviour(emqx_ds_storage_layer_kv).

%% API:
-export([]).

%% behavior callbacks:
-export([
    create/6,
    open/5,
    drop/5,
    prepare_blob_tx/4,
    commit_batch/4,
    get_streams/4,
    make_iterator/5,
    next/6,
    lookup_message/4,

    batch_events/3
]).

%% internal exports:
-export([]).

%% inline small functions:
-compile(inline).

-export_type([schema/0, s/0]).

-include_lib("snabbkaffe/include/trace.hrl").
-include("emqx_ds.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-elvis([{elvis_style, nesting_level, disable}]).
-elvis([{elvis_style, no_if_expression, disable}]).

%%================================================================================
%% Type declarations
%%================================================================================

%% TLOG entry
%%   Keys:
-define(cooked_msg_ops, 6).
-define(cooked_lts_ops, 7).
%%   Payload:
-define(cooked_delete, 100).
-define(cooked_msg_op(STATIC, VARYING, VALUE),
    {STATIC, VARYING, VALUE}
).

%% Permanent state:
-type schema() ::
    #{
        %% Number of index key bytes allocated for the hash of topic level.
        wildcard_hash_bytes := pos_integer(),
        %% Number of data / index key bytes allocated for the static topic index.
        topic_index_bytes := pos_integer(),
        serialization_schema := emqx_ds_msg_serializer:schema(),
        with_guid := boolean(),
        lts_threshold_spec => emqx_ds_lts:threshold_spec()
    }.

%% Default LTS thresholds: 0th level = 100 entries max, other levels = 10 entries.
-define(DEFAULT_LTS_THRESHOLD, {simple, {100, 10}}).

%% Runtime state:
-record(s, {
    db :: rocksdb:db_handle(),
    data_cf :: rocksdb:cf_handle(),
    trie :: emqx_ds_lts:trie(),
    trie_cf :: rocksdb:cf_handle(),
    serialization_schema :: emqx_ds_msg_serializer:schema(),
    gs :: emqx_ds_gen_skipstream_lts:s(),
    threshold_fun :: emqx_ds_lts:threshold_fun()
}).

-type s() :: #s{}.

-record(kv_stream, {
    static_index :: emqx_ds_lts:static_key()
}).

-record(it, {
    static_index :: emqx_ds_lts:static_key(),
    %% Key of the last visited message:
    last_key :: emqx_ds_gen_skipstream_lts:stream_key(),
    %% Compressed topic filter:
    compressed_tf :: binary()
}).

%% Last Seen Key placeholder that is used to indicate that the
%% iterator points at the beginning of the topic.
-define(lsk_begin, <<>>).

%%================================================================================
%% API functions
%%================================================================================

%%================================================================================
%% behavior callbacks
%%================================================================================

create(_ShardId, DBHandle, GenId, Schema0, SPrev, _DBOpts) ->
    Defaults = #{
        wildcard_hash_bytes => 8,
        topic_index_bytes => 8,
        serialization_schema => blob,
        lts_threshold_spec => ?DEFAULT_LTS_THRESHOLD
    },
    Schema = maps:merge(Defaults, Schema0),
    ok = emqx_ds_msg_serializer:check_schema(maps:get(serialization_schema, Schema)),
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
        wildcard_hash_bytes := WCBytes,
        serialization_schema := SSchema
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
            trie => Trie,
            serialization_schema => SSchema,
            %% Byte size of md5 hash:
            stream_key_size => 16,
            get_topic => fun({Topic, _}) -> Topic end,
            wildcard_hash_bytes => WCBytes
        }
    ),
    #s{
        db = DBHandle,
        trie_cf = TrieCF,
        trie = Trie,
        gs = GS,
        serialization_schema = SSchema,
        threshold_fun = emqx_ds_lts:threshold_fun(ThresholdSpec)
    }.

drop(_ShardId, DBHandle, _GenId, _CFRefs, #s{data_cf = DataCF, trie_cf = TrieCF, trie = Trie}) ->
    emqx_ds_lts:destroy(Trie),
    ok = rocksdb:drop_column_family(DBHandle, DataCF),
    ok = rocksdb:drop_column_family(DBHandle, TrieCF),
    ok.

prepare_blob_tx(DBShard, S, Ops, _Options) ->
    _ = emqx_ds_gen_skipstream_lts:pop_lts_persist_ops(),
    W = maps:get(?ds_tx_write, Ops, []),
    DT = maps:get(?ds_tx_delete_topic, Ops, []),
    try
        OperationsCooked = cook_blob_deletes(DBShard, S, DT, cook_blob_writes(S, W, [])),
        {ok, #{
            ?cooked_msg_ops => OperationsCooked,
            ?cooked_lts_ops => emqx_ds_gen_skipstream_lts:pop_lts_persist_ops()
        }}
    catch
        {Type, Err} ->
            {error, Type, Err}
    end.

cook_blob_writes(_, [], Acc) ->
    lists:reverse(Acc);
cook_blob_writes(S = #s{serialization_schema = SSchema}, [{Topic, Value} | Rest], Acc) ->
    #s{trie = Trie, threshold_fun = TFun} = S,
    {Static, Varying} = emqx_ds_lts:topic_key(Trie, TFun, Topic),
    Bin = emqx_ds_msg_serializer:serialize(SSchema, {Varying, Value}),
    cook_blob_writes(S, Rest, [?cooked_msg_op(Static, Varying, Bin) | Acc]).

cook_blob_deletes(DBShard, S, Topics, Acc0) ->
    lists:foldl(
        fun(TopicFilter, Acc) ->
            cook_blob_deletes1(DBShard, S, TopicFilter, Acc)
        end,
        Acc0,
        Topics
    ).

cook_blob_deletes1(DBShard, S, TopicFilter, Acc0) ->
    lists:foldl(
        fun(Stream, Acc) ->
            {ok, #it{static_index = Static, compressed_tf = Varying, last_key = LK}} = make_iterator(
                DBShard, S, Stream, TopicFilter, 0
            ),
            cook_blob_deletes2(S#s.gs, Static, Varying, LK, Acc)
        end,
        Acc0,
        get_streams(S#s.trie, TopicFilter)
    ).

cook_blob_deletes2(GS, Static, Varying, LSK, Acc0) ->
    Fun = fun(_TopicStructure, _DSKey, Var, _Val, Acc) ->
        [?cooked_msg_op(Static, Var, ?cooked_delete) | Acc]
    end,
    Interval = {'[', LSK, infinity},
    %% Note: we don't reverse the batch here.
    case emqx_ds_gen_skipstream_lts:fold(GS, Static, Varying, Interval, 100, Acc0, Fun) of
        {ok, SK, Acc} when SK =:= LSK ->
            %% TODO: is this a reliable stopping condition?
            Acc;
        {ok, SK, Acc} ->
            cook_blob_deletes2(GS, Static, Varying, SK, Acc);
        {error, _, _} = Err ->
            error(Err)
    end.

commit_batch(
    ShardId,
    #s{db = DB, trie_cf = TrieCF, trie = Trie, gs = GS},
    #{?cooked_lts_ops := LtsOps, ?cooked_msg_ops := Operations},
    Options
) ->
    {ok, Batch} = rocksdb:batch(),
    try
        %% Commit LTS trie to the storage:
        lists:foreach(
            fun({Key, Val}) ->
                ok = rocksdb:batch_put(Batch, TrieCF, term_to_binary(Key), term_to_binary(Val))
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
        emqx_ds_gen_skipstream_lts:notify_new_streams(ShardId, Trie, LtsOps),
        %% Commit payloads:
        lists:foreach(
            fun(?cooked_msg_op(Static, Varying, Op)) ->
                StreamKey = hash_topic_levels(Varying),
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
    #{?cooked_msg_ops := Payloads}
) ->
    EventMap = lists:foldl(
        fun
            (?cooked_msg_op(_Static, _Varying, ?cooked_delete), Acc) ->
                Acc;
            (?cooked_msg_op(Static, _Varying, _ValBlob), Acc) ->
                maps:put(#kv_stream{static_index = Static}, 1, Acc)
        end,
        #{},
        Payloads
    ),
    maps:keys(EventMap).

get_streams(_Shard, #s{trie = Trie}, TopicFilter, _) ->
    get_streams(Trie, TopicFilter).

make_iterator(
    _Shard,
    #s{trie = Trie},
    #kv_stream{static_index = StaticIdx},
    TopicFilter,
    StartPos
) ->
    ?tp_ignore_side_effects_in_prod(emqx_ds_storage_skipstream_lts_make_blob_iterator, #{
        static_index => StaticIdx, topic_filter => TopicFilter
    }),
    {ok, TopicStructure} = emqx_ds_lts:reverse_lookup(Trie, StaticIdx),
    CompressedTF = emqx_ds_lts:compress_topic(StaticIdx, TopicStructure, TopicFilter),
    case StartPos of
        beginning ->
            LastSK = ?lsk_begin;
        0 ->
            %% FIXME, don't translate it here.
            LastSK = ?lsk_begin;
        LastSK when is_binary(LastSK) ->
            ok
    end,
    {ok, #it{
        static_index = StaticIdx,
        last_key = LastSK,
        compressed_tf = CompressedTF
    }}.

next(_DBShard, #s{gs = GS}, ItSeed, BatchSize, _, _) ->
    Fun = fun(TopicStructure, DSKey, Varying, {_, Val}, Acc) ->
        Topic = emqx_ds_lts:decompress_topic(TopicStructure, Varying),
        [{DSKey, {Topic, Val}} | Acc]
    end,
    #it{static_index = Static, compressed_tf = Varying, last_key = LSK} = ItSeed,
    Interval = {'(', LSK, infinity},
    %% Note: we don't reverse the batch here.
    case emqx_ds_gen_skipstream_lts:fold(GS, Static, Varying, Interval, BatchSize, [], Fun) of
        {ok, SK, Batch} ->
            {ok, ItSeed#it{last_key = SK}, Batch};
        {error, _, _} = Err ->
            Err
    end.

lookup_message(
    _Shard,
    #s{trie = Trie, gs = GS},
    Topic,
    _Timestamp
) ->
    maybe
        {ok, {Static, Varying}} ?= emqx_ds_lts:lookup_topic_key(Trie, Topic),
        StreamKey = hash_topic_levels(Varying),
        {ok, _DSKey, {_, Value}} ?=
            emqx_ds_gen_skipstream_lts:lookup_message(GS, Static, StreamKey),
        {ok, {Topic, Value}}
    end.

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

get_streams(Trie, TopicFilter) ->
    lists:map(
        fun({Static, _Varying}) ->
            #kv_stream{static_index = Static}
        end,
        emqx_ds_lts:match_topics(Trie, TopicFilter)
    ).

%%%%%%%% Keys %%%%%%%%%%

hash_topic_levels(Varying) ->
    erlang:md5(Varying).

%%%%%%%% Column families %%%%%%%%%%

%% @doc Generate a column family ID for the MQTT messages
-spec data_cf(emqx_ds_storage_layer:gen_id()) -> [char()].
data_cf(GenId) ->
    "emqx_ds_storage_kv_skipstream_lts_data" ++ integer_to_list(GenId).

%% @doc Generate a column family ID for the trie
-spec trie_cf(emqx_ds_storage_layer:gen_id()) -> [char()].
trie_cf(GenId) ->
    "emqx_ds_storage_kv_skipstream_lts_trie" ++ integer_to_list(GenId).

%%================================================================================
%% Tests
%%================================================================================
