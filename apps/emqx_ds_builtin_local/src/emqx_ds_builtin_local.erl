%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_ds_builtin_local).

-behaviour(emqx_ds).
-behaviour(emqx_ds_buffer).
-behaviour(emqx_ds_beamformer).

%% API:
-export([]).

%% behavior callbacks:
-export([
    %% `emqx_ds':
    open_db/2,
    close_db/1,
    add_generation/1,
    update_db_config/2,
    list_generations_with_lifetimes/1,
    drop_generation/2,
    drop_db/1,
    store_batch/3,
    get_streams/3,
    get_delete_streams/3,
    make_iterator/4,
    make_delete_iterator/4,
    update_iterator/3,
    next/3,
    poll/3,
    delete_next/4,

    %% `beamformer':
    unpack_iterator/2,
    scan_stream/5,

    %% `emqx_ds_buffer':
    init_buffer/3,
    flush_buffer/4,
    shard_of_operation/4
]).

%% Internal exports:
-export([
    do_next/3,
    do_delete_next/4,
    %% Used by batch serializer
    make_batch/3
]).

-ifdef(TEST).
-export([test_applications/1, test_db_config/1]).
-endif.

-export_type([db_opts/0, shard/0, iterator/0, delete_iterator/0]).

-include_lib("emqx_utils/include/emqx_message.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(tag, 1).
-define(shard, 2).
-define(enc, 3).

-define(IT, 61).
-define(DELETE_IT, 62).

-type shard() :: binary().

-opaque iterator() ::
    #{
        ?tag := ?IT,
        ?shard := shard(),
        ?enc := term()
    }.

-opaque delete_iterator() ::
    #{
        ?tag := ?DELETE_IT,
        ?shard := shard(),
        ?enc := term()
    }.

-type db_opts() ::
    #{
        backend := builtin_local,
        storage := emqx_ds_storage_layer:prototype(),
        n_shards := pos_integer(),
        poll_workers_per_shard => pos_integer(),
        %% Equivalent to `append_only' from `emqx_ds:create_db_opts':
        force_monotonic_timestamps := boolean(),
        atomic_batches := boolean()
    }.

-type generation_rank() :: {shard(), emqx_ds_storage_layer:gen_id()}.

-define(stream(SHARD, INNER), [2, SHARD | INNER]).
-define(delete_stream(SHARD, INNER), [3, SHARD | INNER]).

%%================================================================================
%% API functions
%%================================================================================

%%================================================================================
%% behavior callbacks
%%================================================================================

-spec open_db(emqx_ds:db(), db_opts()) -> ok | {error, _}.
open_db(DB, CreateOpts0) ->
    %% Rename `append_only' flag to `force_monotonic_timestamps':
    AppendOnly = maps:get(append_only, CreateOpts0),
    CreateOpts = maps:put(force_monotonic_timestamps, AppendOnly, CreateOpts0),
    case emqx_ds_builtin_local_sup:start_db(DB, CreateOpts) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
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
    Errors = lists:filtermap(
        fun(Shard) ->
            ShardId = {DB, Shard},
            case
                emqx_ds_storage_layer:add_generation(
                    ShardId, emqx_ds_builtin_local_meta:ensure_monotonic_timestamp(ShardId)
                )
            of
                ok ->
                    false;
                Error ->
                    {true, {Shard, Error}}
            end
        end,
        Shards
    ),
    case Errors of
        [] -> ok;
        _ -> {error, Errors}
    end.

-spec update_db_config(emqx_ds:db(), db_opts()) -> ok | {error, _}.
update_db_config(DB, CreateOpts) ->
    Opts = #{} = emqx_ds_builtin_local_meta:update_db_config(DB, CreateOpts),
    lists:foreach(
        fun(Shard) ->
            ShardId = {DB, Shard},
            emqx_ds_storage_layer:update_config(
                ShardId, emqx_ds_builtin_local_meta:ensure_monotonic_timestamp(ShardId), Opts
            )
        end,
        emqx_ds_builtin_local_meta:shards(DB)
    ).

-spec list_generations_with_lifetimes(emqx_ds:db()) ->
    #{emqx_ds:generation_rank() => emqx_ds:generation_info()}.
list_generations_with_lifetimes(DB) ->
    lists:foldl(
        fun(Shard, Acc) ->
            maps:fold(
                fun(GenId, Data0, Acc1) ->
                    Data = maps:update_with(
                        until,
                        fun timeus_to_timestamp/1,
                        maps:update_with(since, fun timeus_to_timestamp/1, Data0)
                    ),
                    Acc1#{{Shard, GenId} => Data}
                end,
                Acc,
                emqx_ds_storage_layer:list_generations_with_lifetimes({DB, Shard})
            )
        end,
        #{},
        emqx_ds_builtin_local_meta:shards(DB)
    ).

-spec drop_generation(emqx_ds:db(), generation_rank()) -> ok | {error, _}.
drop_generation(DB, {Shard, GenId}) ->
    emqx_ds_storage_layer:drop_generation({DB, Shard}, GenId).

-spec drop_db(emqx_ds:db()) -> ok | {error, _}.
drop_db(DB) ->
    close_db(DB),
    lists:foreach(
        fun(Shard) ->
            emqx_ds_storage_layer:drop_shard({DB, Shard})
        end,
        emqx_ds_builtin_local_meta:shards(DB)
    ),
    emqx_ds_builtin_local_meta:drop_db(DB).

-spec store_batch(emqx_ds:db(), emqx_ds:batch(), emqx_ds:message_store_opts()) ->
    emqx_ds:store_batch_result().
store_batch(DB, Batch, Opts) ->
    case emqx_ds_builtin_local_meta:db_config(DB) of
        #{atomic_batches := true} ->
            store_batch_atomic(DB, Batch, Opts);
        _ ->
            store_batch_buffered(DB, Batch, Opts)
    end.

store_batch_buffered(DB, Messages, Opts) ->
    try
        emqx_ds_buffer:store_batch(DB, Messages, Opts)
    catch
        error:{Reason, _Call} when Reason == timeout; Reason == noproc ->
            {error, recoverable, Reason}
    end.

store_batch_atomic(DB, Batch, Opts) ->
    Shards = shards_of_batch(DB, Batch),
    case Shards of
        [Shard] ->
            emqx_ds_builtin_local_batch_serializer:store_batch_atomic(DB, Shard, Batch, Opts);
        [] ->
            ok;
        [_ | _] ->
            {error, unrecoverable, atomic_batch_spans_multiple_shards}
    end.

shards_of_batch(DB, #dsbatch{operations = Operations, preconditions = Preconditions}) ->
    shards_of_batch(DB, Preconditions, shards_of_batch(DB, Operations, []));
shards_of_batch(DB, Operations) ->
    shards_of_batch(DB, Operations, []).

shards_of_batch(DB, [Operation | Rest], Acc) ->
    case shard_of_operation(DB, Operation, clientid, #{}) of
        Shard when Shard =:= hd(Acc) ->
            shards_of_batch(DB, Rest, Acc);
        Shard when Acc =:= [] ->
            shards_of_batch(DB, Rest, [Shard]);
        ShardAnother ->
            [ShardAnother | Acc]
    end;
shards_of_batch(_DB, [], Acc) ->
    Acc.

-record(bs, {options :: emqx_ds:create_db_opts()}).
-type buffer_state() :: #bs{}.

-spec init_buffer(emqx_ds:db(), shard(), _Options) -> {ok, buffer_state()}.
init_buffer(DB, Shard, Options) ->
    ShardId = {DB, Shard},
    case current_timestamp(ShardId) of
        undefined ->
            Latest = erlang:system_time(microsecond),
            emqx_ds_builtin_local_meta:set_current_timestamp(ShardId, Latest);
        _Latest ->
            ok
    end,
    {ok, #bs{options = Options}}.

-spec flush_buffer(emqx_ds:db(), shard(), [emqx_types:message()], buffer_state()) ->
    {buffer_state(), emqx_ds:store_batch_result()}.
flush_buffer(DB, Shard, Messages, S0 = #bs{options = Options}) ->
    ShardId = {DB, Shard},
    ForceMonotonic = maps:get(force_monotonic_timestamps, Options),
    {Latest, Batch} = make_batch(ForceMonotonic, current_timestamp(ShardId), Messages),
    DispatchF = fun(Events) ->
        emqx_ds_beamformer:shard_event({DB, Shard}, Events)
    end,
    Result = emqx_ds_storage_layer:store_batch(ShardId, Batch, _Options = #{}, DispatchF),
    emqx_ds_builtin_local_meta:set_current_timestamp(ShardId, Latest),
    {S0, Result}.

make_batch(_ForceMonotonic = true, Latest, Messages) ->
    assign_monotonic_timestamps(Latest, Messages, []);
make_batch(false, Latest, Messages) ->
    assign_operation_timestamps(Latest, Messages, []).

assign_monotonic_timestamps(Latest0, [Message = #message{} | Rest], Acc0) ->
    case emqx_message:timestamp(Message, microsecond) of
        TimestampUs when TimestampUs > Latest0 ->
            Latest = TimestampUs;
        _Earlier ->
            Latest = Latest0 + 1
    end,
    Acc = [assign_timestamp(Latest, Message) | Acc0],
    assign_monotonic_timestamps(Latest, Rest, Acc);
assign_monotonic_timestamps(Latest, [Operation | Rest], Acc0) ->
    Acc = [Operation | Acc0],
    assign_monotonic_timestamps(Latest, Rest, Acc);
assign_monotonic_timestamps(Latest, [], Acc) ->
    {Latest, lists:reverse(Acc)}.

assign_operation_timestamps(Latest0, [Message = #message{} | Rest], Acc0) ->
    TimestampUs = emqx_message:timestamp(Message),
    Latest = max(TimestampUs, Latest0),
    Acc = [assign_timestamp(TimestampUs, Message) | Acc0],
    assign_operation_timestamps(Latest, Rest, Acc);
assign_operation_timestamps(Latest, [Operation | Rest], Acc0) ->
    Acc = [Operation | Acc0],
    assign_operation_timestamps(Latest, Rest, Acc);
assign_operation_timestamps(Latest, [], Acc) ->
    {Latest, lists:reverse(Acc)}.

assign_timestamp(TimestampUs, Message) ->
    {TimestampUs, Message}.

-spec shard_of_operation(emqx_ds:db(), emqx_ds:operation(), clientid | topic, _Options) -> shard().
shard_of_operation(DB, #message{from = From, topic = Topic}, SerializeBy, _Options) ->
    case SerializeBy of
        clientid -> Key = From;
        topic -> Key = Topic
    end,
    shard_of_key(DB, Key);
shard_of_operation(DB, {_, #message_matcher{from = From, topic = Topic}}, SerializeBy, _Options) ->
    case SerializeBy of
        clientid -> Key = From;
        topic -> Key = Topic
    end,
    shard_of_key(DB, Key).

shard_of_key(DB, Key) ->
    N = emqx_ds_builtin_local_meta:n_shards(DB),
    Hash = erlang:phash2(Key, N),
    integer_to_binary(Hash).

-spec get_streams(emqx_ds:db(), emqx_ds:topic_filter(), emqx_ds:time()) ->
    [{emqx_ds:stream_rank(), emqx_ds:ds_specific_stream()}].
get_streams(DB, TopicFilter, StartTime) ->
    Shards = emqx_ds_builtin_local_meta:shards(DB),
    lists:flatmap(
        fun(Shard) ->
            Streams = emqx_ds_storage_layer:get_streams(
                {DB, Shard}, TopicFilter, timestamp_to_timeus(StartTime)
            ),
            lists:map(
                fun({RankY, InnerStream}) ->
                    Rank = {Shard, RankY},
                    {Rank, ?stream(Shard, InnerStream)}
                end,
                Streams
            )
        end,
        Shards
    ).

-spec make_iterator(
    emqx_ds:db(), emqx_ds:ds_specific_stream(), emqx_ds:topic_filter(), emqx_ds:time()
) ->
    emqx_ds:make_iterator_result(iterator()).
make_iterator(DB, ?stream(Shard, InnerStream), TopicFilter, StartTime) ->
    ShardId = {DB, Shard},
    case
        emqx_ds_storage_layer:make_iterator(
            ShardId, InnerStream, TopicFilter, timestamp_to_timeus(StartTime)
        )
    of
        {ok, Iter} ->
            {ok, #{?tag => ?IT, ?shard => Shard, ?enc => Iter}};
        Error = {error, _, _} ->
            Error
    end.

-spec update_iterator(_Shard, emqx_ds:ds_specific_iterator(), emqx_ds:message_key()) ->
    emqx_ds:make_iterator_result(iterator()).
update_iterator(ShardId, Iter0 = #{?tag := ?IT, ?enc := StorageIter0}, Key) ->
    case emqx_ds_storage_layer:update_iterator(ShardId, StorageIter0, Key) of
        {ok, StorageIter} ->
            {ok, Iter0#{?enc => StorageIter}};
        Err = {error, _, _} ->
            Err
    end.

-spec next(emqx_ds:db(), iterator(), pos_integer()) -> emqx_ds:next_result(iterator()).
next(DB, Iter, N) ->
    {ok, Ref} = emqx_ds_lib:with_worker(undefined, ?MODULE, do_next, [DB, Iter, N]),
    receive
        #poll_reply{ref = Ref, payload = Data} ->
            Data
    end.

-spec poll(emqx_ds:db(), emqx_ds:poll_iterators(), emqx_ds:poll_opts()) -> {ok, reference()}.
poll(DB, Iterators, PollOpts = #{timeout := Timeout}) ->
    %% Create a new alias, if not already provided:
    case PollOpts of
        #{reply_to := ReplyTo} ->
            ok;
        _ ->
            ReplyTo = alias([explicit_unalias])
    end,
    %% Spawn a helper process that will notify the caller when the
    %% poll times out:
    emqx_ds_lib:send_poll_timeout(ReplyTo, Timeout),
    %% Submit poll jobs:
    lists:foreach(
        fun({ItKey, It = #{?tag := ?IT, ?shard := Shard}}) ->
            ShardId = {DB, Shard},
            ReturnAddr = {ReplyTo, ItKey},
            emqx_ds_beamformer:poll(node(), ReturnAddr, ShardId, It, PollOpts)
        end,
        Iterators
    ),
    {ok, ReplyTo}.

unpack_iterator(Shard, #{?tag := ?IT, ?enc := Iterator}) ->
    emqx_ds_storage_layer:unpack_iterator(Shard, Iterator).

scan_stream(ShardId, Stream, TopicFilter, StartMsg, BatchSize) ->
    {DB, _} = ShardId,
    Now = current_timestamp(ShardId),
    T0 = erlang:monotonic_time(microsecond),
    Result = emqx_ds_storage_layer:scan_stream(
        ShardId, Stream, TopicFilter, Now, StartMsg, BatchSize
    ),
    T1 = erlang:monotonic_time(microsecond),
    emqx_ds_builtin_metrics:observe_next_time(DB, T1 - T0),
    Result.

-spec get_delete_streams(emqx_ds:db(), emqx_ds:topic_filter(), emqx_ds:time()) ->
    [emqx_ds:ds_specific_delete_stream()].
get_delete_streams(DB, TopicFilter, StartTime) ->
    Shards = emqx_ds_builtin_local_meta:shards(DB),
    lists:flatmap(
        fun(Shard) ->
            Streams = emqx_ds_storage_layer:get_delete_streams(
                {DB, Shard}, TopicFilter, timestamp_to_timeus(StartTime)
            ),
            lists:map(
                fun(InnerStream) ->
                    ?delete_stream(Shard, InnerStream)
                end,
                Streams
            )
        end,
        Shards
    ).

-spec make_delete_iterator(
    emqx_ds:db(), emqx_ds:ds_specific_delete_stream(), emqx_ds:topic_filter(), emqx_ds:time()
) ->
    emqx_ds:make_delete_iterator_result(delete_iterator()).
make_delete_iterator(DB, ?delete_stream(Shard, InnerStream), TopicFilter, StartTime) ->
    ShardId = {DB, Shard},
    case
        emqx_ds_storage_layer:make_delete_iterator(
            ShardId, InnerStream, TopicFilter, timestamp_to_timeus(StartTime)
        )
    of
        {ok, Iter} ->
            {ok, #{?tag => ?DELETE_IT, ?shard => Shard, ?enc => Iter}};
        Error = {error, _, _} ->
            Error
    end.

-spec delete_next(emqx_ds:db(), delete_iterator(), emqx_ds:delete_selector(), pos_integer()) ->
    emqx_ds:delete_next_result(emqx_ds:delete_iterator()).
delete_next(DB, Iter, Selector, N) ->
    {ok, Ref} = emqx_ds_lib:with_worker(undefined, ?MODULE, do_delete_next, [DB, Iter, Selector, N]),
    receive
        #poll_reply{ref = Ref, payload = Data} -> Data
    end.

%%================================================================================
%% Internal exports
%%================================================================================

current_timestamp(ShardId) ->
    emqx_ds_builtin_local_meta:current_timestamp(ShardId).

-spec do_next(emqx_ds:db(), iterator(), pos_integer()) -> emqx_ds:next_result(iterator()).
do_next(DB, Iter0 = #{?tag := ?IT, ?shard := Shard, ?enc := StorageIter0}, N) ->
    ShardId = {DB, Shard},
    T0 = erlang:monotonic_time(microsecond),
    Result = emqx_ds_storage_layer:next(ShardId, StorageIter0, N, current_timestamp(ShardId)),
    T1 = erlang:monotonic_time(microsecond),
    emqx_ds_builtin_metrics:observe_next_time(DB, T1 - T0),
    case Result of
        {ok, StorageIter, Batch} ->
            Iter = Iter0#{?enc := StorageIter},
            {ok, Iter, Batch};
        Other ->
            Other
    end.

-spec do_delete_next(emqx_ds:db(), delete_iterator(), emqx_ds:delete_selector(), pos_integer()) ->
    emqx_ds:delete_next_result(delete_iterator()).
do_delete_next(
    DB, Iter = #{?tag := ?DELETE_IT, ?shard := Shard, ?enc := StorageIter0}, Selector, N
) ->
    ShardId = {DB, Shard},
    case
        emqx_ds_storage_layer:delete_next(
            ShardId, StorageIter0, Selector, N, current_timestamp(ShardId)
        )
    of
        {ok, StorageIter, Ndeleted} ->
            {ok, Iter#{?enc => StorageIter}, Ndeleted};
        {ok, end_of_stream} ->
            {ok, end_of_stream};
        Error ->
            Error
    end.

%%================================================================================
%% Internal functions
%%================================================================================

timestamp_to_timeus(TimestampMs) ->
    TimestampMs * 1000.

timeus_to_timestamp(undefined) ->
    undefined;
timeus_to_timestamp(TimestampUs) ->
    TimestampUs div 1000.

%%================================================================================
%% Common test options
%%================================================================================

-ifdef(TEST).

test_applications(_Config) ->
    [
        emqx_durable_storage,
        emqx_ds_backends
    ].

test_db_config(_Config) ->
    #{
        backend => builtin_local,
        storage => {emqx_ds_storage_reference, #{}},
        n_shards => 1
    }.

-endif.
