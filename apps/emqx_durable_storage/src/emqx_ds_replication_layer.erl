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

%% @doc Replication layer for DS backends that don't support
%% replication on their own.
-module(emqx_ds_replication_layer).

-behaviour(emqx_ds).

-export([
    list_shards/1,
    open_db/2,
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
    delete_next/4,
    shard_of_message/3
]).

%% internal exports:
-export([
    do_drop_db_v1/1,
    do_store_batch_v1/4,
    do_get_streams_v1/4,
    do_get_streams_v2/4,
    do_make_iterator_v1/5,
    do_make_iterator_v2/5,
    do_update_iterator_v2/4,
    do_next_v1/4,
    do_add_generation_v2/1,
    do_list_generations_with_lifetimes_v3/2,
    do_drop_generation_v3/3,
    do_get_delete_streams_v4/4,
    do_make_delete_iterator_v4/5,
    do_delete_next_v4/5,

    %% FIXME
    ra_store_batch/3
]).

-export([
    init/1,
    apply/3
]).

-export_type([
    shard_id/0,
    builtin_db_opts/0,
    stream_v1/0,
    stream/0,
    delete_stream/0,
    iterator/0,
    delete_iterator/0,
    message_id/0,
    batch/0
]).

-include_lib("emqx_utils/include/emqx_message.hrl").
-include("emqx_ds_replication_layer.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-type shard_id() :: binary().

-type builtin_db_opts() ::
    #{
        backend := builtin,
        storage := emqx_ds_storage_layer:prototype(),
        n_shards => pos_integer(),
        n_sites => pos_integer(),
        replication_factor => pos_integer()
    }.

%% This enapsulates the stream entity from the replication level.
%%
%% TODO: this type is obsolete and is kept only for compatibility with
%% v3 BPAPI. Remove it when emqx_ds_proto_v4 is gone (EMQX 5.6)
-opaque stream_v1() ::
    #{
        ?tag := ?STREAM,
        ?shard := emqx_ds_replication_layer:shard_id(),
        ?enc := emqx_ds_storage_layer:stream_v1()
    }.

-define(stream_v2(SHARD, INNER), [2, SHARD | INNER]).
-define(delete_stream(SHARD, INNER), [3, SHARD | INNER]).

-opaque stream() :: nonempty_maybe_improper_list().

-opaque delete_stream() :: nonempty_maybe_improper_list().

-opaque iterator() ::
    #{
        ?tag := ?IT,
        ?shard := emqx_ds_replication_layer:shard_id(),
        ?enc := emqx_ds_storage_layer:iterator()
    }.

-opaque delete_iterator() ::
    #{
        ?tag := ?DELETE_IT,
        ?shard := emqx_ds_replication_layer:shard_id(),
        ?enc := emqx_ds_storage_layer:delete_iterator()
    }.

-type message_id() :: emqx_ds:message_id().

-type batch() :: #{
    ?tag := ?BATCH,
    ?batch_messages := [emqx_types:message()]
}.

-type generation_rank() :: {shard_id(), term()}.

%%================================================================================
%% API functions
%%================================================================================

-spec list_shards(emqx_ds:db()) -> [shard_id()].
list_shards(DB) ->
    emqx_ds_replication_layer_meta:shards(DB).

-spec open_db(emqx_ds:db(), builtin_db_opts()) -> ok | {error, _}.
open_db(DB, CreateOpts) ->
    case emqx_ds_builtin_sup:start_db(DB, CreateOpts) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            ok;
        {error, Err} ->
            {error, Err}
    end.

-spec add_generation(emqx_ds:db()) -> ok | {error, _}.
add_generation(DB) ->
    foreach_shard(
        DB,
        fun(Shard) -> ok = ra_add_generation(DB, Shard) end
    ).

-spec update_db_config(emqx_ds:db(), builtin_db_opts()) -> ok | {error, _}.
update_db_config(DB, CreateOpts) ->
    ok = emqx_ds_replication_layer_meta:update_db_config(DB, CreateOpts),
    Opts = emqx_ds_replication_layer_meta:get_options(DB),
    foreach_shard(
        DB,
        fun(Shard) -> ok = ra_update_config(DB, Shard, Opts) end
    ).

-spec list_generations_with_lifetimes(emqx_ds:db()) ->
    #{generation_rank() => emqx_ds:generation_info()}.
list_generations_with_lifetimes(DB) ->
    Shards = list_shards(DB),
    lists:foldl(
        fun(Shard, GensAcc) ->
            maps:fold(
                fun(GenId, Data, AccInner) ->
                    AccInner#{{Shard, GenId} => Data}
                end,
                GensAcc,
                ra_list_generations_with_lifetimes(DB, Shard)
            )
        end,
        #{},
        Shards
    ).

-spec drop_generation(emqx_ds:db(), generation_rank()) -> ok | {error, _}.
drop_generation(DB, {Shard, GenId}) ->
    ra_drop_generation(DB, Shard, GenId).

-spec drop_db(emqx_ds:db()) -> ok | {error, _}.
drop_db(DB) ->
    Nodes = list_nodes(),
    _ = emqx_ds_proto_v4:drop_db(Nodes, DB),
    _ = emqx_ds_replication_layer_meta:drop_db(DB),
    emqx_ds_builtin_sup:stop_db(DB),
    ok.

-spec store_batch(emqx_ds:db(), [emqx_types:message(), ...], emqx_ds:message_store_opts()) ->
    emqx_ds:store_batch_result().
store_batch(DB, Messages, Opts) ->
    try
        emqx_ds_replication_layer_egress:store_batch(DB, Messages, Opts)
    catch
        error:{Reason, _Call} when Reason == timeout; Reason == noproc ->
            {error, recoverable, Reason}
    end.

-spec get_streams(emqx_ds:db(), emqx_ds:topic_filter(), emqx_ds:time()) ->
    [{emqx_ds:stream_rank(), stream()}].
get_streams(DB, TopicFilter, StartTime) ->
    Shards = list_shards(DB),
    lists:flatmap(
        fun(Shard) ->
            Streams =
                try
                    ra_get_streams(DB, Shard, TopicFilter, StartTime)
                catch
                    error:{erpc, _} ->
                        %% TODO: log?
                        []
                end,
            lists:map(
                fun({RankY, StorageLayerStream}) ->
                    RankX = Shard,
                    Rank = {RankX, RankY},
                    {Rank, ?stream_v2(Shard, StorageLayerStream)}
                end,
                Streams
            )
        end,
        Shards
    ).

-spec get_delete_streams(emqx_ds:db(), emqx_ds:topic_filter(), emqx_ds:time()) ->
    [delete_stream()].
get_delete_streams(DB, TopicFilter, StartTime) ->
    Shards = list_shards(DB),
    lists:flatmap(
        fun(Shard) ->
            Streams = ra_get_delete_streams(DB, Shard, TopicFilter, StartTime),
            lists:map(
                fun(StorageLayerStream) ->
                    ?delete_stream(Shard, StorageLayerStream)
                end,
                Streams
            )
        end,
        Shards
    ).

-spec make_iterator(emqx_ds:db(), stream(), emqx_ds:topic_filter(), emqx_ds:time()) ->
    emqx_ds:make_iterator_result(iterator()).
make_iterator(DB, Stream, TopicFilter, StartTime) ->
    ?stream_v2(Shard, StorageStream) = Stream,
    try ra_make_iterator(DB, Shard, StorageStream, TopicFilter, StartTime) of
        {ok, Iter} ->
            {ok, #{?tag => ?IT, ?shard => Shard, ?enc => Iter}};
        Error = {error, _, _} ->
            Error
    catch
        error:RPCError = {erpc, _} ->
            {error, recoverable, RPCError}
    end.

-spec make_delete_iterator(emqx_ds:db(), delete_stream(), emqx_ds:topic_filter(), emqx_ds:time()) ->
    emqx_ds:make_delete_iterator_result(delete_iterator()).
make_delete_iterator(DB, Stream, TopicFilter, StartTime) ->
    ?delete_stream(Shard, StorageStream) = Stream,
    case ra_make_delete_iterator(DB, Shard, StorageStream, TopicFilter, StartTime) of
        {ok, Iter} ->
            {ok, #{?tag => ?DELETE_IT, ?shard => Shard, ?enc => Iter}};
        Err = {error, _} ->
            Err
    end.

-spec update_iterator(emqx_ds:db(), iterator(), emqx_ds:message_key()) ->
    emqx_ds:make_iterator_result(iterator()).
update_iterator(DB, OldIter, DSKey) ->
    #{?tag := ?IT, ?shard := Shard, ?enc := StorageIter} = OldIter,
    try ra_update_iterator(DB, Shard, StorageIter, DSKey) of
        {ok, Iter} ->
            {ok, #{?tag => ?IT, ?shard => Shard, ?enc => Iter}};
        Error = {error, _, _} ->
            Error
    catch
        error:RPCError = {erpc, _} ->
            {error, recoverable, RPCError}
    end.

-spec next(emqx_ds:db(), iterator(), pos_integer()) -> emqx_ds:next_result(iterator()).
next(DB, Iter0, BatchSize) ->
    #{?tag := ?IT, ?shard := Shard, ?enc := StorageIter0} = Iter0,
    %% TODO: iterator can contain information that is useful for
    %% reconstructing messages sent over the network. For example,
    %% when we send messages with the learned topic index, we could
    %% send the static part of topic once, and append it to the
    %% messages on the receiving node, hence saving some network.
    %%
    %% This kind of trickery should be probably done here in the
    %% replication layer. Or, perhaps, in the logic layer.
    case ra_next(DB, Shard, StorageIter0, BatchSize) of
        {ok, StorageIter, Batch} ->
            Iter = Iter0#{?enc := StorageIter},
            {ok, Iter, Batch};
        Ok = {ok, _} ->
            Ok;
        Error = {error, _, _} ->
            Error;
        RPCError = {badrpc, _} ->
            {error, recoverable, RPCError}
    end.

-spec delete_next(emqx_ds:db(), delete_iterator(), emqx_ds:delete_selector(), pos_integer()) ->
    emqx_ds:delete_next_result(delete_iterator()).
delete_next(DB, Iter0, Selector, BatchSize) ->
    #{?tag := ?DELETE_IT, ?shard := Shard, ?enc := StorageIter0} = Iter0,
    case ra_delete_next(DB, Shard, StorageIter0, Selector, BatchSize) of
        {ok, StorageIter, NumDeleted} ->
            Iter = Iter0#{?enc := StorageIter},
            {ok, Iter, NumDeleted};
        Other ->
            Other
    end.

-spec shard_of_message(emqx_ds:db(), emqx_types:message(), clientid | topic) ->
    emqx_ds_replication_layer:shard_id().
shard_of_message(DB, #message{from = From, topic = Topic}, SerializeBy) ->
    N = emqx_ds_replication_layer_meta:n_shards(DB),
    Hash =
        case SerializeBy of
            clientid -> erlang:phash2(From, N);
            topic -> erlang:phash2(Topic, N)
        end,
    integer_to_binary(Hash).

foreach_shard(DB, Fun) ->
    lists:foreach(Fun, list_shards(DB)).

%%================================================================================
%% behavior callbacks
%%================================================================================

%%================================================================================
%% Internal exports (RPC targets)
%%================================================================================

-spec do_drop_db_v1(emqx_ds:db()) -> ok | {error, _}.
do_drop_db_v1(DB) ->
    MyShards = emqx_ds_replication_layer_meta:my_shards(DB),
    emqx_ds_builtin_sup:stop_db(DB),
    lists:foreach(
        fun(Shard) ->
            emqx_ds_storage_layer:drop_shard({DB, Shard}),
            ra_drop_shard(DB, Shard)
        end,
        MyShards
    ).

-spec do_store_batch_v1(
    emqx_ds:db(),
    emqx_ds_replication_layer:shard_id(),
    batch(),
    emqx_ds:message_store_opts()
) ->
    emqx_ds:store_batch_result().
do_store_batch_v1(DB, Shard, #{?tag := ?BATCH, ?batch_messages := Messages}, Options) ->
    emqx_ds_storage_layer:store_batch({DB, Shard}, Messages, Options).

%% Remove me in EMQX 5.6
-dialyzer({nowarn_function, do_get_streams_v1/4}).
-spec do_get_streams_v1(
    emqx_ds:db(), emqx_ds_replication_layer:shard_id(), emqx_ds:topic_filter(), emqx_ds:time()
) ->
    [{integer(), emqx_ds_storage_layer:stream_v1()}].
do_get_streams_v1(_DB, _Shard, _TopicFilter, _StartTime) ->
    error(obsolete_api).

-spec do_get_streams_v2(
    emqx_ds:db(), emqx_ds_replication_layer:shard_id(), emqx_ds:topic_filter(), emqx_ds:time()
) ->
    [{integer(), emqx_ds_storage_layer:stream()}].
do_get_streams_v2(DB, Shard, TopicFilter, StartTime) ->
    emqx_ds_storage_layer:get_streams({DB, Shard}, TopicFilter, StartTime).

-dialyzer({nowarn_function, do_make_iterator_v1/5}).
-spec do_make_iterator_v1(
    emqx_ds:db(),
    emqx_ds_replication_layer:shard_id(),
    emqx_ds_storage_layer:stream_v1(),
    emqx_ds:topic_filter(),
    emqx_ds:time()
) ->
    emqx_ds:make_iterator_result(emqx_ds_storage_layer:iterator()).
do_make_iterator_v1(_DB, _Shard, _Stream, _TopicFilter, _StartTime) ->
    error(obsolete_api).

-spec do_make_iterator_v2(
    emqx_ds:db(),
    emqx_ds_replication_layer:shard_id(),
    emqx_ds_storage_layer:stream(),
    emqx_ds:topic_filter(),
    emqx_ds:time()
) ->
    emqx_ds:make_iterator_result(emqx_ds_storage_layer:iterator()).
do_make_iterator_v2(DB, Shard, Stream, TopicFilter, StartTime) ->
    emqx_ds_storage_layer:make_iterator({DB, Shard}, Stream, TopicFilter, StartTime).

-spec do_make_delete_iterator_v4(
    emqx_ds:db(),
    emqx_ds_replication_layer:shard_id(),
    emqx_ds_storage_layer:delete_stream(),
    emqx_ds:topic_filter(),
    emqx_ds:time()
) ->
    {ok, emqx_ds_storage_layer:delete_iterator()} | {error, _}.
do_make_delete_iterator_v4(DB, Shard, Stream, TopicFilter, StartTime) ->
    emqx_ds_storage_layer:make_delete_iterator({DB, Shard}, Stream, TopicFilter, StartTime).

-spec do_update_iterator_v2(
    emqx_ds:db(),
    emqx_ds_replication_layer:shard_id(),
    emqx_ds_storage_layer:iterator(),
    emqx_ds:message_key()
) ->
    emqx_ds:make_iterator_result(emqx_ds_storage_layer:iterator()).
do_update_iterator_v2(DB, Shard, OldIter, DSKey) ->
    emqx_ds_storage_layer:update_iterator(
        {DB, Shard}, OldIter, DSKey
    ).

-spec do_next_v1(
    emqx_ds:db(),
    emqx_ds_replication_layer:shard_id(),
    emqx_ds_storage_layer:iterator(),
    pos_integer()
) ->
    emqx_ds:next_result(emqx_ds_storage_layer:iterator()).
do_next_v1(DB, Shard, Iter, BatchSize) ->
    emqx_ds_storage_layer:next({DB, Shard}, Iter, BatchSize).

-spec do_delete_next_v4(
    emqx_ds:db(),
    emqx_ds_replication_layer:shard_id(),
    emqx_ds_storage_layer:delete_iterator(),
    emqx_ds:delete_selector(),
    pos_integer()
) ->
    emqx_ds:delete_next_result(emqx_ds_storage_layer:delete_iterator()).
do_delete_next_v4(DB, Shard, Iter, Selector, BatchSize) ->
    emqx_ds_storage_layer:delete_next({DB, Shard}, Iter, Selector, BatchSize).

-spec do_add_generation_v2(emqx_ds:db()) -> ok | {error, _}.
do_add_generation_v2(DB) ->
    %% FIXME
    MyShards = [],
    lists:foreach(
        fun(ShardId) ->
            emqx_ds_storage_layer:add_generation({DB, ShardId})
        end,
        MyShards
    ).

-spec do_list_generations_with_lifetimes_v3(emqx_ds:db(), shard_id()) ->
    #{emqx_ds:ds_specific_generation_rank() => emqx_ds:generation_info()}.
do_list_generations_with_lifetimes_v3(DB, ShardId) ->
    emqx_ds_storage_layer:list_generations_with_lifetimes({DB, ShardId}).

-spec do_drop_generation_v3(emqx_ds:db(), shard_id(), emqx_ds_storage_layer:gen_id()) ->
    ok | {error, _}.
do_drop_generation_v3(DB, ShardId, GenId) ->
    emqx_ds_storage_layer:drop_generation({DB, ShardId}, GenId).

-spec do_get_delete_streams_v4(
    emqx_ds:db(), emqx_ds_replication_layer:shard_id(), emqx_ds:topic_filter(), emqx_ds:time()
) ->
    [emqx_ds_storage_layer:delete_stream()].
do_get_delete_streams_v4(DB, Shard, TopicFilter, StartTime) ->
    emqx_ds_storage_layer:get_delete_streams({DB, Shard}, TopicFilter, StartTime).

%%================================================================================
%% Internal functions
%%================================================================================

list_nodes() ->
    mria:running_nodes().

%%

%% TODO
%% Too large for normal operation, need better backpressure mechanism.
-define(RA_TIMEOUT, 60 * 1000).

ra_store_batch(DB, Shard, Messages) ->
    Command = #{
        ?tag => ?BATCH,
        ?batch_messages => Messages
    },
    Servers = emqx_ds_replication_layer_shard:servers(DB, Shard, leader_preferred),
    case ra:process_command(Servers, Command, ?RA_TIMEOUT) of
        {ok, Result, _Leader} ->
            Result;
        Error ->
            Error
    end.

ra_add_generation(DB, Shard) ->
    Command = #{?tag => add_generation},
    Servers = emqx_ds_replication_layer_shard:servers(DB, Shard, leader_preferred),
    case ra:process_command(Servers, Command, ?RA_TIMEOUT) of
        {ok, Result, _Leader} ->
            Result;
        Error ->
            error(Error, [DB, Shard])
    end.

ra_update_config(DB, Shard, Opts) ->
    Command = #{?tag => update_config, ?config => Opts},
    Servers = emqx_ds_replication_layer_shard:servers(DB, Shard, leader_preferred),
    case ra:process_command(Servers, Command, ?RA_TIMEOUT) of
        {ok, Result, _Leader} ->
            Result;
        Error ->
            error(Error, [DB, Shard])
    end.

ra_drop_generation(DB, Shard, GenId) ->
    Command = #{?tag => drop_generation, ?generation => GenId},
    Servers = emqx_ds_replication_layer_shard:servers(DB, Shard, leader_preferred),
    case ra:process_command(Servers, Command, ?RA_TIMEOUT) of
        {ok, Result, _Leader} ->
            Result;
        Error ->
            error(Error, [DB, Shard])
    end.

ra_get_streams(DB, Shard, TopicFilter, Time) ->
    {_Name, Node} = emqx_ds_replication_layer_shard:server(DB, Shard, local_preferred),
    emqx_ds_proto_v4:get_streams(Node, DB, Shard, TopicFilter, Time).

ra_get_delete_streams(DB, Shard, TopicFilter, Time) ->
    {_Name, Node} = emqx_ds_replication_layer_shard:server(DB, Shard, local_preferred),
    emqx_ds_proto_v4:get_delete_streams(Node, DB, Shard, TopicFilter, Time).

ra_make_iterator(DB, Shard, Stream, TopicFilter, StartTime) ->
    {_Name, Node} = emqx_ds_replication_layer_shard:server(DB, Shard, local_preferred),
    emqx_ds_proto_v4:make_iterator(Node, DB, Shard, Stream, TopicFilter, StartTime).

ra_make_delete_iterator(DB, Shard, Stream, TopicFilter, StartTime) ->
    {_Name, Node} = emqx_ds_replication_layer_shard:server(DB, Shard, local_preferred),
    emqx_ds_proto_v4:make_delete_iterator(Node, DB, Shard, Stream, TopicFilter, StartTime).

ra_update_iterator(DB, Shard, Iter, DSKey) ->
    {_Name, Node} = emqx_ds_replication_layer_shard:server(DB, Shard, local_preferred),
    emqx_ds_proto_v4:update_iterator(Node, DB, Shard, Iter, DSKey).

ra_next(DB, Shard, Iter, BatchSize) ->
    {_Name, Node} = emqx_ds_replication_layer_shard:server(DB, Shard, local_preferred),
    emqx_ds_proto_v4:next(Node, DB, Shard, Iter, BatchSize).

ra_delete_next(DB, Shard, Iter, Selector, BatchSize) ->
    {_Name, Node} = emqx_ds_replication_layer_shard:server(DB, Shard, local_preferred),
    emqx_ds_proto_v4:delete_next(Node, DB, Shard, Iter, Selector, BatchSize).

ra_list_generations_with_lifetimes(DB, Shard) ->
    {_Name, Node} = emqx_ds_replication_layer_shard:server(DB, Shard, local_preferred),
    emqx_ds_proto_v4:list_generations_with_lifetimes(Node, DB, Shard).

ra_drop_shard(DB, Shard) ->
    LocalServer = emqx_ds_replication_layer_shard:server(DB, Shard, local),
    ra:force_delete_server(_System = default, LocalServer).

%%

init(#{db := DB, shard := Shard}) ->
    #{db_shard => {DB, Shard}, latest => 0}.

apply(
    #{index := RaftIdx},
    #{
        ?tag := ?BATCH,
        ?batch_messages := MessagesIn
    },
    #{db_shard := DBShard, latest := Latest} = State
) ->
    %% NOTE
    %% Unique timestamp tracking real time closely.
    %% With microsecond granularity it should be nearly impossible for it to run
    %% too far ahead than the real time clock.
    {NLatest, Messages} = assign_timestamps(Latest, MessagesIn),
    %% TODO
    %% Batch is now reversed, but it should not make a lot of difference.
    %% Even if it would be in order, it's still possible to write messages far away
    %% in the past, i.e. when replica catches up with the leader. Storage layer
    %% currently relies on wall clock time to decide if it's safe to iterate over
    %% next epoch, this is likely wrong. Ideally it should rely on consensus clock
    %% time instead.
    Result = emqx_ds_storage_layer:store_batch(DBShard, Messages, #{}),
    NState = State#{latest := NLatest},
    %% TODO: Need to measure effects of changing frequency of `release_cursor`.
    Effect = {release_cursor, RaftIdx, NState},
    {NState, Result, Effect};
apply(
    _RaftMeta,
    #{?tag := add_generation},
    #{db_shard := DBShard} = State
) ->
    Result = emqx_ds_storage_layer:add_generation(DBShard),
    {State, Result};
apply(
    _RaftMeta,
    #{?tag := update_config, ?config := Opts},
    #{db_shard := DBShard} = State
) ->
    Result = emqx_ds_storage_layer:update_config(DBShard, Opts),
    {State, Result};
apply(
    _RaftMeta,
    #{?tag := drop_generation, ?generation := GenId},
    #{db_shard := DBShard} = State
) ->
    Result = emqx_ds_storage_layer:drop_generation(DBShard, GenId),
    {State, Result}.

assign_timestamps(Latest, Messages) ->
    assign_timestamps(Latest, Messages, []).

assign_timestamps(Latest, [MessageIn | Rest], Acc) ->
    case emqx_message:timestamp(MessageIn) of
        Later when Later > Latest ->
            assign_timestamps(Later, Rest, [MessageIn | Acc]);
        _Earlier ->
            Message = emqx_message:set_timestamp(Latest + 1, MessageIn),
            assign_timestamps(Latest + 1, Rest, [Message | Acc])
    end;
assign_timestamps(Latest, [], Acc) ->
    {Latest, Acc}.
