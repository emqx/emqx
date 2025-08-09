%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Replication layer for DS backends that don't support
%% replication on their own.
-module(emqx_ds_replication_layer).

-behaviour(emqx_ds).

-export([
    list_shards/1,
    open_db/2,
    close_db/1,
    add_generation/1,
    add_generation/2,
    update_db_config/2,
    list_generations_with_lifetimes/1,
    drop_slab/2,
    drop_db/1,
    store_batch/3,
    get_streams/4,
    get_delete_streams/3,
    make_iterator/4,
    make_delete_iterator/4,
    next/3,
    delete_next/4,

    subscribe/3,
    unsubscribe/2,
    suback/3,
    subscription_info/2,

    current_timestamp/2,

    new_tx/2,
    commit_tx/3,
    tx_commit_outcome/1,

    stream_to_binary/2,
    binary_to_stream/2,
    iterator_to_binary/2,
    binary_to_iterator/2
]).

-behaviour(emqx_ds_buffer).
-export([
    shard_of/2,
    shard_of_operation/4,
    flush_buffer/4,
    init_buffer/3
]).

%% internal exports:
-export([
    %% RPC Targets:
    do_drop_db_v1/1,
    do_get_streams_v2/4,
    do_get_streams_v3/5,
    do_make_iterator_v2/5,
    do_make_iterator_v3/5,
    do_make_iterator_ttv_v1/5,
    do_update_iterator_v2/4,
    do_next_v1/4,
    do_next_v2/4,
    do_next_ttv/3,
    do_list_generations_with_lifetimes_v3/2,
    do_get_delete_streams_v4/4,
    do_make_delete_iterator_v4/5,
    do_delete_next_v4/5,
    do_poll_v1/5,
    %% Obsolete:
    do_store_batch_v1/4,
    do_add_generation_v2/1,
    do_drop_generation_v3/3,
    %% OTX:
    do_new_kv_tx_ctx_v1/4,

    %% Egress API:
    ra_store_batch/3
]).

-behaviour(ra_machine).
-export([
    init/1,
    apply/3,
    tick/2,

    state_enter/2,

    snapshot_module/0
]).

-behaviour(emqx_ds_beamformer).
-export([
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
    otx_become_leader/2,
    otx_prepare_tx/5,
    otx_commit_tx_batch/5,
    otx_lookup_ttv/4,
    otx_get_runtime_config/1
]).

-ifdef(TEST).
-export([test_applications/1, test_db_config/1]).
-endif.

-export_type([
    shard_id/0,
    builtin_db_opts/0,
    stream_v1/0,
    stream/0,
    delete_stream/0,
    iterator/0,
    delete_iterator/0,
    batch/0,
    tx_context/0
]).

-export_type([
    ra_state/0
]).

-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include("emqx_ds_replication_layer.hrl").
-include("../../emqx_durable_storage/gen_src/DSBuiltinMetadata.hrl").

-elvis([{elvis_style, atom_naming_convention, disable}]).
%% https://github.com/erlang/otp/issues/9841
-dialyzer(
    {nowarn_function, [
        stream_to_binary/2, binary_to_stream/2, iterator_to_binary/2, binary_to_iterator/2
    ]}
).

-define(SAFE_ERPC(EXPR),
    try
        EXPR
    catch
        error:RPCError__ = {erpc, _} ->
            {error, recoverable, RPCError__};
        %% Note: remote node never _throws_ unrecoverable errors, so
        %% we can assume that all exceptions are transient.
        EC__:RPCError__:Stack__ ->
            {error, recoverable, #{EC__ => RPCError__, stacktrace => Stack__}}
    end
).

-define(SAFE_GEN_RPC(EXPR),
    case EXPR of
        RPCError__ = {badrpc, _} ->
            {error, recoverable, RPCError__};
        RPCRet__ ->
            RPCRet__
    end
).

-define(SHARD_RPC(DB, SHARD, NODE, BODY),
    case
        emqx_ds_builtin_raft_shard:servers(
            DB, SHARD, rpc_target_preference(DB)
        )
    of
        [{_, NODE} | _] ->
            begin
                BODY
            end;
        [] ->
            {error, recoverable, replica_offline}
    end
).

-define(ERR_UPGRADE(NODE, VSN), ?err_rec({node_needs_upgrade, #{node => NODE, api_vsn => VSN}})).

%%================================================================================
%% Type declarations
%%================================================================================

-type shard_id() :: binary().

-type builtin_db_opts() ::
    #{
        backend := builtin_raft,
        store_ttv := boolean(),
        storage := emqx_ds_storage_layer:prototype(),
        reads => leader_preferred | local_preferred | undefined,
        n_shards => pos_integer(),
        n_sites => pos_integer(),
        replication_factor => pos_integer(),
        replication_options => _TODO :: #{},
        %% Equivalent to `append_only' from `emqx_ds:create_db_opts'
        force_monotonic_timestamps => boolean(),
        atomic_batches => boolean(),
        %% Optimistic transaction:
        transaction => emqx_ds_optimistic_tx:runtime_config()
    }.

%% This enapsulates the stream entity from the replication level.
%%
%% TODO: this type is obsolete and is kept only for compatibility with
%% v3 BPAPI. Remove it when emqx_ds_proto_v4 is gone (EMQX 5.6)
-opaque stream_v1() ::
    #{
        ?tag := ?STREAM,
        ?shard := shard_id(),
        ?enc := emqx_ds_storage_layer:stream_v1()
    }.

-define(stream_v2(SHARD, INNER), [2, SHARD | INNER]).
-define(delete_stream(SHARD, INNER), [3, SHARD | INNER]).

-opaque stream() :: nonempty_maybe_improper_list().

-opaque delete_stream() :: nonempty_maybe_improper_list().

-opaque iterator() ::
    #{
        ?tag := ?IT,
        ?shard := shard_id(),
        ?enc := emqx_ds_storage_layer:iterator()
    }.

-opaque delete_iterator() ::
    #{
        ?tag := ?DELETE_IT,
        ?shard := shard_id(),
        ?enc := emqx_ds_storage_layer:delete_iterator()
    }.

%% Write batch.
%% Instances of this type currently form the majority of the Raft log.
-type batch() :: #{
    ?tag := ?BATCH,
    ?batch_operations := [emqx_ds:operation()],
    ?batch_preconditions => [emqx_ds:precondition()]
}.

-opaque tx_context() :: emqx_ds_optimistic_tx:ctx().

-type slab() :: {shard_id(), term()}.

%% Core state of the replication, i.e. the state of ra machine.
-type ra_state() :: #{
    %% Shard ID.
    db_shard := {emqx_ds:db(), shard_id()},

    %% Unique timestamp tracking real time closely.
    %% With microsecond granularity it should be nearly impossible for it to run
    %% too far ahead of the real time clock.
    latest := timestamp_us(),

    %% Transaction serial.
    tx_serial => emqx_ds_optimistic_tx:serial(),

    %% Pid of the OTX leader process:
    otx_leader_pid => pid() | undefined
}.

%% Command. Each command is an entry in the replication log.
-type ra_command() :: #{
    ?tag := ?BATCH | add_generation | update_config | drop_generation | storage_event,
    _ => _
}.

-type timestamp_us() :: non_neg_integer().

-define(gv_timestamp(SHARD), {gv_timestamp, SHARD}).
-define(gv_otx_leader_pid(SHARD), {gv_otx_leader_pid, SHARD}).

%%================================================================================
%% API functions
%%================================================================================

-spec list_shards(emqx_ds:db()) -> [shard_id()].
list_shards(DB) ->
    emqx_ds_builtin_raft_meta:shards(DB).

-spec open_db(emqx_ds:db(), builtin_db_opts()) -> ok | {error, _}.
open_db(DB, CreateOpts0) ->
    %% Rename `append_only' flag to `force_monotonic_timestamps':
    AppendOnly = maps:get(append_only, CreateOpts0),
    CreateOpts1 = maps:put(force_monotonic_timestamps, AppendOnly, CreateOpts0),
    CreateOpts = emqx_utils_maps:deep_merge(
        #{
            transaction => #{
                flush_interval => 1_000,
                idle_flush_interval => 1,
                conflict_window => 5_000
            },
            replication_options => #{},
            n_sites => application:get_env(emqx_ds_builtin_raft, n_sites, 1)
        },
        CreateOpts1
    ),
    case emqx_ds_builtin_raft_sup:start_db(DB, CreateOpts) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            ok;
        {error, Err} ->
            {error, Err}
    end.

-spec close_db(emqx_ds:db()) -> ok.
close_db(DB) ->
    emqx_ds_builtin_raft_sup:stop_db(DB).

-spec add_generation(emqx_ds:db()) -> ok | {error, _}.
add_generation(DB) ->
    add_generation(DB, emqx_ds:timestamp_us()).

-spec add_generation(emqx_ds:db(), emqx_ds:time()) -> ok | {error, _}.
add_generation(DB, Since) ->
    foreach_shard(
        DB,
        fun(Shard) -> ok = ra_add_generation(DB, Shard, Since) end
    ).

-spec update_db_config(emqx_ds:db(), builtin_db_opts()) -> ok | {error, _}.
update_db_config(DB, CreateOpts) ->
    Opts = #{} = emqx_ds_builtin_raft_meta:update_db_config(DB, CreateOpts),
    Since = emqx_ds:timestamp_us(),
    foreach_shard(
        DB,
        fun(Shard) -> ok = ra_update_config(DB, Shard, Opts, Since) end
    ).

-spec list_generations_with_lifetimes(emqx_ds:db()) ->
    #{slab() => emqx_ds:slab_info()}.
list_generations_with_lifetimes(DB) ->
    Shards = list_shards(DB),
    lists:foldl(
        fun(Shard, GensAcc) ->
            case ra_list_generations_with_lifetimes(DB, Shard) of
                Gens = #{} ->
                    ok;
                {error, _Class, _Reason} ->
                    %% TODO: log error
                    Gens = #{}
            end,
            maps:fold(
                fun(GenId, Data, AccInner) ->
                    AccInner#{{Shard, GenId} => Data}
                end,
                GensAcc,
                Gens
            )
        end,
        #{},
        Shards
    ).

-spec drop_slab(emqx_ds:db(), slab()) -> ok | {error, _}.
drop_slab(DB, {Shard, GenId}) ->
    ra_drop_slab(DB, Shard, GenId).

-spec drop_db(emqx_ds:db()) -> ok | {error, _}.
drop_db(DB) ->
    foreach_shard(DB, fun(Shard) ->
        {ok, _} = ra_drop_shard(DB, Shard)
    end),
    _ = emqx_ds_proto_v6:drop_db(list_nodes(), DB),
    emqx_ds_builtin_raft_meta:drop_db(DB).

-spec store_batch(emqx_ds:db(), emqx_ds:batch(), emqx_ds:message_store_opts()) ->
    emqx_ds:store_batch_result().
store_batch(DB, Batch = #dsbatch{preconditions = [_ | _]}, Opts) ->
    %% NOTE: Atomic batch is implied, will not check with DB config.
    store_batch_atomic(DB, Batch, Opts);
store_batch(DB, Batch, Opts) ->
    case emqx_ds_builtin_raft_meta:db_config(DB) of
        #{atomic_batches := true} ->
            store_batch_atomic(DB, Batch, Opts);
        #{} ->
            store_batch_buffered(DB, Batch, Opts)
    end.

store_batch_buffered(DB, #dsbatch{operations = Operations}, Opts) ->
    store_batch_buffered(DB, Operations, Opts);
store_batch_buffered(DB, Batch, Opts) ->
    try
        emqx_ds_buffer:store_batch(DB, Batch, Opts)
    catch
        error:{Reason, _Call} when Reason == timeout; Reason == noproc ->
            {error, recoverable, Reason}
    end.

store_batch_atomic(DB, Batch, _Opts) ->
    Shards = shards_of_batch(DB, Batch),
    case Shards of
        [Shard] ->
            case ra_store_batch(DB, Shard, Batch) of
                {timeout, ServerId} ->
                    {error, recoverable, {timeout, ServerId}};
                Result ->
                    Result
            end;
        [] ->
            ok;
        [_ | _] ->
            {error, unrecoverable, atomic_batch_spans_multiple_shards}
    end.

-spec get_streams(emqx_ds:db(), emqx_ds:topic_filter(), emqx_ds:time(), emqx_ds:get_streams_opts()) ->
    emqx_ds:get_streams_result().
get_streams(DB, TopicFilter, StartTime, Opts) ->
    Shards =
        case Opts of
            #{shard := ReqShard} ->
                [ReqShard];
            _ ->
                list_shards(DB)
        end,
    MinGeneration = maps:get(generation_min, Opts, 0),
    lists:foldl(
        fun(Shard, {Acc, AccErr}) ->
            try ra_get_streams(DB, Shard, TopicFilter, StartTime, MinGeneration) of
                Streams when is_list(Streams) ->
                    L = lists:map(
                        fun
                            (#'Stream'{generation = Generation} = Stream) ->
                                Slab = {Shard, Generation},
                                {Slab, Stream};
                            ({Generation, StorageLayerStream}) ->
                                Slab = {Shard, Generation},
                                {Slab, ?stream_v2(Shard, StorageLayerStream)}
                        end,
                        Streams
                    ),
                    {L ++ Acc, AccErr};
                {error, _, _} = Err ->
                    E = {Shard, Err},
                    {Acc, [E | AccErr]}
            catch
                EC:Err:Stack ->
                    E = {Shard, ?err_rec({EC, Err, Stack})},
                    {Acc, [E | AccErr]}
            end
        end,
        {[], []},
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
make_iterator(DB, Stream = #'Stream'{shard = Shard}, TopicFilter, StartTime) ->
    ra_make_iterator_ttv(DB, Shard, Stream, TopicFilter, StartTime);
make_iterator(DB, ?stream_v2(Shard, StorageStream), TopicFilter, StartTime) ->
    case ra_make_iterator(DB, Shard, StorageStream, TopicFilter, StartTime) of
        {ok, Iter} ->
            {ok, #{?tag => ?IT, ?shard => Shard, ?enc => Iter}};
        Error = {error, _, _} ->
            Error
    end.

-spec make_delete_iterator(emqx_ds:db(), delete_stream(), emqx_ds:topic_filter(), emqx_ds:time()) ->
    emqx_ds:make_delete_iterator_result(delete_iterator()).
make_delete_iterator(DB, Stream, TopicFilter, StartTime) ->
    ?delete_stream(Shard, StorageStream) = Stream,
    case ra_make_delete_iterator(DB, Shard, StorageStream, TopicFilter, StartTime) of
        {ok, Iter} ->
            {ok, #{?tag => ?DELETE_IT, ?shard => Shard, ?enc => Iter}};
        Error = {error, _, _} ->
            Error
    end.

-spec next(emqx_ds:db(), iterator(), emqx_ds:next_limit()) ->
    emqx_ds:next_result(iterator()).
next(DB, Iter = #'Iterator'{shard = Shard}, Limit) ->
    T0 = erlang:monotonic_time(microsecond),
    Result = ra_next_ttv(DB, Shard, Iter, Limit),
    T1 = erlang:monotonic_time(microsecond),
    emqx_ds_builtin_metrics:observe_next_time(DB, T1 - T0),
    Result;
next(DB, Iter0, BatchSize) when is_map(Iter0) ->
    #{?tag := ?IT, ?shard := Shard, ?enc := StorageIter0} = Iter0,
    %% TODO: iterator can contain information that is useful for
    %% reconstructing messages sent over the network. For example,
    %% when we send messages with the learned topic index, we could
    %% send the static part of topic once, and append it to the
    %% messages on the receiving node, hence saving some network.
    %%
    %% This kind of trickery should be probably done here in the
    %% replication layer. Or, perhaps, in the logic layer.
    T0 = erlang:monotonic_time(microsecond),
    Result = ra_next(DB, Shard, StorageIter0, BatchSize),
    T1 = erlang:monotonic_time(microsecond),
    emqx_ds_builtin_metrics:observe_next_time(DB, T1 - T0),
    case Result of
        {ok, StorageIter, Batch} ->
            Iter = Iter0#{?enc := StorageIter},
            {ok, Iter, Batch};
        Other ->
            Other
    end.

-spec subscribe(emqx_ds:db(), iterator(), emqx_ds:sub_opts()) ->
    {ok, emqx_ds:subscription_handle(), emqx_ds:sub_ref()} | emqx_ds:error(_).
subscribe(DB, It, SubOpts) ->
    case It of
        #{?tag := ?IT, ?shard := Shard} -> ok;
        #'Iterator'{shard = Shard} -> ok
    end,
    ?SHARD_RPC(
        DB,
        Shard,
        Node,
        case beam_proto_vsn(Node) of
            Vsn when Vsn >= 1 ->
                ra_subscribe(Node, DB, Shard, It, SubOpts);
            Vsn ->
                ?ERR_UPGRADE(Node, Vsn)
        end
    ).

ra_subscribe(Node, DB, Shard, It, SubOpts) ->
    try emqx_ds_beamformer_proto_v1:where(Node, {DB, Shard}) of
        Server when is_pid(Server) ->
            MRef = monitor(process, Server),
            Result = ?SAFE_ERPC(
                emqx_ds_beamformer_proto_v1:subscribe(
                    Node, Server, self(), MRef, It, SubOpts
                )
            ),
            case Result of
                {ok, MRef} ->
                    {ok, #sub_handle{shard = Shard, server = Server, ref = MRef}, MRef};
                Err ->
                    Err
            end;
        undefined ->
            ?err_rec(beamformer_is_not_started);
        Err ->
            Err
    catch
        EC:Err:Stack ->
            ?err_rec(#{EC => Err, stacktrace => Stack})
    end.

-spec unsubscribe(emqx_ds:db(), emqx_ds:subscription_handle()) -> boolean().
unsubscribe(DB, #sub_handle{shard = Shard, server = Server, ref = SubRef}) ->
    Node = node(Server),
    case beam_proto_vsn(Node) of
        Vsn when Vsn >= 1 ->
            ?SAFE_ERPC(
                emqx_ds_beamformer_proto_v1:unsubscribe(Node, {DB, Shard}, SubRef)
            );
        Vsn ->
            ?ERR_UPGRADE(Node, Vsn)
    end.

-spec suback(emqx_ds:db(), emqx_ds:subscription_handle(), emqx_ds:sub_seqno()) ->
    ok.
suback(DB, #sub_handle{shard = Shard, server = Server, ref = SubRef}, SeqNo) ->
    Node = node(Server),
    case beam_proto_vsn(Node) of
        Vsn when Vsn >= 1 ->
            ?SAFE_ERPC(
                emqx_ds_beamformer_proto_v1:suback_a(Node, {DB, Shard}, SubRef, SeqNo)
            );
        Vsn ->
            ?ERR_UPGRADE(Node, Vsn)
    end.

-spec subscription_info(emqx_ds:db(), emqx_ds:subscription_handle()) ->
    emqx_ds:sub_info() | undefined.
subscription_info(DB, #sub_handle{shard = Shard, server = Server, ref = SubRef}) ->
    Node = node(Server),
    case beam_proto_vsn(Node) of
        Vsn when Vsn >= 1 ->
            ?SAFE_ERPC(
                emqx_ds_beamformer_proto_v1:subscription_info(Node, {DB, Shard}, SubRef)
            );
        Vsn ->
            ?ERR_UPGRADE(Node, Vsn)
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

-spec foreach_shard(emqx_ds:db(), fun((shard_id()) -> _)) -> ok.
foreach_shard(DB, Fun) ->
    lists:foreach(Fun, list_shards(DB)).

%% @doc Messages have been replicated up to this timestamp on the
%% local server
-spec current_timestamp(emqx_ds:db(), shard_id()) -> emqx_ds:time().
current_timestamp(DB, Shard) ->
    emqx_ds_builtin_raft_sup:get_gvar(DB, ?gv_timestamp(Shard), 0).

-spec new_tx(emqx_ds:db(), emqx_ds:transaction_opts()) ->
    {ok, tx_context()} | emqx_ds:error(_).
new_tx(DB, Options = #{shard := ShardOpt, generation := Generation}) ->
    case emqx_ds_builtin_raft_meta:db_config(DB) of
        #{store_ttv := true} ->
            case ShardOpt of
                {auto, Owner} ->
                    Shard = shard_of(DB, Owner);
                Shard ->
                    ok
            end,
            ?SHARD_RPC(
                DB,
                Shard,
                Node,
                case otx_proto_vsn(Node) of
                    Vsn when Vsn >= 1 ->
                        ?SAFE_ERPC(
                            emqx_ds_otx_proto_v1:new_kv_tx_ctx(Node, DB, Shard, Generation, Options)
                        );
                    Vsn ->
                        ?ERR_UPGRADE(Node, Vsn)
                end
            );
        _ ->
            ?err_unrec(database_does_not_support_transactions)
    end.

commit_tx(DB, Ctx, Ops) ->
    %% NOTE: pid of the leader is stored in the context, this should
    %% work for remote processes too.
    emqx_ds_optimistic_tx:commit_kv_tx(DB, Ctx, Ops).

tx_commit_outcome(Reply) ->
    emqx_ds_optimistic_tx:tx_commit_outcome(Reply).

%%================================================================================
%% emqx_ds_buffer callbacks
%%================================================================================

-record(bs, {}).
-type egress_state() :: #bs{}.

-spec init_buffer(emqx_ds:db(), shard_id(), _Options) -> {ok, egress_state()}.
init_buffer(_DB, _Shard, _Options) ->
    {ok, #bs{}}.

-spec flush_buffer(emqx_ds:db(), shard_id(), [emqx_types:message()], egress_state()) ->
    {egress_state(), ok | emqx_ds:error(_)}.
flush_buffer(DB, Shard, Messages, State) ->
    case ra_store_batch(DB, Shard, Messages) of
        {timeout, ServerId} ->
            Result = {error, recoverable, {timeout, ServerId}};
        Result ->
            ok
    end,
    {State, Result}.

-spec shard_of_operation(
    emqx_ds:db(),
    emqx_ds:operation() | emqx_ds:precondition(),
    clientid | topic,
    _Options
) ->
    shard_id().
shard_of_operation(DB, #message{from = From, topic = Topic}, SerializeBy, _Options) ->
    case SerializeBy of
        clientid -> Key = From;
        topic -> Key = Topic
    end,
    shard_of(DB, Key);
shard_of_operation(DB, {_OpName, Matcher}, SerializeBy, _Options) ->
    #message_matcher{from = From, topic = Topic} = Matcher,
    case SerializeBy of
        clientid -> Key = From;
        topic -> Key = Topic
    end,
    shard_of(DB, Key).

shard_of(DB, Key) ->
    N = emqx_ds_builtin_raft_shard_allocator:n_shards(DB),
    Hash = erlang:phash2(Key, N),
    integer_to_binary(Hash).

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

stream_to_binary(_DB, Stream = #'Stream'{}) ->
    'DSBuiltinMetadata':encode('Stream', Stream);
stream_to_binary(DB, ?stream_v2(Shard, Inner)) ->
    stream_to_binary(DB, emqx_ds_storage_layer:old_stream_to_new(Shard, Inner)).

binary_to_stream(DB, Bin) ->
    maybe
        {ok, Stream} ?= 'DSBuiltinMetadata':decode('Stream', Bin),
        case emqx_ds_builtin_raft_meta:db_config(DB) of
            #{store_ttv := true} ->
                {ok, Stream};
            #{store_ttv := false} ->
                case emqx_ds_storage_layer:new_stream_to_old(Stream) of
                    {ok, Shard, Inner} ->
                        {ok, ?stream_v2(Shard, Inner)};
                    {error, _} = Err ->
                        Err
                end
        end
    end.

iterator_to_binary(_DB, end_of_stream) ->
    'DSBuiltinMetadata':encode('ReplayPosition', {endOfStream, 'NULL'});
iterator_to_binary(_DB, It = #'Iterator'{}) ->
    'DSBuiltinMetadata':encode('ReplayPosition', {value, It});
iterator_to_binary(_DB, #{?tag := ?IT, ?shard := Shard, ?enc := Inner}) ->
    It = emqx_ds_storage_layer:old_iterator_to_new(Shard, Inner),
    'DSBuiltinMetadata':encode('ReplayPosition', {value, It}).

binary_to_iterator(DB, Bin) ->
    case 'DSBuiltinMetadata':decode('ReplayPosition', Bin) of
        {ok, {endOfStream, 'NULL'}} ->
            {ok, end_of_stream};
        {ok, {value, It}} ->
            case emqx_ds_builtin_raft_meta:db_config(DB) of
                #{store_ttv := true} ->
                    {ok, It};
                #{store_ttv := false} ->
                    case emqx_ds_storage_layer:new_iterator_to_old(It) of
                        {ok, Shard, Inner} ->
                            {ok, #{?tag => ?IT, ?shard => Shard, ?enc => Inner}};
                        Err ->
                            Err
                    end
            end
    end.

%%================================================================================
%% emqx_ds_optimistic_tx callbacks
%%================================================================================

otx_get_tx_serial(DB, Shard) ->
    emqx_ds_storage_layer_ttv:get_read_tx_serial({DB, Shard}).

otx_get_leader(DB, Shard) ->
    emqx_ds_builtin_raft_sup:get_gvar(DB, ?gv_otx_leader_pid(Shard), undefined).

otx_become_leader(DB, Shard) ->
    Command = #{?tag => new_otx_leader, ?otx_leader_pid => self()},
    case local_raft_leader(DB, Shard) of
        unknown ->
            ?err_rec(leader_unavailable);
        Leader ->
            case ra:process_command(Leader, Command, 5_000) of
                {ok, {Serial, Timestamp}, _Leader} ->
                    {ok, Serial, Timestamp};
                Err ->
                    ?err_unrec({raft, Err})
            end
    end.

-spec otx_prepare_tx(
    {emqx_ds:db(), emqx_ds:shard()},
    emqx_ds:generation(),
    _SerialBin :: binary(),
    emqx_ds:tx_ops(),
    _MiscOpts :: map()
) ->
    {ok, _CookedTx} | emqx_ds:error(_).
otx_prepare_tx(DBShard, Generation, SerialBin, Ops, Opts) ->
    emqx_ds_storage_layer_ttv:prepare_tx(DBShard, Generation, SerialBin, Ops, Opts).

otx_commit_tx_batch({DB, Shard}, SerCtl, Serial, Timestamp, Batches) ->
    Command = #{
        ?tag => commit_tx_batch,
        ?prev_serial => SerCtl,
        ?serial => Serial,
        ?otx_timestamp => Timestamp,
        ?batches => Batches,
        ?otx_leader_pid => self()
    },
    case local_raft_leader(DB, Shard) of
        unknown ->
            ?err_rec(leader_unavailable);
        Leader ->
            case ra:process_command(Leader, Command, 5_000) of
                {ok, ok, _Leader} ->
                    ok;
                {ok, Err, _Leader} ->
                    Err;
                Err ->
                    ?err_unrec({raft, Err})
            end
    end.

otx_lookup_ttv(DBShard, GenId, Topic, Timestamp) ->
    emqx_ds_storage_layer_ttv:lookup(DBShard, GenId, Topic, Timestamp).

otx_get_runtime_config(DB) ->
    #{transaction := Val} = emqx_ds_builtin_raft_meta:db_config(DB),
    Val.

%%================================================================================
%% Internal exports (RPC targets)
%%================================================================================

%% NOTE
%% Target node may still be in the process of starting up when RPCs arrive, it's
%% good to have them handled gracefully.
%% TODO
%% There's a possibility of race condition: storage may shut down right after we
%% ask for its status.
-define(IF_SHARD_READY(SHARDID, EXPR),
    case emqx_ds_builtin_raft_db_sup:shard_info(SHARDID, ready) of
        true -> EXPR;
        _Unready -> {error, recoverable, shard_unavailable}
    end
).

-spec do_drop_db_v1(emqx_ds:db()) -> ok | {error, _}.
do_drop_db_v1(DB) ->
    MyShards = emqx_ds_builtin_raft_meta:my_shards(DB),
    emqx_ds_builtin_raft_sup:stop_db(DB),
    lists:foreach(
        fun(Shard) ->
            emqx_ds_storage_layer:drop_shard({DB, Shard})
        end,
        MyShards
    ).

-spec do_store_batch_v1(
    emqx_ds:db(),
    shard_id(),
    batch(),
    emqx_ds:message_store_opts()
) ->
    no_return().
do_store_batch_v1(_DB, _Shard, _Batch, _Options) ->
    error(obsolete_api).

-spec do_get_streams_v2(
    emqx_ds:db(),
    shard_id(),
    emqx_ds:topic_filter(),
    emqx_ds:time()
) ->
    [{integer(), emqx_ds_storage_layer:stream()}] | emqx_ds:error(storage_down).
do_get_streams_v2(DB, Shard, TopicFilter, StartTimeUs) ->
    DBShard = {DB, Shard},
    MinGeneration = 0,
    ?IF_SHARD_READY(
        DBShard,
        begin
            #{store_ttv := IsTTV} = emqx_ds_builtin_raft_meta:db_config(DB),
            case IsTTV of
                false ->
                    emqx_ds_storage_layer:get_streams(
                        DBShard, TopicFilter, StartTimeUs, MinGeneration
                    );
                true ->
                    emqx_ds_storage_layer_ttv:get_streams(
                        DBShard, TopicFilter, StartTimeUs, MinGeneration
                    )
            end
        end
    ).

-spec do_get_streams_v3(
    emqx_ds:db(),
    shard_id(),
    emqx_ds:topic_filter(),
    emqx_ds:time(),
    emqx_ds:generation()
) ->
    [{emqx_ds:generation(), emqx_ds_storage_layer:stream() | emqx_ds_storage_layer_ttv:stream()}]
    | emqx_ds:error(storage_down).
do_get_streams_v3(DB, Shard, TopicFilter, StartTime, MinGeneration) ->
    DBShard = {DB, Shard},
    ?IF_SHARD_READY(
        DBShard,
        begin
            #{store_ttv := IsTTV} = emqx_ds_builtin_raft_meta:db_config(DB),
            case IsTTV of
                false ->
                    emqx_ds_storage_layer:get_streams(
                        DBShard, TopicFilter, timestamp_to_timeus(StartTime), MinGeneration
                    );
                true ->
                    emqx_ds_storage_layer_ttv:get_streams(
                        DBShard, TopicFilter, StartTime, MinGeneration
                    )
            end
        end
    ).

-spec do_make_iterator_v2(
    emqx_ds:db(),
    shard_id(),
    emqx_ds_storage_layer:stream() | emqx_ds_storage_layer_ttv:stream(),
    emqx_ds:topic_filter(),
    emqx_ds:time()
) ->
    emqx_ds:make_iterator_result(emqx_ds_storage_layer:iterator()).
do_make_iterator_v2(DB, Shard, Stream = #'Stream'{}, TopicFilter, StartTime) ->
    do_make_iterator_ttv_v1(DB, Shard, Stream, TopicFilter, StartTime);
do_make_iterator_v2(DB, Shard, Stream, TopicFilter, StartTime) ->
    ShardId = {DB, Shard},
    ?IF_SHARD_READY(
        ShardId,
        emqx_ds_storage_layer:make_iterator(ShardId, Stream, TopicFilter, StartTime)
    ).

-spec do_make_iterator_v3(
    emqx_ds:db(),
    shard_id(),
    emqx_ds_storage_layer:stream(),
    emqx_ds:topic_filter(),
    emqx_ds:time()
) ->
    emqx_ds:make_iterator_result(emqx_ds_storage_layer:iterator()).
do_make_iterator_v3(DB, Shard, Stream, TopicFilter, StartTime) ->
    ShardId = {DB, Shard},
    ?IF_SHARD_READY(
        ShardId,
        emqx_ds_storage_layer:make_iterator(
            ShardId, Stream, TopicFilter, timestamp_to_timeus(StartTime)
        )
    ).

-spec do_make_iterator_ttv_v1(
    emqx_ds:db(),
    shard_id(),
    emqx_ds_storage_layer_ttv:stream(),
    emqx_ds:topic_filter(),
    emqx_ds:time()
) ->
    emqx_ds:make_iterator_result(emqx_ds_storage_layer_ttv:iterator()).
do_make_iterator_ttv_v1(DB, Shard, Stream = #'Stream'{}, TopicFilter, StartTime) ->
    ShardId = {DB, Shard},
    ?IF_SHARD_READY(
        ShardId,
        emqx_ds_storage_layer_ttv:make_iterator(
            DB, Stream, TopicFilter, StartTime
        )
    ).

-spec do_make_delete_iterator_v4(
    emqx_ds:db(),
    shard_id(),
    emqx_ds_storage_layer:delete_stream(),
    emqx_ds:topic_filter(),
    emqx_ds:time()
) ->
    {ok, emqx_ds_storage_layer:delete_iterator()} | {error, _}.
do_make_delete_iterator_v4(DB, Shard, Stream, TopicFilter, StartTime) ->
    emqx_ds_storage_layer:make_delete_iterator({DB, Shard}, Stream, TopicFilter, StartTime).

%% Backward-compatibility with v5.
-spec do_update_iterator_v2(
    emqx_ds:db(),
    shard_id(),
    emqx_ds_storage_layer:iterator(),
    emqx_ds:message_key()
) ->
    emqx_ds:make_iterator_result(emqx_ds_storage_layer:iterator()).
do_update_iterator_v2(DB, Shard, OldIter, DSKey) ->
    emqx_ds_storage_layer:update_iterator({DB, Shard}, OldIter, DSKey).

%% Backward-compatible version that returns DSKeys. TODO: Remove
-spec do_next_v1(
    emqx_ds:db(),
    shard_id(),
    emqx_ds_storage_layer:iterator(),
    pos_integer()
) ->
    _.
do_next_v1(DB, Shard, Iter, NextLimit) ->
    DBShard = {DB, Shard},
    ?IF_SHARD_READY(
        DBShard,
        begin
            {BatchSize, TimeLimit} = batch_size_and_time_limit(false, DB, Shard, NextLimit),
            emqx_ds_storage_layer:next(DBShard, Iter, BatchSize, TimeLimit, true)
        end
    ).

-spec do_next_v2(
    emqx_ds:db(),
    shard_id(),
    emqx_ds_storage_layer:iterator(),
    emqx_ds:next_limit()
) ->
    emqx_ds:next_result(emqx_ds_storage_layer:iterator()).
do_next_v2(DB, Shard, Iter, NextLimit) ->
    DBShard = {DB, Shard},
    ?IF_SHARD_READY(
        DBShard,
        begin
            {BatchSize, TimeLimit} = batch_size_and_time_limit(false, DB, Shard, NextLimit),
            emqx_ds_storage_layer:next(DBShard, Iter, BatchSize, TimeLimit, false)
        end
    ).

-spec do_next_ttv(
    emqx_ds:db(),
    emqx_ds_storage_layer_ttv:iterator(),
    pos_integer()
) ->
    emqx_ds:next_result(emqx_ds_storage_layer_ttv:iterator()).
do_next_ttv(DB, Iter = #'Iterator'{shard = Shard}, NextLimit) ->
    ?IF_SHARD_READY(
        {DB, Shard},
        begin
            {BatchSize, TimeLimit} = batch_size_and_time_limit(true, DB, Shard, NextLimit),
            emqx_ds_storage_layer_ttv:next(DB, Iter, BatchSize, TimeLimit)
        end
    ).

-spec do_delete_next_v4(
    emqx_ds:db(),
    shard_id(),
    emqx_ds_storage_layer:delete_iterator(),
    emqx_ds:delete_selector(),
    pos_integer()
) ->
    emqx_ds:delete_next_result(emqx_ds_storage_layer:delete_iterator()).
do_delete_next_v4(DB, Shard, Iter, Selector, BatchSize) ->
    emqx_ds_storage_layer:delete_next(
        {DB, Shard},
        Iter,
        Selector,
        BatchSize,
        current_timestamp(DB, Shard)
    ).

-spec do_add_generation_v2(emqx_ds:db()) -> no_return().
do_add_generation_v2(_DB) ->
    error(obsolete_api).

-spec do_list_generations_with_lifetimes_v3(emqx_ds:db(), shard_id()) ->
    #{emqx_ds:generation() => emqx_ds:slab_info()}
    | emqx_ds:error(storage_down).
do_list_generations_with_lifetimes_v3(DB, Shard) ->
    ShardId = {DB, Shard},
    ?IF_SHARD_READY(
        ShardId,
        emqx_ds_storage_layer:list_generations_with_lifetimes(ShardId)
    ).

-spec do_drop_generation_v3(emqx_ds:db(), shard_id(), emqx_ds_storage_layer:gen_id()) ->
    no_return().
do_drop_generation_v3(_DB, _ShardId, _GenId) ->
    error(obsolete_api).

-spec do_get_delete_streams_v4(
    emqx_ds:db(), shard_id(), emqx_ds:topic_filter(), emqx_ds:time()
) ->
    [emqx_ds_storage_layer:delete_stream()].
do_get_delete_streams_v4(DB, Shard, TopicFilter, StartTime) ->
    emqx_ds_storage_layer:get_delete_streams({DB, Shard}, TopicFilter, StartTime).

%% TODO: remove
-spec do_poll_v1(
    node(),
    emqx_ds:db(),
    shard_id(),
    [{emqx_ds_beamformer:return_addr(_), emqx_ds_storage_layer:iterator()}],
    emqx_ds:poll_opts()
) ->
    _.
do_poll_v1(_SourceNode, _DB, _Shard, _Iterators, _PollOpts) ->
    ?err_rec(obsolete_client_api).

-spec do_new_kv_tx_ctx_v1(
    emqx_ds:db(),
    emqx_ds:shard(),
    emqx_ds:generation(),
    emqx_ds:transaction_opts()
) -> {ok, tx_context()} | emqx_ds:error(_).
do_new_kv_tx_ctx_v1(DB, Shard, Generation, Opts) ->
    emqx_ds_optimistic_tx:new_kv_tx_ctx(?MODULE, DB, Shard, Generation, Opts).

%%================================================================================
%% Internal functions
%%================================================================================

list_nodes() ->
    mria:running_nodes().

%% TODO
%% Too large for normal operation, need better backpressure mechanism.
-define(RA_TIMEOUT, 60 * 1000).

%% How often to release Raft logs?
%% Each time we written approximately this number of bytes.
%% Close to the RocksDB's default of 64 MiB.
-define(RA_RELEASE_LOG_APPROX_SIZE, 50_000_000).
%% ...Or at least each N log entries.
-define(RA_RELEASE_LOG_MIN_FREQ, 64_000).

-ifdef(TEST).
-undef(RA_RELEASE_LOG_APPROX_SIZE).
-undef(RA_RELEASE_LOG_MIN_FREQ).
-define(RA_RELEASE_LOG_APPROX_SIZE, 50_000).
-define(RA_RELEASE_LOG_MIN_FREQ, 1_000).
-endif.

-spec ra_store_batch(emqx_ds:db(), shard_id(), emqx_ds:batch()) ->
    ok | {timeout, _} | emqx_ds:error(_).
ra_store_batch(DB, Shard, Batch) ->
    case Batch of
        #dsbatch{operations = Operations, preconditions = Preconditions} ->
            Command = #{
                ?tag => ?BATCH,
                ?batch_operations => Operations,
                ?batch_preconditions => Preconditions
            };
        Operations ->
            Command = #{
                ?tag => ?BATCH,
                ?batch_operations => Operations
            }
    end,
    Servers = emqx_ds_builtin_raft_shard:servers(DB, Shard, leader_preferred),
    case emqx_ds_builtin_raft_shard:process_command(Servers, Command, ?RA_TIMEOUT) of
        {ok, Result, _Leader} ->
            Result;
        {timeout, _} = Timeout ->
            Timeout;
        {error, Reason = servers_unreachable} ->
            {error, recoverable, Reason}
    end.

ra_add_generation(DB, Shard, Since) ->
    Command = #{
        ?tag => add_generation,
        ?since => Since
    },
    ra_command(DB, Shard, Command, 10).

ra_update_config(DB, Shard, Opts, Since) ->
    Command = #{
        ?tag => update_config,
        ?config => Opts,
        ?since => Since
    },
    ra_command(DB, Shard, Command, 10).

ra_drop_slab(DB, Shard, GenId) ->
    Command = #{?tag => drop_generation, ?generation => GenId},
    ra_command(DB, Shard, Command, 10).

ra_command(DB, Shard, Command, Retries) ->
    Servers = emqx_ds_builtin_raft_shard:servers(DB, Shard, leader_preferred),
    case ra:process_command(Servers, Command, ?RA_TIMEOUT) of
        {ok, Result, _Leader} ->
            Result;
        _Error when Retries > 0 ->
            timer:sleep(?RA_TIMEOUT),
            ra_command(DB, Shard, Command, Retries - 1);
        Error ->
            error(Error, [DB, Shard])
    end.

ra_get_streams(DB, Shard, TopicFilter, Time, MinGeneration) ->
    ?SHARD_RPC(
        DB,
        Shard,
        Node,
        case proto_vsn(Node) of
            Vsn when Vsn >= 6 ->
                ?SAFE_ERPC(
                    emqx_ds_proto_v6:get_streams(Node, DB, Shard, TopicFilter, Time, MinGeneration)
                );
            Vsn when Vsn >= 4 ->
                %% Use timestamp conversion and polyfill `min_generation' filtering:
                TimestampUs = timestamp_to_timeus(Time),
                ?SAFE_ERPC(
                    polyfill_filter_streams(
                        MinGeneration,
                        emqx_ds_proto_v4:get_streams(Node, DB, Shard, TopicFilter, TimestampUs)
                    )
                );
            Vsn ->
                ?ERR_UPGRADE(Node, Vsn)
        end
    ).

polyfill_filter_streams(MinGeneration, L) ->
    lists:filter(
        fun({G, _}) ->
            G >= MinGeneration
        end,
        L
    ).

ra_get_delete_streams(DB, Shard, TopicFilter, Time) ->
    ?SHARD_RPC(
        DB,
        Shard,
        Node,
        case proto_vsn(Node) of
            Vsn when Vsn >= 4 ->
                ?SAFE_ERPC(emqx_ds_proto_v6:get_delete_streams(Node, DB, Shard, TopicFilter, Time));
            Vsn ->
                ?ERR_UPGRADE(Node, Vsn)
        end
    ).

ra_make_iterator_ttv(DB, Shard, Stream = #'Stream'{}, TopicFilter, StartTime) ->
    ?SHARD_RPC(
        DB,
        Shard,
        Node,
        case proto_vsn(Node) of
            Vsn when Vsn >= 6 ->
                ?SAFE_ERPC(
                    emqx_ds_proto_v6:make_iterator_ttv(
                        Node, DB, Shard, Stream, TopicFilter, StartTime
                    )
                );
            Vsn ->
                ?ERR_UPGRADE(Node, Vsn)
        end
    ).

ra_make_iterator(DB, Shard, Stream, TopicFilter, StartTime) ->
    ?SHARD_RPC(
        DB,
        Shard,
        Node,
        case proto_vsn(Node) of
            Vsn when Vsn >= 6 ->
                ?SAFE_ERPC(
                    emqx_ds_proto_v6:make_iterator(Node, DB, Shard, Stream, TopicFilter, StartTime)
                );
            Vsn when Vsn >= 4 ->
                ?SAFE_ERPC(
                    emqx_ds_proto_v4:make_iterator(
                        Node, DB, Shard, Stream, TopicFilter, timestamp_to_timeus(StartTime)
                    )
                );
            Vsn ->
                ?ERR_UPGRADE(Node, Vsn)
        end
    ).

ra_make_delete_iterator(DB, Shard, Stream, TopicFilter, StartTime) ->
    TimeUs = timestamp_to_timeus(StartTime),
    ?SHARD_RPC(
        DB,
        Shard,
        Node,
        case proto_vsn(Node) of
            Vsn when Vsn >= 4 ->
                ?SAFE_ERPC(
                    emqx_ds_proto_v4:make_delete_iterator(
                        Node, DB, Shard, Stream, TopicFilter, TimeUs
                    )
                );
            Vsn ->
                ?ERR_UPGRADE(Node, Vsn)
        end
    ).

ra_next(DB, Shard, Iter, NextLimit) ->
    ?SHARD_RPC(
        DB,
        Shard,
        Node,
        case proto_vsn(Node) of
            Vsn when Vsn >= 6 ->
                ?SAFE_GEN_RPC(emqx_ds_proto_v6:next(Node, DB, Shard, Iter, NextLimit));
            Vsn when Vsn >= 4, is_integer(NextLimit) ->
                %% Limiting iteration by time is not supported.
                %%
                %% Polyfill: get rid of DSKeys.
                ?SAFE_GEN_RPC(
                    maybe
                        {ok, It, Batch} ?= emqx_ds_proto_v4:next(Node, DB, Shard, Iter, NextLimit),
                        {ok, It, emqx_ds_storage_layer:rid_of_dskeys(Batch)}
                    end
                );
            Vsn ->
                ?ERR_UPGRADE(Node, Vsn)
        end
    ).

ra_next_ttv(DB, Shard, Iter = #'Iterator'{shard = Shard}, NextLimit) ->
    ?SHARD_RPC(
        DB,
        Shard,
        Node,
        case proto_vsn(Node) of
            Vsn when Vsn >= 6 ->
                ?SAFE_GEN_RPC(
                    emqx_ds_proto_v6:next_ttv(Node, DB, Shard, Iter, NextLimit)
                );
            Vsn ->
                ?ERR_UPGRADE(Node, Vsn)
        end
    ).

ra_delete_next(DB, Shard, Iter, Selector, BatchSize) ->
    ?SHARD_RPC(
        DB,
        Shard,
        Node,
        case proto_vsn(Node) of
            Vsn when Vsn >= 4 ->
                ?SAFE_ERPC(
                    emqx_ds_proto_v4:delete_next(Node, DB, Shard, Iter, Selector, BatchSize)
                );
            Vsn ->
                ?ERR_UPGRADE(Node, Vsn)
        end
    ).

ra_list_generations_with_lifetimes(DB, Shard) ->
    Reply = ?SHARD_RPC(
        DB,
        Shard,
        Node,
        case proto_vsn(Node) of
            Vsn when Vsn >= 6 ->
                ?SAFE_ERPC(emqx_ds_proto_v6:list_generations_with_lifetimes(Node, DB, Shard));
            Vsn ->
                ?ERR_UPGRADE(Node, Vsn)
        end
    ),
    case Reply of
        Gens = #{} ->
            maps:map(
                fun(_GenId, Data = #{since := Since, until := Until}) ->
                    Data#{
                        since := timeus_to_timestamp(Since),
                        until := emqx_maybe:apply(fun timeus_to_timestamp/1, Until)
                    }
                end,
                Gens
            );
        Error ->
            Error
    end.

ra_drop_shard(DB, Shard) ->
    ra:delete_cluster(emqx_ds_builtin_raft_shard:shard_servers(DB, Shard), ?RA_TIMEOUT).

%% Ra Machine implementation
%%
%% This code decides how successfully replicated and committed log entries (e.g.
%% commands) are applied to the shard storage state. This state is actually comprised
%% logically of 2 parts:
%% 1. RocksDB database managed through `emqx_ds_storage_layer`.
%% 2. Machine state (`ra_state()`) that holds very minimal state needed to ensure
%%    higher-level semantics, most importantly strictly monotonic quasi-wallclock
%%    timestamp used to assign unique message timestamps to fulfill "append-only"
%%    guarantees.
%%
%% There are few subtleties in how storage state is persisted and recovered.
%% When the shard recovers from a shutdown or crash, this is what usually happens:
%% 1. Shard storage layer starts up the RocksDB database.
%% 2. Ra recovers the Raft log.
%% 3. Ra recovers the latest machine snapshot (`ra_state()`), taken at some point
%%    in time (`RaftIdx`).
%% 4. Ra applies existing Raft log entries starting from `RaftIdx`.
%%
%% While most of the time storage layer state, machine snapshot and log entries are
%% consistent with each other, there are situations when they are not. Namely:
%%  * RocksDB decides to flush memtables to disk by itself, which is unexpected but
%%    possible.
%%  * Lagging replica accepts a storage snapshot sourced from a RocksDB checkpoint,
%%    and RocksDB database is always implicitly flushed before checkpointing.
%% In both of those cases, the Raft log would contain entries that were already
%% applied from the point of view of the storage layer, and we must anticipate that.
%%
%% The process running Ra machine also keeps auxiliary ephemeral state in the process
%% dictionary, see `?pd_ra_*` macrodefs for details.

%% Index of the last yet unreleased Ra log entry.
-define(pd_ra_idx_need_release, '$emqx_ds_raft_idx_need_release').

%% Approximate number of bytes occupied by yet unreleased Ra log entries.
-define(pd_ra_bytes_need_release, '$emqx_ds_raft_bytes_need_release').

%% Cached value of the `append_only` DS DB configuration setting.
-define(pd_ra_force_monotonic, '$emqx_ds_raft_force_monotonic').

-spec init(_Args :: map()) -> ra_state().
init(#{db := DB, shard := Shard}) ->
    #{
        db_shard => {DB, Shard},
        latest => 0,
        tx_serial => 0,
        otx_leader_pid => undefined
    }.

-spec apply(ra_machine:command_meta_data(), ra_command(), ra_state()) ->
    {ra_state(), _Reply, _Effects}.
apply(
    RaftMeta = #{index := RaftIdx},
    Command = #{
        ?tag := ?BATCH,
        ?batch_operations := OperationsIn
    },
    #{db_shard := DBShard = {DB, Shard}, latest := Latest0} = State0
) ->
    ?tp(ds_ra_apply_batch, #{db => DB, shard => Shard, batch => OperationsIn, latest => Latest0}),
    {Stats, Latest, Operations} = assign_timestamps(DB, Latest0, OperationsIn),
    Preconditions = maps:get(?batch_preconditions, Command, []),
    Admission =
        case Preconditions of
            [] ->
                %% No preconditions.
                true;
            _ ->
                %% Since preconditions are part of the Ra log, we need to be sure they
                %% are applied perfectly idempotently, even when Ra log entries are
                %% replayed on top of a more recent storage state that already had them
                %% evaluated and applied before.
                case read_storage_raidx(DBShard) of
                    {ok, SRI} when RaftIdx > SRI ->
                        emqx_ds_precondition:verify(emqx_ds_storage_layer, DBShard, Preconditions);
                    {ok, SRI} when SRI >= RaftIdx ->
                        %% This batch looks outdated relative to the storage layer state.
                        false;
                    {error, _, _} = Error ->
                        Error
                end
        end,
    %% Always advance latest timestamp nonetheless, so it won't diverge on replay.
    State = State0#{latest := Latest},
    set_ts(DBShard, Latest),
    case Admission of
        true ->
            %% Plain batch, no preconditions.
            Result = store_batch_nondurable(DBShard, Operations),
            Effects = try_release_log(Stats, RaftMeta, State);
        ok ->
            %% Preconditions succeeded, need to persist `RaftIdx` in the storage layer.
            Result = store_batch_nondurable(DBShard, Operations),
            Result == ok andalso update_storage_raidx(DBShard, RaftIdx),
            Effects = try_release_log(Stats, RaftMeta, State);
        Result = false ->
            %% There are preconditions, but the batch looks outdated. Skip the batch.
            %% This is log replay, reply with `false`, noone expects the reply anyway.
            Effects = [];
        PreconditionFailed = {precondition_failed, _} ->
            %% Preconditions failed. Skip the batch, persist `RaftIdx` in the storage layer.
            Result = {error, unrecoverable, PreconditionFailed},
            update_storage_raidx(DBShard, RaftIdx),
            Effects = [];
        Result = {error, unrecoverable, Reason} ->
            ?tp(error, "emqx_ds_replication_apply_batch_failed", #{
                db => DB,
                shard => Shard,
                reason => Reason,
                details =>
                    "Unrecoverable error storing committed batch. Replicas may diverge. "
                    "Consider rebuilding this shard replica from scratch."
            }),
            Effects = []
    end,
    Effects =/= [] andalso ?tp(ds_ra_effects, #{effects => Effects, meta => RaftMeta}),
    {State, Result, Effects};
apply(
    RaftMeta,
    #{?tag := add_generation, ?since := Since},
    #{db_shard := DBShard, latest := Latest0} = State0
) ->
    ?tp(
        info,
        ds_ra_add_generation,
        #{
            shard => DBShard,
            since => Since
        }
    ),
    {Timestamp, Latest} = ensure_monotonic_timestamp(Since, Latest0),
    Result = emqx_ds_storage_layer:add_generation(DBShard, Timestamp),
    emqx_ds_beamformer:generation_event(DBShard),
    State = State0#{latest := Latest},
    set_ts(DBShard, Latest),
    Effects = release_log(RaftMeta, State),
    Effects =/= [] andalso ?tp(ds_ra_effects, #{effects => Effects, meta => RaftMeta}),
    {State, Result, Effects};
apply(
    RaftMeta,
    #{?tag := update_config, ?since := Since, ?config := Opts},
    #{db_shard := DBShard, latest := Latest0} = State0
) ->
    ?tp(
        notice,
        ds_ra_update_config,
        #{
            shard => DBShard,
            config => Opts,
            since => Since
        }
    ),
    {Timestamp, Latest} = ensure_monotonic_timestamp(Since, Latest0),
    Result = emqx_ds_storage_layer:update_config(DBShard, Timestamp, Opts),
    State = State0#{latest := Latest},
    Effects = release_log(RaftMeta, State),
    Effects =/= [] andalso ?tp(ds_ra_effects, #{effects => Effects, meta => RaftMeta}),
    {State, Result, Effects};
apply(
    _RaftMeta,
    #{?tag := drop_generation, ?generation := GenId},
    #{db_shard := DBShard} = State
) ->
    ?tp(
        info,
        ds_ra_drop_generation,
        #{
            shard => DBShard,
            generation => GenId
        }
    ),
    Result = emqx_ds_storage_layer:drop_slab(DBShard, GenId),
    {State, Result};
apply(
    _RaftMeta,
    #{?tag := storage_event, ?payload := CustomEvent, ?now := Now},
    #{db_shard := DBShard, latest := Latest0} = State
) ->
    Latest = max(Latest0, Now),
    set_ts(DBShard, Latest),
    ?tp(
        debug,
        ds_ra_storage_event,
        #{
            shard => DBShard, payload => CustomEvent, latest => Latest
        }
    ),
    Effects = handle_custom_event(DBShard, Latest, CustomEvent),
    {State#{latest => Latest}, ok, Effects};
apply(
    RaftMeta,
    #{
        ?tag := commit_tx_batch,
        ?prev_serial := SerCtl,
        ?serial := Serial,
        ?otx_timestamp := Timestamp,
        ?batches := Batches,
        ?otx_leader_pid := From
    },
    State0 = #{db_shard := DBShard, tx_serial := ExpectedSerial, otx_leader_pid := Leader}
) ->
    case From of
        Leader when SerCtl =:= ExpectedSerial ->
            case emqx_ds_storage_layer_ttv:commit_batch(DBShard, Batches, #{durable => false}) of
                ok ->
                    emqx_ds_storage_layer_ttv:set_read_tx_serial(DBShard, Serial),
                    State = State0#{tx_serial := Serial, latest := Timestamp},
                    Result = ok,
                    set_ts(DBShard, Timestamp + 1),
                    DispatchF = fun(Stream) ->
                        emqx_ds_beamformer:shard_event(DBShard, [Stream])
                    end,
                    emqx_ds_storage_layer_ttv:dispatch_events(DBShard, Batches, DispatchF),
                    Effects = try_release_log({Serial, length(Batches)}, RaftMeta, State);
                Err = ?err_unrec(_) ->
                    State = State0,
                    Result = Err,
                    Effects = []
            end;
        Leader ->
            %% Leader pid matches, but not the serial:
            State = State0,
            Result = ?err_unrec({serial_mismatch, SerCtl, ExpectedSerial}),
            Effects = [];
        _ ->
            %% Leader mismatch:
            State = State0,
            Result = ?err_unrec({not_the_leader, #{got => From, expect => Leader}}),
            Effects = []
    end,
    Effects =/= [] andalso ?tp(ds_ra_effects, #{effects => Effects, meta => RaftMeta}),
    {State, Result, Effects};
apply(
    _RaftMeta,
    #{
        ?tag := new_otx_leader,
        ?otx_leader_pid := Pid
    },
    State = #{db_shard := DBShard, tx_serial := Serial, latest := Timestamp}
) ->
    set_otx_leader(DBShard, Pid),
    Reply = {Serial, Timestamp},
    {State#{otx_leader_pid => Pid}, Reply}.

start_otx_leader(DB, Shard) ->
    ?tp_span(
        debug,
        dsrepl_start_otx_leader,
        #{db => DB, shard => Shard},
        emqx_ds_builtin_raft_db_sup:start_shard_leader_sup(DB, Shard)
    ).

assign_timestamps(DB, Latest, Messages) ->
    ForceMonotonic = force_monotonic_timestamps(DB),
    assign_timestamps(ForceMonotonic, Latest, Messages, [], 0, 0).

force_monotonic_timestamps(DB) ->
    case erlang:get(?pd_ra_force_monotonic) of
        undefined ->
            DBConfig = emqx_ds_builtin_raft_meta:db_config(DB),
            Flag = maps:get(force_monotonic_timestamps, DBConfig, _Default = true),
            erlang:put(?pd_ra_force_monotonic, Flag);
        Flag ->
            ok
    end,
    Flag.

store_batch_nondurable(DBShard = {DB, Shard}, Operations) ->
    DispatchF = fun(Events) ->
        emqx_ds_beamformer:shard_event({DB, Shard}, Events)
    end,
    emqx_ds_storage_layer:store_batch(DBShard, Operations, #{durable => false}, DispatchF).

%% Latest Raft index tracking
%%
%% Latest RaIdx is kept in a global, basically a KV pair in the default column family.
%% Each update goes to the same spot in the RocksDB, but this should not be a problem:
%% writes are non-durable and issued only when preconditions are involved.

-define(DSREPL_RAFTIDX, <<"dsrepl/ri">>).

read_storage_raidx(DBShard) ->
    case emqx_ds_storage_layer:fetch_global(DBShard, ?DSREPL_RAFTIDX) of
        {ok, V} when byte_size(V) =< 8 ->
            {ok, binary:decode_unsigned(V)};
        not_found ->
            {ok, 0};
        Error ->
            Error
    end.

update_storage_raidx(DBShard, RaftIdx) ->
    KV = #{?DSREPL_RAFTIDX => binary:encode_unsigned(RaftIdx)},
    ok = emqx_ds_storage_layer:store_global(DBShard, KV, #{durable => false}).

%% Log truncation

try_release_log({_N, BatchSize}, RaftMeta = #{index := CurrentIdx}, State) ->
    %% NOTE
    %% Because cursor release means storage flush (see
    %% `emqx_ds_builtin_raft_server_snapshot:write/3`), we should do that not too often
    %% (so the storage is happy with L0 SST sizes) and not too rarely (so we don't
    %% accumulate huge Raft logs).
    case inc_bytes_need_release(BatchSize) of
        AccSize when AccSize > ?RA_RELEASE_LOG_APPROX_SIZE ->
            release_log(RaftMeta, State);
        _NotYet ->
            case get_log_need_release(RaftMeta) of
                undefined ->
                    [];
                PrevIdx when CurrentIdx - PrevIdx > ?RA_RELEASE_LOG_MIN_FREQ ->
                    %% Release everything up to the last log entry, but only if there were
                    %% more than %% `?RA_RELEASE_LOG_MIN_FREQ` new entries since the last
                    %% release.
                    release_log(RaftMeta, State);
                _ ->
                    []
            end
    end.

release_log(RaftMeta = #{index := CurrentIdx}, State) ->
    %% NOTE
    %% Release everything up to the last log entry. This is important: any log entries
    %% following `CurrentIdx` should not contribute to `State` (that will be recovered
    %% from a snapshot).
    update_log_need_release(RaftMeta),
    reset_bytes_need_release(),
    {release_cursor, CurrentIdx, State}.

get_log_need_release(RaftMeta) ->
    case erlang:get(?pd_ra_idx_need_release) of
        undefined ->
            update_log_need_release(RaftMeta),
            undefined;
        LastIdx ->
            LastIdx
    end.

update_log_need_release(#{index := CurrentIdx}) ->
    erlang:put(?pd_ra_idx_need_release, CurrentIdx).

get_bytes_need_release() ->
    emqx_maybe:define(erlang:get(?pd_ra_bytes_need_release), 0).

inc_bytes_need_release(Size) ->
    Acc = get_bytes_need_release() + Size,
    erlang:put(?pd_ra_bytes_need_release, Acc),
    Acc.

reset_bytes_need_release() ->
    erlang:put(?pd_ra_bytes_need_release, 0).

-spec tick(integer(), ra_state()) -> ra_machine:effects().
tick(TimeMs, #{db_shard := DBShard, latest := Latest}) ->
    {Timestamp, _} = ensure_monotonic_timestamp(timestamp_to_timeus(TimeMs), Latest),
    handle_custom_event(DBShard, Timestamp, ra_tick).

assign_timestamps(true, Latest0, [Message0 = #message{} | Rest], Acc, N, Sz) ->
    case emqx_message:timestamp(Message0, microsecond) of
        TimestampUs when TimestampUs > Latest0 ->
            Latest = TimestampUs,
            Message = assign_timestamp(TimestampUs, Message0);
        _Earlier ->
            Latest = Latest0 + 1,
            Message = assign_timestamp(Latest, Message0)
    end,
    MSize = approx_message_size(Message0),
    assign_timestamps(true, Latest, Rest, [Message | Acc], N + 1, Sz + MSize);
assign_timestamps(false, Latest0, [Message0 = #message{} | Rest], Acc, N, Sz) ->
    TimestampUs = emqx_message:timestamp(Message0),
    Latest = max(Latest0, TimestampUs),
    Message = assign_timestamp(TimestampUs, Message0),
    MSize = approx_message_size(Message0),
    assign_timestamps(false, Latest, Rest, [Message | Acc], N + 1, Sz + MSize);
assign_timestamps(ForceMonotonic, Latest, [Operation | Rest], Acc, N, Sz) ->
    assign_timestamps(ForceMonotonic, Latest, Rest, [Operation | Acc], N + 1, Sz);
assign_timestamps(_ForceMonotonic, Latest, [], Acc, N, Size) ->
    {{N, Size}, Latest, lists:reverse(Acc)}.

assign_timestamp(TimestampUs, Message) ->
    {TimestampUs, Message}.

ensure_monotonic_timestamp(TimestampUs, Latest) when TimestampUs > Latest ->
    {TimestampUs, TimestampUs + 1};
ensure_monotonic_timestamp(_TimestampUs, Latest) ->
    {Latest, Latest + 1}.

%% FIXME: time unit conversion = always wrong. Remove it from DS.
timestamp_to_timeus(TimestampMs) ->
    TimestampMs * 1000.

timeus_to_timestamp(TimestampUs) ->
    TimestampUs div 1000.

snapshot_module() ->
    emqx_ds_builtin_raft_server_snapshot.

unpack_iterator(Shard, Iterator = #'Iterator'{}) ->
    emqx_ds_storage_layer_ttv:unpack_iterator(Shard, Iterator);
unpack_iterator(Shard, #{?tag := ?IT, ?enc := Iterator}) ->
    emqx_ds_storage_layer:unpack_iterator(Shard, Iterator).

high_watermark(DBShard = {DB, Shard}, Stream) ->
    Now = current_timestamp(DB, Shard),
    case Stream of
        #'Stream'{} ->
            emqx_ds_storage_layer_ttv:high_watermark(DBShard, Stream, Now);
        _ ->
            emqx_ds_storage_layer:high_watermark(DBShard, Stream, Now)
    end.

fast_forward(DBShard = {DB, Shard}, It = #'Iterator'{}, Key, BatchSize) ->
    ?IF_SHARD_READY(
        DBShard,
        begin
            Now = current_timestamp(DB, Shard),
            emqx_ds_storage_layer_ttv:fast_forward(DBShard, It, Key, Now, BatchSize)
        end
    );
fast_forward(
    DBShard = {DB, Shard}, #{?tag := ?IT, ?shard := Shard, ?enc := Inner0}, Key, BatchSize
) ->
    ?IF_SHARD_READY(
        DBShard,
        begin
            Now = current_timestamp(DB, Shard),
            case emqx_ds_storage_layer:fast_forward(DBShard, Inner0, Key, Now, BatchSize) of
                {ok, end_of_stream} ->
                    {ok, end_of_stream};
                {ok, Pos, Data} ->
                    {ok, Pos, Data};
                {error, _, _} = Err ->
                    Err
            end
        end
    ).

message_match_context(DBShard, Stream = #'Stream'{}, MsgKey, TTV) ->
    emqx_ds_storage_layer_ttv:message_match_context(DBShard, Stream, MsgKey, TTV);
message_match_context(DBShard, Stream, MsgKey, Message) ->
    emqx_ds_storage_layer:message_match_context(DBShard, Stream, MsgKey, Message).

iterator_match_context(DBShard, Iterator = #'Iterator'{}) ->
    emqx_ds_storage_layer_ttv:iterator_match_context(DBShard, Iterator);
iterator_match_context(DBShard = {_DB, Shard}, #{?tag := ?IT, ?shard := Shard, ?enc := Iterator}) ->
    emqx_ds_storage_layer:iterator_match_context(DBShard, Iterator).

scan_stream(DBShard = {DB, Shard}, Stream, TopicFilter, StartMsg, BatchSize) ->
    ?IF_SHARD_READY(
        DBShard,
        begin
            Now = current_timestamp(DB, Shard),
            case Stream of
                #'Stream'{} ->
                    emqx_ds_storage_layer_ttv:scan_stream(
                        DBShard, Stream, TopicFilter, infinity, StartMsg, BatchSize
                    );
                _ ->
                    emqx_ds_storage_layer:scan_stream(
                        DBShard, Stream, TopicFilter, Now, StartMsg, BatchSize
                    )
            end
        end
    ).

-spec update_iterator(emqx_ds_storage_layer:dbshard(), iterator(), emqx_ds:message_key()) ->
    emqx_ds:make_iterator_result(iterator()).
update_iterator(ShardId, #'Iterator'{} = Iter, DSKey) ->
    emqx_ds_storage_layer_ttv:update_iterator(ShardId, Iter, DSKey);
update_iterator(ShardId, #{?tag := ?IT, ?enc := Inner0} = OldIter, DSKey) ->
    case emqx_ds_storage_layer:update_iterator(ShardId, Inner0, DSKey) of
        {ok, Inner} ->
            {ok, OldIter#{?enc => Inner}};
        Err = {error, _, _} ->
            Err
    end.

handle_custom_event(_DBShard, _Latest, ra_tick) ->
    [];
handle_custom_event(DBShard, Latest, Event) ->
    try
        Events = emqx_ds_storage_layer:handle_event(DBShard, Latest, Event),
        [{append, #{?tag => storage_event, ?payload => I, ?now => Latest}} || I <- Events]
    catch
        EC:Err:Stacktrace ->
            ?tp(error, ds_storage_custom_event_fail, #{
                EC => Err, stacktrace => Stacktrace, event => Event
            }),
            []
    end.

set_ts({DB, Shard}, TS) ->
    emqx_ds_builtin_raft_sup:set_gvar(DB, ?gv_timestamp(Shard), TS).

set_otx_leader({DB, Shard}, Pid) ->
    ?tp(info, dsrepl_set_otx_leader, #{db => DB, shard => Shard, pid => Pid}),
    emqx_ds_builtin_raft_sup:set_gvar(DB, ?gv_otx_leader_pid(Shard), Pid).

%%

-spec state_enter(ra_server:ra_state() | eol, ra_state()) -> ra_machine:effects().
state_enter(MemberState, State = #{db_shard := {DB, Shard}}) ->
    ?tp(
        debug,
        ds_ra_state_enter,
        State#{state => MemberState}
    ),
    emqx_ds_builtin_raft_metrics:rasrv_state_changed(DB, Shard, MemberState),
    set_cache(MemberState, State),
    _ =
        case MemberState of
            leader ->
                start_otx_leader(DB, Shard);
            _ ->
                emqx_ds_builtin_raft_db_sup:stop_shard_leader_sup(DB, Shard)
        end,
    [].

%%

set_cache(MemberState, State = #{db_shard := DBShard, latest := Latest}) when
    MemberState =:= leader; MemberState =:= follower
->
    set_ts(DBShard, Latest),
    case State of
        #{tx_serial := Serial} ->
            emqx_ds_storage_layer_ttv:set_read_tx_serial(DBShard, Serial);
        #{} ->
            ok
    end,
    case State of
        #{otx_leader_pid := Pid} ->
            set_otx_leader(DBShard, Pid);
        #{} ->
            ok
    end;
set_cache(_, _) ->
    ok.

approx_message_size(#message{from = ClientID, topic = Topic, payload = Payload}) ->
    %% NOTE: Overhead here is basically few empty maps + 8-byte message id.
    %% TODO: Probably need to ask the storage layer about the footprint.
    MinOverhead = 40,
    MinOverhead + clientid_size(ClientID) + byte_size(Topic) + byte_size(Payload).

clientid_size(ClientID) when is_binary(ClientID) ->
    byte_size(ClientID);
clientid_size(ClientID) ->
    erlang:external_size(ClientID).

-spec rpc_target_preference(emqx_ds:db()) -> leader_preferred | local_preferred | undefined.
rpc_target_preference(DB) ->
    case emqx_ds_builtin_raft_meta:db_config(DB) of
        #{reads := ReadFrom} ->
            ReadFrom;
        #{} ->
            leader_preferred
    end.

%% @doc This internal function is used by the OTX leader process to
%% communicate with the Raft machine.
-spec local_raft_leader(emqx_ds:db(), emqx_ds:shard()) ->
    ra:server_id() | unknown.
local_raft_leader(DB, Shard) ->
    emqx_ds_builtin_raft_shard:server_info(
        leader,
        emqx_ds_builtin_raft_shard:local_server(DB, Shard)
    ).

proto_vsn(Node) ->
    proto_vsn(emqx_ds, Node).

otx_proto_vsn(Node) ->
    proto_vsn(emqx_ds_otx, Node).

beam_proto_vsn(Node) ->
    proto_vsn(emqx_ds_beamformer, Node).

proto_vsn(API, Node) ->
    case emqx_bpapi:supported_version(Node, API) of
        undefined -> -1;
        N when is_integer(N) -> N
    end.

-spec batch_size_and_time_limit(
    _StoreTTV :: boolean(), emqx_ds:db(), shard_id(), emqx_ds:next_limit()
) ->
    {pos_integer(), emqx_ds:time()}.
batch_size_and_time_limit(false, DB, Shard, BatchSize) when is_integer(BatchSize) ->
    {BatchSize, current_timestamp(DB, Shard)};
batch_size_and_time_limit(false, DB, Shard, {time, MaxTS, BatchSize}) ->
    {BatchSize, min(current_timestamp(DB, Shard), MaxTS)};
batch_size_and_time_limit(true, _DB, _Shard, BatchSize) when is_integer(BatchSize) ->
    {BatchSize, infinity};
batch_size_and_time_limit(true, _DB, _Shard, {time, MaxTS, BatchSize}) ->
    {BatchSize, MaxTS}.

-ifdef(TEST).

test_db_config(_Config) ->
    #{
        backend => builtin_raft,
        storage => {emqx_ds_storage_skipstream_lts, #{with_guid => true}},
        n_shards => 1,
        n_sites => 1,
        replication_factor => 3,
        replication_options => #{}
    }.

test_applications(Config) ->
    [
        {App, maps:get(App, Config, #{})}
     || App <- [emqx_durable_storage, emqx_ds_backends]
    ].

-endif.
