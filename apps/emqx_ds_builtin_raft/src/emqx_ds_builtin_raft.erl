%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_builtin_raft).
-moduledoc """
This is the entrypoint into the `builtin_raft` backend.
""".

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

%% RPC targets:
-export([
    do_drop_db_v1/1,
    do_get_streams_v1/5,
    do_make_iterator_v1/5,
    do_next_v1/3,
    do_list_slabs_v1/2,
    do_new_kv_tx_ctx_v1/4
]).

%% Internal exports:
-export([
    current_timestamp/2
]).

-ifdef(TEST).
-export([test_applications/1, test_db_config/1]).
-endif.

-export_type([
    db_opts/0,
    db_schema/0,
    db_runtime_config/0,

    stream/0,
    iterator/0,

    tx_context/0
]).

-include("emqx_ds_builtin_raft.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include("../../emqx_durable_storage/gen_src/DSBuiltinMetadata.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-elvis([{elvis_style, atom_naming_convention, disable}]).

%%================================================================================
%% Type declarations
%%================================================================================

-type db_opts() ::
    #{
        backend := builtin_raft,
        payload_type := emqx_ds_payload_transform:type(),
        n_shards := pos_integer(),
        n_sites := pos_integer(),
        replication_factor := pos_integer(),
        storage := emqx_ds_storage_layer:prototype(),
        reads := local_preferred | leader_preferred,
        replication_options => map(),
        subscriptions => emqx_ds_beamformer:opts(),
        transactions => emqx_ds_optimistic_tx:runtime_config(),
        rocksdb => emqx_ds_storage_layer:rocksdb_options()
    }.

-type db_schema() :: #{
    backend := builtin_raft,
    n_shards := pos_integer(),
    n_sites := pos_integer(),
    replication_factor := pos_integer(),
    storage := emqx_ds_storage_layer:prototype(),
    payload_type := emqx_ds_payload_transform:type()
}.

-type db_runtime_config() :: #{
    reads => leader_preferred | local_preferred,
    %% TODO: clarify type
    replication_options := #{},
    %% Beamformer
    subscriptions := emqx_ds_beamformer:opts(),
    %% Optimistic transaction:
    transactions := emqx_ds_optimistic_tx:runtime_config(),
    %% RocksDB options:
    rocksdb := emqx_ds_storage_layer:rocksdb_options()
}.

-type stream() :: emqx_ds_storage_layer_ttv:stream().

-type iterator() :: emqx_ds_storage_layer_ttv:iterator().

-record(sub_handle, {
    shard, server, ref
}).

-opaque tx_context() :: emqx_ds_optimistic_tx:ctx().

-define(SAFE_ERPC(EXPR),
    try
        EXPR
    catch
        error:RPCError__ = {erpc, _} ->
            ?err_rec(RPCError__);
        %% Note: remote node never _throws_ unrecoverable errors, so
        %% we can assume that all exceptions are transient.
        EC__:RPCError__:Stack__ ->
            ?err_rec(#{EC__ => RPCError__, stacktrace => Stack__})
    end
).

-define(SAFE_GEN_RPC(EXPR),
    case EXPR of
        RPCError__ = {badrpc, _} ->
            ?err_rec(RPCError__);
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
            ?err_rec(replica_offline)
    end
).

%% NOTE
%% Target node may still be in the process of starting up when RPCs arrive, it's
%% good to have them handled gracefully.
%% TODO
%% There's a possibility of race condition: storage may shut down right after we
%% ask for its status.
-define(IF_SHARD_READY(DBSHARD, EXPR),
    case emqx_ds_builtin_raft_db_sup:shard_info(DBSHARD, ready) of
        true -> EXPR;
        _Unready -> ?err_rec(shard_unavailable)
    end
).

-define(ERR_UPGRADE(NODE, VSN), ?err_rec({node_needs_upgrade, #{node => NODE, api_vsn => VSN}})).

%% TODO
%% Too large for normal operation, need better backpressure mechanism.
-define(RA_TIMEOUT, 60 * 1000).

%%================================================================================
%% API functions
%%================================================================================

%%================================================================================
%% `emqx_ds' behavior callbacks
%%================================================================================

-spec default_db_opts() -> map().
default_db_opts() ->
    #{
        backend => builtin_local,
        reads => local_preferred,
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
        replication_options => #{},
        n_sites => application:get_env(emqx_ds_builtin_raft, n_sites, 1),
        rocksdb => #{},
        storage => {emqx_ds_storage_skipstream_lts_v2, #{}}
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

-spec open_db(emqx_ds:db(), boolean(), db_schema(), db_runtime_config()) ->
    ok | {error, _}.
open_db(DB, Create, Schema, RuntimeConf) ->
    case emqx_ds_builtin_raft_sup:start_db(DB, Create, Schema, RuntimeConf) of
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
    foreach_shard(
        DB,
        fun(Shard) ->
            ok = emqx_ds_optimistic_tx:add_generation(DB, Shard)
        end
    ).

-spec update_db_config(emqx_ds:db(), emqx_dsch:db_schema(), emqx_dsch:db_runtime_config()) ->
    ok | {error, _}.
update_db_config(DB, NewSchema, NewRTConf) ->
    %% TODO: broadcast to the peers
    maybe
        ok ?= emqx_dsch:update_db_schema(DB, NewSchema),
        ok ?= emqx_dsch:update_db_config(DB, NewRTConf)
    end.

-spec list_slabs(emqx_ds:db()) -> #{emqx_ds:slab() => emqx_ds:slab_info()}.
list_slabs(DB) ->
    Shards = list_shards(DB),
    lists:foldl(
        fun(Shard, GensAcc) ->
            Result = ?SHARD_RPC(
                DB,
                Shard,
                Node,
                case proto_vsn(Node) of
                    Vsn when Vsn >= 1 ->
                        ?SAFE_ERPC(emqx_ds_builtin_raft_proto_v1:list_slabs(Node, DB, Shard));
                    Vsn ->
                        ?ERR_UPGRADE(Node, Vsn)
                end
            ),
            case Result of
                Gens = #{} ->
                    ok;
                {error, _Class, _Reason} ->
                    %% TODO: report errors
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

-spec drop_slab(emqx_ds:db(), emqx_ds:slab()) -> ok | {error, _}.
drop_slab(DB, {Shard, Generation}) ->
    Command = emqx_ds_builtin_raft_machine:drop_generation(Generation),
    ra_command(DB, Shard, Command, ra_retries()).

-spec drop_db(emqx_ds:db()) -> ok | {error, _}.
drop_db(DB) ->
    foreach_shard(DB, fun(Shard) ->
        {ok, _} = ra:delete_cluster(
            emqx_ds_builtin_raft_shard:shard_servers(DB, Shard), ?RA_TIMEOUT
        )
    end),
    _ = emqx_ds_builtin_raft_proto_v1:drop_db(list_nodes(), DB),
    emqx_ds_builtin_raft_meta:drop_db(DB),
    emqx_dsch:drop_db_schema(DB).

-spec list_shards(emqx_ds:db()) -> [emqx_ds:shard()].
list_shards(DB) ->
    emqx_ds_builtin_raft_meta:shards(DB).

-spec shard_of(emqx_ds:db(), binary()) -> emqx_ds:shard().
shard_of(DB, Obj) ->
    #{n_shards := N} = emqx_dsch:get_db_schema(DB),
    Hash = erlang:phash2(Obj, N),
    integer_to_binary(Hash).

-spec dirty_append(emqx_ds:dirty_append_opts(), emqx_ds:dirty_append_data()) ->
    reference() | noreply.
dirty_append(#{db := _, shard := _} = Opts, Data) ->
    emqx_ds_optimistic_tx:dirty_append(Opts, Data).

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
    Fun = fun(Shard, {Acc, AccErr}) ->
        Result = ?SHARD_RPC(
            DB,
            Shard,
            Node,
            case proto_vsn(Node) of
                Vsn when Vsn >= 1 ->
                    ?SAFE_ERPC(
                        emqx_ds_builtin_raft_proto_v1:get_streams(
                            Node, DB, Shard, TopicFilter, StartTime, MinGeneration
                        )
                    );
                Vsn ->
                    ?ERR_UPGRADE(Node, Vsn)
            end
        ),
        case Result of
            Streams when is_list(Streams) ->
                L = lists:map(
                    fun(#'Stream'{generation = Generation} = Stream) ->
                        Slab = {Shard, Generation},
                        {Slab, Stream}
                    end,
                    Streams
                ),
                {L ++ Acc, AccErr};
            {error, _, _} = Err ->
                E = {Shard, Err},
                {Acc, [E | AccErr]}
        end
    end,
    lists:foldl(Fun, {[], []}, Shards).

-spec make_iterator(emqx_ds:db(), stream(), emqx_ds:topic_filter(), emqx_ds:time()) ->
    emqx_ds:make_iterator_result(emqx_ds:ds_specific_iterator()).
make_iterator(DB, Stream = #'Stream'{shard = Shard}, TopicFilter, StartTime) ->
    ?SHARD_RPC(
        DB,
        Shard,
        Node,
        case proto_vsn(Node) of
            Vsn when Vsn >= 1 ->
                ?SAFE_ERPC(
                    emqx_ds_builtin_raft_proto_v1:make_iterator(
                        Node, DB, Shard, Stream, TopicFilter, StartTime
                    )
                );
            Vsn ->
                ?ERR_UPGRADE(Node, Vsn)
        end
    ).

-spec next(emqx_ds:db(), iterator(), emqx_ds:next_limit()) -> emqx_ds:next_result(iterator()).
next(DB, Iter = #'Iterator'{shard = Shard}, Limit) ->
    T0 = erlang:monotonic_time(microsecond),
    Result = ?SHARD_RPC(
        DB,
        Shard,
        Node,
        case proto_vsn(Node) of
            Vsn when Vsn >= 1 ->
                ?SAFE_GEN_RPC(
                    emqx_ds_builtin_raft_proto_v1:next(Node, DB, Shard, Iter, Limit)
                );
            Vsn ->
                ?ERR_UPGRADE(Node, Vsn)
        end
    ),
    T1 = erlang:monotonic_time(microsecond),
    emqx_ds_builtin_metrics:observe_next_time(DB, T1 - T0),
    Result.

%% Subscriptions

-spec subscribe(emqx_ds:db(), iterator(), emqx_ds:sub_opts()) ->
    {ok, emqx_ds:subscription_handle(), emqx_ds:sub_ref()} | emqx_ds:error(_).
subscribe(DB, #'Iterator'{shard = Shard} = It, SubOpts) ->
    ?SHARD_RPC(
        DB,
        Shard,
        Node,
        maybe
            ok ?=
                case beam_proto_vsn(Node) of
                    Vsn when Vsn >= 1 ->
                        ok;
                    Vsn ->
                        ?ERR_UPGRADE(Node, Vsn)
                end,
            {ok, Server} ?= whereis_beamformer(Node, DB, Shard),
            MRef = monitor(process, Server),
            {ok, MRef} ?= beamformer_subscribe(Node, Server, self(), MRef, It, SubOpts),
            {ok, #sub_handle{shard = Shard, server = Server, ref = MRef}, MRef}
        end
    ).

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

%% Blob transaction API:
-spec new_tx(emqx_ds:db(), emqx_ds:transaction_opts()) ->
    {ok, tx_context()} | emqx_ds:error(_).
new_tx(DB, Options = #{shard := ShardOpt, generation := Generation}) ->
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
        case proto_vsn(Node) of
            Vsn when Vsn >= 1 ->
                ?SAFE_ERPC(
                    emqx_ds_builtin_raft_proto_v1:new_kv_tx_ctx(
                        Node, DB, Shard, Generation, Options
                    )
                );
            Vsn ->
                ?ERR_UPGRADE(Node, Vsn)
        end
    ).

-spec commit_tx(emqx_ds:db(), tx_context(), emqx_ds:tx_ops()) -> reference().
commit_tx(DB, Ctx, Ops) ->
    %% NOTE: pid of the leader is stored in the context, this should
    %% work for remote processes too.
    emqx_ds_optimistic_tx:commit_kv_tx(DB, Ctx, Ops).

-spec tx_commit_outcome({'DOWN', reference(), _, _, _}) -> emqx_ds:commit_result().
tx_commit_outcome(Reply) ->
    emqx_ds_optimistic_tx:tx_commit_outcome(Reply).

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
handle_schema_event(DB, PendingId, Pending) ->
    case Pending of
        #{command := change_schema, old := OldSchema, new := NewSchema, originator := Site} ->
            update_shards_schema(DB, PendingId, Site, OldSchema, NewSchema);
        _ ->
            ok
    end,
    emqx_dsch:del_pending(PendingId).

%%================================================================================
%% Beamformer callbacks
%%================================================================================

-spec beamformer_config(emqx_ds:db()) -> emqx_ds_beamformer:opts().
beamformer_config(DB) ->
    #{runtime := #{subscriptions := Conf}} = emqx_dsch:get_db_runtime(DB),
    Conf.

unpack_iterator(Shard, Iterator = #'Iterator'{}) ->
    emqx_ds_storage_layer_ttv:unpack_iterator(Shard, Iterator).

high_watermark(DBShard = {DB, Shard}, Stream = #'Stream'{}) ->
    Now = current_timestamp(DB, Shard),
    emqx_ds_storage_layer_ttv:high_watermark(DBShard, Stream, Now).

fast_forward(DBShard = {DB, Shard}, It = #'Iterator'{}, Key, BatchSize) ->
    ?IF_SHARD_READY(
        DBShard,
        begin
            Now = current_timestamp(DB, Shard),
            emqx_ds_storage_layer_ttv:fast_forward(DBShard, It, Key, Now, BatchSize)
        end
    ).

message_match_context(DBShard, Stream = #'Stream'{}, MsgKey, TTV) ->
    emqx_ds_storage_layer_ttv:message_match_context(DBShard, Stream, MsgKey, TTV).

iterator_match_context(DBShard, Iterator = #'Iterator'{}) ->
    emqx_ds_storage_layer_ttv:iterator_match_context(DBShard, Iterator).

scan_stream(DBShard = {DB, Shard}, Stream = #'Stream'{}, TopicFilter, StartMsg, BatchSize) ->
    ?IF_SHARD_READY(
        DBShard,
        begin
            %% TODO: this has been changed during refactoring. Double-check.
            Now = current_timestamp(DB, Shard),
            emqx_ds_storage_layer_ttv:scan_stream(
                DBShard, Stream, TopicFilter, Now, StartMsg, BatchSize
            )
        end
    ).

-spec update_iterator(emqx_ds_storage_layer:dbshard(), iterator(), emqx_ds:message_key()) ->
    emqx_ds:make_iterator_result(iterator()).
update_iterator(ShardId, #'Iterator'{} = Iter, DSKey) ->
    emqx_ds_storage_layer_ttv:update_iterator(ShardId, Iter, DSKey).

%%================================================================================
%% emqx_ds_optimistic_tx callbacks
%%================================================================================

otx_get_tx_serial(DB, Shard) ->
    emqx_ds_storage_layer_ttv:get_read_tx_serial({DB, Shard}).

-spec otx_get_leader(emqx_ds:db(), emqx_ds:shard()) -> pid() | undefined.
otx_get_leader(DB, Shard) ->
    case emqx_dsch:gvar_get(DB, Shard, ?gv_sc_replica, ?gv_otx_leader_pid) of
        {ok, Pid} -> Pid;
        undefined -> undefined
    end.

otx_get_latest_generation(DB, Shard) ->
    emqx_ds_storage_layer:generation_current({DB, Shard}).

otx_become_leader(DB, Shard) ->
    Command = emqx_ds_builtin_raft_machine:otx_new_leader(self()),
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
    Command = emqx_ds_builtin_raft_machine:otx_commit(SerCtl, Serial, Timestamp, Batches, self()),
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

otx_add_generation(DB, Shard, Since) ->
    ra_command(DB, Shard, emqx_ds_builtin_raft_machine:add_generation(Since), ra_retries()).

otx_lookup_ttv(DBShard, GenId, Topic, Timestamp) ->
    emqx_ds_storage_layer_ttv:lookup(DBShard, GenId, Topic, Timestamp).

otx_get_runtime_config(DB) ->
    #{runtime := #{transactions := Conf}} = emqx_dsch:get_db_runtime(DB),
    Conf.

%%================================================================================
%% Internal exports
%%================================================================================

-doc """
Messages have been replicated up to this timestamp on the local replica.
""".
-spec current_timestamp(emqx_ds:db(), emqx_ds:shard()) -> emqx_ds:time().
current_timestamp(DB, Shard) ->
    {ok, Val} = emqx_dsch:gvar_get(DB, Shard, ?gv_sc_replica, ?gv_timestamp),
    Val.

%%================================================================================
%% RPC targets
%%================================================================================

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

-spec do_get_streams_v1(
    emqx_ds:db(),
    emqx_ds:shard(),
    emqx_ds:topic_filter(),
    emqx_ds:time(),
    emqx_ds:generation()
) ->
    [{emqx_ds:generation(), emqx_ds_storage_layer:stream() | emqx_ds_storage_layer_ttv:stream()}]
    | emqx_ds:error(storage_down).
do_get_streams_v1(DB, Shard, TopicFilter, StartTime, MinGeneration) ->
    DBShard = {DB, Shard},
    ?IF_SHARD_READY(
        DBShard,
        emqx_ds_storage_layer_ttv:get_streams(DBShard, TopicFilter, StartTime, MinGeneration)
    ).

-spec do_make_iterator_v1(
    emqx_ds:db(),
    emqx_ds:shard(),
    emqx_ds_storage_layer_ttv:stream(),
    emqx_ds:topic_filter(),
    emqx_ds:time()
) ->
    emqx_ds:make_iterator_result().
do_make_iterator_v1(DB, Shard, Stream = #'Stream'{}, TopicFilter, Time) ->
    ?IF_SHARD_READY(
        {DB, Shard},
        emqx_ds_storage_layer_ttv:make_iterator(DB, Stream, TopicFilter, Time)
    ).

-spec do_list_slabs_v1(emqx_ds:db(), emqx_ds:shard()) ->
    #{emqx_ds:generation() => emqx_ds:slab_info()} | emqx_ds:error(_).
do_list_slabs_v1(DB, Shard) ->
    DBShard = {DB, Shard},
    ?IF_SHARD_READY(
        DBShard,
        emqx_ds_storage_layer:list_slabs(DBShard)
    ).

-spec do_next_v1(
    emqx_ds:db(),
    emqx_ds_storage_layer_ttv:iterator(),
    emqx_ds:next_limit()
) ->
    emqx_ds:next_result().
do_next_v1(DB, Iter = #'Iterator'{shard = Shard}, NextLimit) ->
    ?IF_SHARD_READY(
        {DB, Shard},
        begin
            {BatchSize, TimeLimit} = batch_size_and_time_limit(DB, Shard, NextLimit),
            emqx_ds_storage_layer_ttv:next(DB, Iter, BatchSize, TimeLimit)
        end
    ).

-spec do_new_kv_tx_ctx_v1(
    emqx_ds:db(), emqx_ds:shard(), emqx_ds:generation(), emqx_ds:transaction_opts()
) ->
    {ok, tx_context()} | emqx_ds:error(_).
do_new_kv_tx_ctx_v1(DB, Shard, Generation, Options) ->
    emqx_ds_optimistic_tx:new_kv_tx_ctx(?MODULE, DB, Shard, Generation, Options).

%%================================================================================
%% Internal functions
%%================================================================================

-spec update_shards_schema(
    emqx_ds:db(), emqx_dsch:pending_id(), emqx_dsch:site(), Schema, Schema
) -> ok when
    Schema :: db_schema().
update_shards_schema(DB, PendingId, Site, _OldSchema, NewSchema) ->
    Command = emqx_ds_builtin_raft_machine:update_schema(PendingId, Site, NewSchema),
    foreach_shard(
        DB,
        fun(Shard) ->
            ra_command(DB, Shard, Command, ra_retries())
        end
    ).

-spec verify_db_opts(db_opts()) -> {ok, db_schema(), db_runtime_config()} | emqx_ds:error(_).
verify_db_opts(Opts) ->
    maybe
        #{
            backend := builtin_raft,
            payload_type := PType,
            n_shards := NShards,
            n_sites := NSites,
            replication_factor := ReplFactor,
            storage := Storage,
            reads := Reads,
            replication_options := ReplOpts,
            subscriptions := Subs,
            transactions := Trans,
            rocksdb := RocksDB
        } ?= Opts,
        true ?= is_integer(NShards) andalso NShards > 0,
        true ?= is_integer(NSites) andalso NSites > 0,
        true ?= is_integer(ReplFactor) andalso ReplFactor > 0,
        true ?= Reads =:= local_preferred orelse Reads =:= leader_preferred,
        true ?= is_map(ReplOpts),
        true ?= is_map(Subs),
        true ?= is_map(Trans),
        true ?= is_map(RocksDB),
        Schema = #{
            backend => builtin_raft,
            n_shards => NShards,
            n_sites => NSites,
            replication_factor => ReplFactor,
            payload_type => PType,
            storage => Storage
        },
        RTOpts = #{
            reads => Reads,
            replication_options => ReplOpts,
            subscriptions => Subs,
            transactions => Trans,
            rocksdb => RocksDB
        },
        {ok, Schema, RTOpts}
    else
        What ->
            %% TODO: This reporting is rather insufficient
            ?tp(error, emqx_ds_builtin_raft_invalid_conf, #{fail => What, conf => Opts}),
            ?err_unrec(badarg)
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
                [n_shards, replication_factor]
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

-spec batch_size_and_time_limit(emqx_ds:db(), emqx_ds:shard(), emqx_ds:next_limit()) ->
    {pos_integer(), emqx_ds:time() | infinity}.
batch_size_and_time_limit(_DB, _Shard, BatchSize) when is_integer(BatchSize) ->
    {BatchSize, infinity};
batch_size_and_time_limit(_DB, _Shard, {time, MaxTS, BatchSize}) ->
    {BatchSize, MaxTS}.

-spec whereis_beamformer(node(), emqx_ds:db(), emqx_ds:shard()) -> {ok, pid()} | emqx_ds:error(_).
whereis_beamformer(Node, DB, Shard) ->
    ?SAFE_ERPC(
        case emqx_ds_beamformer_proto_v1:where(Node, {DB, Shard}) of
            Pid when is_pid(Pid) ->
                {ok, Pid};
            undefined ->
                ?err_rec(not_available);
            Err ->
                Err
        end
    ).

-spec beamformer_subscribe(node(), pid(), pid(), reference(), iterator(), emqx_ds:sub_opts()) ->
    {ok, emqx_ds:sub_ref()} | emqx_ds:error(_).
beamformer_subscribe(Node, Server, Subscriber, SubRef, It, SubOpts) ->
    ?SAFE_ERPC(
        emqx_ds_beamformer_proto_v1:subscribe(
            Node, Server, Subscriber, SubRef, It, SubOpts
        )
    ).

-doc """
This internal function is used by the OTX leader process to
communicate with the Raft machine.
""".
-spec local_raft_leader(emqx_ds:db(), emqx_ds:shard()) ->
    ra:server_id() | unknown.
local_raft_leader(DB, Shard) ->
    emqx_ds_builtin_raft_shard:server_info(
        leader,
        emqx_ds_builtin_raft_shard:local_server(DB, Shard)
    ).

list_nodes() ->
    %% TODO: list sites via dsch
    mria:running_nodes().

ra_retries() ->
    %% TODO: make configurable
    10.

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

-spec foreach_shard(emqx_ds:db(), fun((emqx_ds:shard()) -> _)) -> ok.
foreach_shard(DB, Fun) ->
    lists:foreach(Fun, list_shards(DB)).

-spec rpc_target_preference(emqx_ds:db()) -> leader_preferred | local_preferred.
rpc_target_preference(DB) ->
    #{runtime := #{reads := Reads}} = emqx_dsch:get_db_runtime(DB),
    Reads.

proto_vsn(Node) ->
    proto_vsn(emqx_ds, Node).

beam_proto_vsn(Node) ->
    proto_vsn(emqx_ds_beamformer, Node).

proto_vsn(API, Node) ->
    case emqx_bpapi:supported_version(Node, API) of
        undefined -> -1;
        N when is_integer(N) -> N
    end.

-ifdef(TEST).

test_db_config(_Config) ->
    #{
        backend => builtin_raft,
        storage => {emqx_ds_storage_skipstream_lts_v2, #{}},
        n_shards => 1,
        n_sites => 1,
        replication_factor => 3
    }.

test_applications(Config) ->
    [
        {App, maps:get(App, Config, #{})}
     || App <- [emqx_durable_storage, emqx_ds_backends]
    ].

-endif.
