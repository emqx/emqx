%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc An alternative storage layer interface that works with
%% Topic-Time-Value triples instead of `#message' records.
-module(emqx_ds_storage_layer_ttv).

%% API:
-export([
    prepare_tx/5,
    commit_batch/3,

    make_iterator/4,
    get_streams/3,
    next/4,

    lookup/4,
    get_read_tx_serial/1,
    set_read_tx_serial/2
]).

%% internal exports:
-export([]).

-export_type([stream/0, iterator/0]).

-include_lib("snabbkaffe/include/trace.hrl").
-include("emqx_ds.hrl").
-include("../gen_src/DSBuiltinMetadata.hrl").

-elvis([{elvis_style, atom_naming_convention, disable}]).

%%================================================================================
%% Type declarations
%%================================================================================

-type inner_stream() :: binary().

-type stream() :: #'Stream'{
    shard :: emqx_ds:shard(),
    generation :: emqx_ds:generation(),
    inner :: inner_stream()
}.

-type it_static() :: binary().
-type it_pos() :: binary().

-type iterator() :: #'Iterator'{
    shard :: emqx_ds:shard(),
    generation :: emqx_ds:generation(),
    innerStatic :: it_static(),
    innerPos :: it_pos()
}.

-type cooked_tx() :: term().

-callback create(
    emqx_ds_storage_layer:dbshard(),
    rocksdb:db_handle(),
    emqx_ds:generation(),
    Options :: map(),
    emqx_ds_storage_layer:generation_data() | undefined,
    emqx_ds:db_opts()
) ->
    {_Schema, emqx_ds_storage_layer:cf_refs()}.

%% Open the existing schema
-callback open(
    emqx_ds_storage_layer:dbshard(),
    rocksdb:db_handle(),
    emqx_ds:generation(),
    emqx_ds_storage_layer:cf_refs(),
    _Schema
) ->
    emqx_ds_storage_layer:generation_data().

%% Delete the schema and data
-callback drop(
    emqx_ds_storage_layer:dbshard(),
    rocksdb:db_handle(),
    emqx_ds:generation(),
    emqx_ds_storage_layer:cf_refs(),
    emqx_ds_storage_layer:generation_data()
) ->
    ok | {error, _Reason}.

-callback prepare_tx(
    emqx_ds_storage_layer:dbshard(),
    emqx_ds_storage_layer:generation_data(),
    emqx_ds:tx_serial(),
    emqx_ds:tx_ops(),
    emqx_ds_storage_layer:batch_prepare_opts()
) ->
    {ok, cooked_tx()} | emqx_ds:error(_).

-callback commit_batch(
    emqx_ds_storage_layer:dbshard(),
    emqx_ds_storage_layer:generation_data(),
    [cooked_tx()],
    emqx_ds_storage_layer:batch_store_opts()
) -> ok | emqx_ds:error(_).

-callback get_streams(
    emqx_ds_storage_layer:dbshard(),
    emqx_ds_storage_layer:generation_data(),
    emqx_ds:topic_filter(),
    emqx_ds:time()
) ->
    [_Stream].

-callback make_iterator(
    emqx_ds:db(),
    emqx_ds:shard(),
    emqx_ds_storage_layer:generation_data(),
    inner_stream(),
    emqx_ds:topic_filter(),
    emqx_ds:time()
) ->
    {ok, it_static(), it_pos()} | emqx_ds:error(_).

-callback lookup(
    emqx_ds_storage_layer:dbshard(),
    emqx_ds_storage_layer:generation_data(),
    emqx_ds:topic(),
    emqx_ds:time()
) ->
    {ok, binary()} | undefined | emqx_ds:error(_).

-callback next(
    emqx_ds:db(),
    emqx_ds:shard(),
    emqx_ds_storage_layer:generation_data(),
    it_static(),
    it_pos(),
    pos_integer(),
    emqx_ds:time(),
    boolean()
) ->
    {ok, it_pos(), [emqx_ds:ttv()]} | emqx_ds:error(_).

-callback batch_events(
    emqx_ds_storage_layer:dbshard(),
    emqx_ds_storage_layer:generation_data(),
    _CookedBatch
) -> [inner_stream()].

-define(tx_serial_gvar, tx_serial).

-define(ERR_GEN_GONE, ?err_unrec(generation_not_found)).

%%================================================================================
%% API functions
%%================================================================================

%% @doc Transform write and delete operations of a transaction into a
%% "cooked batch" that can be stored in the transaction log or
%% transfered over the network.
-spec prepare_tx(
    emqx_ds_storage_layer:dbshard(),
    emqx_ds_storage_layer:gen_id(),
    emqx_ds:tx_serial(),
    emqx_ds:tx_ops(),
    emqx_ds_storage_layer:batch_prepare_opts()
) ->
    {ok, cooked_tx()} | emqx_ds:error(_).
prepare_tx(DBShard, GenId, TXSerial, Tx, Options) ->
    ?tp(emqx_ds_storage_layer_prepare_kv_tx, #{
        shard => DBShard, generation => GenId, batch => Tx, options => Options
    }),
    case emqx_ds_storage_layer:generation_get(DBShard, GenId) of
        #{module := Mod, data := GenData} ->
            T0 = erlang:monotonic_time(microsecond),
            Result = Mod:prepare_tx(DBShard, GenData, TXSerial, Tx, Options),
            T1 = erlang:monotonic_time(microsecond),
            %% TODO store->prepare
            emqx_ds_builtin_metrics:observe_store_batch_time(DBShard, T1 - T0),
            Result;
        not_found ->
            ?err_unrec({storage_not_found, GenId})
    end.

%% @doc Lookup a single value matching a concrete topic and timestamp
-spec lookup(
    emqx_ds_storage_layer:dbshard(), emqx_ds:generation(), emqx_ds:topic(), emqx_ds:time()
) ->
    {ok, binary()} | undefined | emqx_ds:error(_).
lookup(DBShard, Generation, Topic, Time) ->
    case emqx_ds_storage_layer:generation_get(DBShard, Generation) of
        not_found ->
            ?err_unrec(generation_not_found);
        #{module := Mod, data := GenData} ->
            Mod:lookup(DBShard, GenData, Topic, Time)
    end.

-spec get_read_tx_serial(emqx_ds_storage_layer:dbshard()) ->
    {ok, emqx_ds_optimistic_tx:serial()} | undefined.
get_read_tx_serial(DBShard) ->
    GVars = emqx_ds_storage_layer:get_gvars(DBShard),
    case ets:lookup(GVars, ?tx_serial_gvar) of
        [{_, Serial}] ->
            {ok, Serial};
        [] ->
            undefined
    end.

-spec set_read_tx_serial(emqx_ds_storage_layer:dbshard(), emqx_ds_optimistic_tx:serial()) -> ok.
set_read_tx_serial(DBShard, Serial) ->
    GVars = emqx_ds_storage_layer:get_gvars(DBShard),
    ets:insert(GVars, {?tx_serial_gvar, Serial}),
    ok.

-spec commit_batch(
    emqx_ds_storage_layer:dbshard(),
    [{emqx_ds:generation(), [cooked_tx()]}],
    emqx_ds_storage_layer:batch_store_opts()
) -> emqx_ds:store_batch_result().
commit_batch(DBShard, [{Generation, GenBatches} | Rest], Opts) ->
    case do_commit_batch(DBShard, Generation, GenBatches, Opts) of
        ok ->
            commit_batch(DBShard, Rest, Opts);
        Err ->
            Err
    end;
commit_batch(_, [], _) ->
    ok.

-spec get_streams(emqx_ds_storage_layer:dbshard(), emqx_ds:topic_filter(), emqx_ds:time()) ->
    [stream()].
get_streams(DBShard = {_DB, Shard}, TopicFilter, StartTime) ->
    Gens = emqx_ds_storage_layer:generations_since(DBShard, StartTime),
    ?tp(get_streams_all_gens, #{gens => Gens}),
    lists:flatmap(
        fun(GenId) ->
            ?tp(get_streams_get_gen, #{gen_id => GenId}),
            case emqx_ds_storage_layer:generation_get(DBShard, GenId) of
                #{module := Mod, data := GenData} ->
                    Streams = Mod:get_streams(DBShard, GenData, TopicFilter, StartTime),
                    [
                        #'Stream'{shard = Shard, generation = GenId, inner = InnerStream}
                     || InnerStream <- Streams
                    ];
                not_found ->
                    %% race condition: generation was dropped before getting its streams?
                    []
            end
        end,
        Gens
    ).

-spec make_iterator(emqx_ds:db(), stream(), emqx_ds:topic_filter(), emqx_ds:time()) ->
    emqx_ds:make_iterator_result(iterator()).
make_iterator(
    DB, #'Stream'{shard = Shard, generation = Generation, inner = Inner}, TopicFilter, StartTime
) ->
    case emqx_ds_storage_layer:generation_get({DB, Shard}, Generation) of
        #{module := Mod, data := GenData} ->
            case Mod:make_iterator(DB, Shard, GenData, Inner, TopicFilter, StartTime) of
                {ok, ItStatic, ItPos} ->
                    {ok, #'Iterator'{
                        shard = Shard,
                        generation = Generation,
                        innerStatic = ItStatic,
                        innerPos = ItPos
                    }};
                Err ->
                    Err
            end;
        not_found ->
            ?ERR_GEN_GONE
    end.

-spec next(emqx_ds:db(), iterator(), pos_integer(), emqx_ds:time()) ->
    emqx_ds:next_result(iterator()).
next(
    DB,
    It0 = #'Iterator'{
        shard = Shard, generation = Generation, innerStatic = InnerStatic, innerPos = InnerPos0
    },
    BatchSize,
    Now
) ->
    DBShard = {DB, Shard},
    case emqx_ds_storage_layer:generation_get(DBShard, Generation) of
        #{module := Mod, data := GenData} ->
            IsCurrent = Generation =:= emqx_ds_storage_layer:generation_current(DBShard),
            case Mod:next(DB, Shard, GenData, InnerStatic, InnerPos0, BatchSize, Now, IsCurrent) of
                {ok, InnerPos, Batch} ->
                    {ok, It0#'Iterator'{innerPos = InnerPos}, Batch};
                {ok, end_of_stream} ->
                    {ok, end_of_stream};
                Error = {error, _, _} ->
                    Error
            end;
        not_found ->
            ?ERR_GEN_GONE
    end.

%%================================================================================
%% behavior callbacks
%%================================================================================

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

%% @doc Commit a collection of cooked transactions that all belong to
%% the same generation to the storage
-spec do_commit_batch(
    emqx_ds_storage_layer:dbshard(),
    emqx_ds:generation(),
    [cooked_tx()],
    emqx_ds_storage_layer:batch_store_opts()
) -> emqx_ds:store_batch_result().
do_commit_batch(DBShard, GenId, CookedTransactions, Options) ->
    case emqx_ds_storage_layer:generation_get(DBShard, GenId) of
        #{module := Mod, data := GenData} ->
            T0 = erlang:monotonic_time(microsecond),
            Result = Mod:commit_batch(DBShard, GenData, CookedTransactions, Options),
            T1 = erlang:monotonic_time(microsecond),
            emqx_ds_builtin_metrics:observe_store_batch_time(DBShard, T1 - T0),
            Result;
        not_found ->
            ?err_unrec({storage_not_found, GenId})
    end.
