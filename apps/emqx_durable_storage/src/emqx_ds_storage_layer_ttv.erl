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
    lookup/4,
    get_read_tx_serial/1,
    set_read_tx_serial/2
]).

%% internal exports:
-export([]).

-export_type([]).

-include_lib("snabbkaffe/include/trace.hrl").
-include("emqx_ds.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

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
    emqx_ds_storage_layer:dbshard(),
    emqx_ds_storage_layer:generation_data(),
    _Stream,
    emqx_ds:topic_filter(),
    emqx_ds:time()
) ->
    emqx_ds:make_iterator_result(_Iterator).

-callback lookup(
    emqx_ds_storage_layer:dbshard(),
    emqx_ds_storage_layer:generation_data(),
    emqx_ds:topic(),
    emqx_ds:time()
) ->
    {ok, emqx_ds:value()} | undefined | emqx_ds:error(_).

-callback next(
    emqx_ds_storage_layer:dbshard(),
    emqx_ds_storage_layer:generation_data(),
    Iter,
    pos_integer(),
    %% TODO: Currently calls to kv layouts are still dispatched
    %% through `emqx_ds_storage_layer' and carry some extra
    %% parameters. Fixing it is not super trivial and not very useful,
    %% except from aesthetics perspective, so there are some useless
    %% parameters.
    _,
    _
) ->
    {ok, Iter, [emqx_ds:ttv()]} | emqx_ds:error(_).

-callback batch_events(
    emqx_ds_storage_layer:dbshard(),
    emqx_ds_storage_layer:generation_data(),
    _CookedBatch
) -> [_Stream].

-define(tx_serial_gvar, tx_serial).

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
    {ok, emqx_ds:value()} | undefined | emqx_ds:error(_).
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
