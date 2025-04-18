%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_builtin_local_batch_serializer).

-feature(maybe_expr, enable).

-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds_storage_layer_tx.hrl").

%% API
-export([
    start_link/3,

    store_batch_atomic/4,
    commit_blob_tx/3
]).

%% `gen_server' API
-export([
    init/1,
    handle_call/3,
    handle_cast/2
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(serial_key, <<"emqx_ds_builtin_local_tx_serial">>).

-define(name(DB, SHARD), {n, l, {?MODULE, DB, SHARD}}).
-define(via(DB, SHARD), {via, gproc, ?name(DB, SHARD)}).

-record(store_batch_atomic, {batch :: emqx_ds:batch(), opts :: emqx_ds:message_store_opts()}).
-record(commit_blob_tx, {ctx :: emqx_ds_storage_layer_tx:ctx(), ops :: emqx_ds:blob_tx_ops()}).

-record(s, {
    dbshard :: emqx_ds_storage_layer:dbshard(),
    serial :: atomics:atomics_ref()
}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

start_link(DB, Shard, _Opts) ->
    gen_server:start_link(?via(DB, Shard), ?MODULE, [DB, Shard], []).

store_batch_atomic(DB, Shard, Batch, Opts) ->
    gen_server:call(?via(DB, Shard), #store_batch_atomic{batch = Batch, opts = Opts}, infinity).

commit_blob_tx(DB, #ds_tx_ctx{shard = Shard} = Ctx, Ops) ->
    gen_server:call(?via(DB, Shard), #commit_blob_tx{ctx = Ctx, ops = Ops}, infinity).

%%------------------------------------------------------------------------------
%% `gen_server' callbacks
%%------------------------------------------------------------------------------

init([DB, Shard]) ->
    process_flag(message_queue_data, off_heap),
    DBShard = {DB, Shard},
    S = #s{
        dbshard = DBShard,
        serial = open_serial(DBShard)
    },
    {ok, S}.

handle_call(#store_batch_atomic{batch = Batch, opts = StoreOpts}, _From, S = #s{dbshard = DBShard}) ->
    Result = do_store_batch_atomic(DBShard, Batch, StoreOpts),
    {reply, Result, S};
handle_call(#commit_blob_tx{ctx = Ctx, ops = Ops}, _From, S) ->
    Result = blob_tx(S, Ctx, Ops),
    {reply, Result, S};
handle_call(Call, _From, S) ->
    {reply, {error, {unknown_call, Call}}, S}.

handle_cast(_Cast, S) ->
    {noreply, S}.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

blob_tx(S0, Ctx = #ds_tx_ctx{generation = GenId}, Ops) ->
    #s{dbshard = DBShard = {DB, _}, serial = SerRef} = S0,
    DispatchF = fun(Events) ->
        emqx_ds_beamformer:shard_event(DBShard, Events)
    end,
    maybe
        ok ?= emqx_ds_storage_layer_tx:verify_preconditions(DB, Ctx, Ops),
        CommitSerial = term_to_binary(new_serial(DBShard, SerRef)),
        {ok, CookedBatch} ?=
            emqx_ds_storage_layer:prepare_blob_tx(DBShard, GenId, CommitSerial, Ops, #{}),
        ok ?= emqx_ds_storage_layer:commit_batch(DBShard, CookedBatch, #{}),
        emqx_ds_storage_layer:dispatch_events(DBShard, CookedBatch, DispatchF),
        {ok, CommitSerial}
    end.

-spec do_store_batch_atomic(
    emqx_ds_storage_layer:dbshard(),
    emqx_ds:dsbatch(),
    emqx_ds:message_store_opts()
) ->
    emqx_ds:store_batch_result().
do_store_batch_atomic(DBShard, #dsbatch{} = Batch, StoreOpts) ->
    DBOpts = db_config(DBShard),
    #dsbatch{
        operations = Operations0,
        preconditions = Preconditions
    } = Batch,
    case emqx_ds_precondition:verify(emqx_ds_storage_layer, DBShard, Preconditions) of
        ok ->
            do_store_operations(DBShard, Operations0, DBOpts, StoreOpts);
        {precondition_failed, _} = PreconditionFailed ->
            {error, unrecoverable, PreconditionFailed};
        Error ->
            Error
    end;
do_store_batch_atomic(DBShard, Operations, StoreOpts) ->
    DBOpts = db_config(DBShard),
    do_store_operations(DBShard, Operations, DBOpts, StoreOpts).

do_store_operations(DBShard, Operations0, DBOpts, _StoreOpts) ->
    ForceMonotonic = maps:get(force_monotonic_timestamps, DBOpts),
    {Latest, Operations} =
        emqx_ds_builtin_local:make_batch(
            ForceMonotonic,
            current_timestamp(DBShard),
            Operations0
        ),
    Result = emqx_ds_storage_layer:store_batch(DBShard, Operations, _Options = #{}),
    emqx_ds_builtin_local_meta:set_current_timestamp(DBShard, Latest),
    Result.

db_config(#{db := DB}) ->
    emqx_ds_builtin_local_meta:db_config(DB).

current_timestamp(DBShard) ->
    emqx_ds_builtin_local_meta:current_timestamp(DBShard).

open_serial(DBShard) ->
    case emqx_ds_storage_layer:fetch_global(DBShard, ?serial_key) of
        {ok, <<Val:64>>} ->
            A = atomics:new(1, [{signed, false}]),
            atomics:put(A, 1, Val),
            A;
        not_found ->
            ok = emqx_ds_storage_layer:store_global(DBShard, #{?serial_key => <<0:64>>}, #{}),
            open_serial(DBShard)
    end.

new_serial(DBShard, A) ->
    Epoch = 1 bsl 16,
    case atomics:add_get(A, 1, 1) of
        Val when Val rem Epoch =:= 0 ->
            NextEpoch = <<(Val + Epoch):64>>,
            ok = emqx_ds_storage_layer:store_global(DBShard, #{?serial_key => NextEpoch}, #{}),
            Val;
        Val ->
            Val
    end.
