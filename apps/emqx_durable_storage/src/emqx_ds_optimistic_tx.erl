%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc This module implements a per-shard singleton process
%% facilitating optimistic transactions. This process is dynamically
%% spawned on the leader node.
%%
%% Optimistic transaction leader tracks recent writes and verifies
%% transactions from the clients. Transactions containing reads that
%% may race with the recently updated topics are rejected. Valid
%% transactions are "cooked" and added to the buffer.
%%
%% Buffer is periodically flushed in a single call to the backend.
%%
%% Leader is also tasked with keeping and incrementing the transaction
%% serial for the pending transactions. The latest serial is committed
%% to the storage together with the batches.
%%
%% Potential split brain situations are handled optimistically: the
%% backend can reject flush request.
-module(emqx_ds_optimistic_tx).

-behaviour(gen_statem).

%% API:
-export([
    start_link/3,

    where/2,
    global/2,

    dirty_append/2,

    new_kv_tx_ctx/5,
    commit_kv_tx/3,
    tx_commit_outcome/1,

    get_monotonic_timestamp/0
]).

%% Behaviour callbacks
-export([
    init/1,
    terminate/3,
    handle_event/4,
    callback_mode/0
]).

-export_type([
    tx/0,
    ctx/0,
    serial/0,
    runtime_config/0
]).

-include("emqx_ds.hrl").
-include("emqx_ds_builtin_tx.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%================================================================================
%% Type declarations
%%================================================================================

-type tx() :: #ds_tx{}.

-type serial() :: integer().

-type batch() :: {emqx_ds:generation(), [_CookedTx]}.

%% Get the last committed transaction ID on the local replica
-callback otx_get_tx_serial(emqx_ds:db(), emqx_ds:shard()) ->
    {ok, serial()} | undefined.

%% Cook a raw transaction into a data structure that will be stored in
%% the buffer until flush.
-callback otx_prepare_tx(
    {emqx_ds:db(), emqx_ds:shard()},
    emqx_ds:generation(),
    _SerialBin :: binary(),
    emqx_ds:tx_ops(),
    _MiscOpts :: map()
) ->
    {ok, _CookedTx} | emqx_ds:error(_).

%% Commit a batch of cooked transactions to the storage.
-callback otx_commit_tx_batch(
    {emqx_ds:db(), emqx_ds:shard()},
    _OldSerial :: serial(),
    _NewSerial :: serial(),
    _NewTimestamp :: emqx_ds:time(),
    batch()
) ->
    ok | emqx_ds:error(_).

%% Lookup a value identified by topic and timestamp. This callback is
%% executed only on the leader node.
-callback otx_lookup_ttv(
    {emqx_ds:db(), emqx_ds:shard()},
    emqx_ds:generation(),
    emqx_ds:topic(),
    emqx_ds:time()
) ->
    {ok, binary()} | undefined | emqx_ds:error(_).

%% Configuration that can be changed in the runtime.
-type runtime_config() :: #{
    flush_interval := non_neg_integer(),
    idle_flush_interval := non_neg_integer(),
    max_items := pos_integer(),
    conflict_window := pos_integer()
}.

%% Executed by the leader when it updates its runtime config
-callback otx_get_runtime_config(emqx_ds:db()) -> runtime_config().

%% Executed in the leader process when it attempts to step up.
-callback otx_become_leader(emqx_ds:db(), emqx_ds:shard()) ->
    {ok, serial(), emqx_ds:time()} | {error, _}.

%% Executed by the leader
-callback otx_get_latest_generation(emqx_ds:db(), emqx_ds:shard()) -> emqx_ds:generation().

%% Executed by the readers
-callback otx_get_leader(emqx_ds:db(), emqx_ds:shard()) -> pid() | undefined.

-type ctx() :: #kv_tx_ctx{}.

-define(name(DB, SHARD), {n, l, {?MODULE, DB, SHARD}}).
-define(via(DB, SHARD), {via, gproc, ?name(DB, SHARD)}).

%% States
-define(initial, initial).
-define(leader(SUBSTATE), {leader, SUBSTATE}).
-define(idle, idle).
-define(pending, pending).

-type leader_state() :: ?idle | ?pending.
%% Note: not including ?initial here because it shouldn't occur normally.
-type state() :: ?leader(leader_state()).

%% Timeouts
-define(timeout_initialize, timeout_initialize).
%%   Flush pending transactions to the storage:
-define(timeout_flush, timout_flush).

-record(gen_data, {
    dirty_w :: emqx_ds_tx_conflict_trie:t(),
    dirty_d :: emqx_ds_tx_conflict_trie:t(),
    buffer = [],
    pending_replies = []
}).

-type gen_data() :: #gen_data{}.

-type generations() :: #{emqx_ds:generation() => gen_data()}.

-record(d, {
    db :: emqx_ds:db(),
    shard :: emqx_ds:shard(),
    cbm :: module(),
    flush_interval :: pos_integer(),
    idle_flush_interval :: pos_integer(),
    rotate_interval :: pos_integer(),
    max_items :: pos_integer(),
    last_rotate_ts,
    n_items = 0,
    serial :: non_neg_integer() | undefined,
    timestamp :: emqx_ds:time() | undefined,
    committed_serial :: non_neg_integer() | undefined,
    pending_dirty = [] :: [dirty_append()],
    gens = #{} :: generations()
}).
-type d() :: #d{}.

-define(ds_tx_monotonic_ts, ds_tx_monotonic_ts).

%% Entry that is stored in the process dictionary of a process that
%% initiated commit of the transaction:
-record(client_pending_commit, {
    timeout_tref :: reference(),
    alias :: reference()
}).
-define(pending_commit, emqx_ds_optimistic_tx_pending_commit).

-define(global(DB, SHARD), {?MODULE, DB, SHARD}).

-record(cast_dirty_append, {
    reply_to :: pid() | undefined,
    ref :: reference() | undefined,
    data :: emqx_ds:dirty_append_data(),
    reserved
}).
-type dirty_append() :: #cast_dirty_append{}.

-type leader_loop_result() ::
    {next_state, ?leader(leader_state()), d(), [gen_statem:action()]}
    | {keep_state, d(), [gen_statem:action()] | gen_statem:action()}
    | {keep_state, d()}
    | keep_state_and_data.

%%================================================================================
%% API functions
%%================================================================================

-spec where(emqx_ds:db(), emqx_ds:shard()) -> pid() | undefined.
where(DB, Shard) ->
    gproc:where(?name(DB, Shard)).

-spec global(emqx_ds:db(), emqx_ds:shard()) -> pid() | undefined.
global(DB, Shard) ->
    global:whereis_name(?global(DB, Shard)).

-spec start_link(emqx_ds:db(), emqx_ds:shard(), module()) -> {ok, pid()}.
start_link(DB, Shard, CBM) ->
    gen_statem:start_link(?via(DB, Shard), ?MODULE, [DB, Shard, CBM], []).

-spec dirty_append(emqx_ds:dirty_append_opts(), emqx_ds:dirty_append_data()) ->
    reference() | noreply.
dirty_append(#{db := DB, shard := Shard} = Opts, Data = [{_, ?ds_tx_ts_monotonic, _} | _]) ->
    Reply = maps:get(reply, Opts, true),
    case global(DB, Shard) of
        Leader when is_pid(Leader) ->
            case Reply of
                true ->
                    ReplyTo = self(),
                    Result = Ref = monitor(process, Leader);
                false ->
                    ReplyTo = Ref = undefined,
                    Result = noreply
            end,
            gen_statem:cast(Leader, #cast_dirty_append{
                reply_to = ReplyTo,
                ref = Ref,
                data = Data
            }),
            Result;
        undefined when Reply ->
            Ref = make_ref(),
            reply_error(self(), Ref, dirty, recoverable, leader_down),
            Ref;
        undefined ->
            noreply
    end.

-spec new_kv_tx_ctx(
    module(),
    emqx_ds:db(),
    emqx_ds:shard(),
    emqx_ds:generation() | latest,
    emqx_ds:transaction_opts()
) -> {ok, ctx()} | emqx_ds:error(_).
new_kv_tx_ctx(CBM, DB, Shard, Generation0, Opts) ->
    case Generation0 of
        latest ->
            Generation = CBM:otx_get_latest_generation(DB, Shard),
            IsLatest = true;
        Generation when is_integer(Generation) ->
            IsLatest = false
    end,
    maybe
        Leader = CBM:otx_get_leader(DB, Shard),
        true ?= is_pid(Leader),
        {ok, Serial} ?= CBM:otx_get_tx_serial(DB, Shard),
        Ctx = #kv_tx_ctx{
            shard = Shard,
            leader = Leader,
            serial = Serial,
            generation = Generation,
            latest_generation = IsLatest,
            opts = Opts
        },
        {ok, Ctx}
    else
        _ ->
            ?err_rec(leader_down)
    end.

commit_kv_tx(DB, Ctx = #kv_tx_ctx{opts = #{timeout := Timeout}, shard = Shard}, Ops) ->
    ?tp(emqx_ds_optimistic_tx_commit_begin, #{db => DB, ctx => Ctx, ops => Ops}),
    case global(DB, Shard) of
        Leader when is_pid(Leader) ->
            #kv_tx_ctx{} = Ctx,
            Ref = monitor(process, Leader),
            TRef = emqx_ds_lib:send_after(Timeout, self(), tx_timeout_msg(Ref)),
            Alias = erlang:alias([reply]),
            put({?pending_commit, Ref}, #client_pending_commit{
                timeout_tref = TRef,
                alias = Alias
            }),
            gen_statem:cast(Leader, #ds_tx{
                ctx = Ctx, ops = Ops, from = Alias, ref = Ref, meta = TRef
            }),
            Ref;
        undefined ->
            Ref = make_ref(),
            self() ! ?ds_tx_commit_error(Ref, undefined, recoverable, leader_unavailable),
            Ref
    end.

-spec tx_commit_outcome(term()) -> {ok, emqx_ds:tx_serial()} | emqx_ds:error(_).
tx_commit_outcome(Reply) ->
    case Reply of
        ?ds_tx_commit_ok(Ref, _Meta, Serial) ->
            client_post_commit_cleanup(Ref),
            {ok, Serial};
        ?ds_tx_commit_error(Ref, _Meta, Class, Info) ->
            client_post_commit_cleanup(Ref),
            {error, Class, Info};
        {'DOWN', Ref, Type, Object, Info} ->
            client_post_commit_cleanup(Ref),
            ?err_rec({Type, Object, Info})
    end.

-doc """
Get shard's monotonic timestamp.

This function must be called only in the optimistic_tx process, inside
`with_time_and_generation` wrapper.
""".
-spec get_monotonic_timestamp() -> emqx_ds:time().
get_monotonic_timestamp() ->
    TS = max(get(?ds_tx_monotonic_ts) + 1, erlang:system_time(microsecond)),
    put(?ds_tx_monotonic_ts, TS),
    TS.

%%================================================================================
%% Behavior callbacks
%%================================================================================

callback_mode() ->
    [handle_event_function, state_enter].

init([DB, Shard, CBM]) ->
    erlang:process_flag(trap_exit, true),
    logger:update_process_metadata(#{db => DB, shard => Shard}),
    ?tp(info, ds_otx_start, #{db => DB, shard => Shard}),
    #{
        flush_interval := FI,
        idle_flush_interval := IFI,
        conflict_window := CW,
        max_items := MaxItems
    } = CBM:otx_get_runtime_config(DB),
    D = #d{
        db = DB,
        shard = Shard,
        cbm = CBM,
        rotate_interval = CW,
        last_rotate_ts = erlang:monotonic_time(millisecond),
        flush_interval = FI,
        idle_flush_interval = IFI,
        max_items = MaxItems
    },
    {ok, ?initial, D, {state_timeout, 0, ?timeout_initialize}}.

terminate(Reason, State, _Data) ->
    Level =
        case Reason =:= normal orelse Reason =:= shutdown of
            true ->
                info;
            false ->
                %% Sleep some to prevent a hot restart loop
                timer:sleep(1_000),
                error
        end,
    ?tp(Level, ds_otx_terminate, #{state => State, reason => Reason}).

handle_event(info, {'EXIT', _, Reason}, _State, _Data) ->
    case Reason of
        normal -> keep_state_and_data;
        _ -> {stop, shutdown}
    end;
handle_event(enter, _OldState, ?leader(?pending), #d{flush_interval = T}) ->
    %% Schedule unconditional flush after the given interval:
    {keep_state_and_data, {state_timeout, T, ?timeout_flush}};
handle_event(enter, _, _, _) ->
    keep_state_and_data;
handle_event(state_timeout, ?timeout_initialize, ?initial, D) ->
    async_init(D);
handle_event(ET, Evt, ?initial, _) ->
    ?tp(warning, unexpected_event_in_initial_state, #{ET => Evt}),
    {keep_state_and_data, postpone};
handle_event(cast, Tx = #ds_tx{}, ?leader(LeaderState), D0) ->
    %% Enqueue transaction commit:
    case handle_tx(D0, Tx) of
        {ok, _PresumedCommitSerial, D} ->
            finalize_add_pending(LeaderState, D, []);
        aborted ->
            keep_state_and_data
    end;
handle_event(cast, #ds_tx{from = From, ref = Ref, meta = Meta}, _Other, _) ->
    reply_error(From, Ref, Meta, recoverable, not_the_leader),
    keep_state_and_data;
handle_event(cast, #cast_dirty_append{} = DirtyAppend, State, D) ->
    handle_dirty_append(DirtyAppend, State, D);
handle_event(ET, ?timeout_flush, ?leader(_LeaderState), D) when
    ET =:= state_timeout; ET =:= timeout
->
    handle_flush(D, []);
handle_event(ET, Event, State, _D) ->
    ?tp(
        error,
        emqx_ds_tx_serializer_unknown_event,
        #{ET => Event, state => State}
    ),
    keep_state_and_data.

%%================================================================================
%% Internal functions
%%================================================================================

-spec async_init(d()) -> leader_loop_result() | {stop, _}.
async_init(D = #d{db = DB, shard = Shard, cbm = CBM}) ->
    maybe
        {ok, Serial, Timestamp} ?= CBM:otx_become_leader(DB, Shard),
        global:unregister_name(?global(DB, Shard)),
        yes = global:register_name(?global(DB, Shard), self()),
        %% Issue a dummy transaction to trigger metadata update:
        ok ?= CBM:otx_commit_tx_batch({DB, Shard}, Serial, Serial, Timestamp, []),
        ?tp(info, ds_otx_up, #{serial => Serial, db => DB, shard => Shard, ts => Timestamp}),
        {next_state, ?leader(?idle), D#d{
            serial = Serial,
            timestamp = Timestamp,
            committed_serial = Serial
        }}
    else
        Err ->
            {stop, {init_failed, Err}}
    end.

-spec handle_flush(d(), [gen_statem:action()]) ->
    leader_loop_result().
handle_flush(D0, Actions) ->
    %% Execute flush. After flushing the buffer it's safe to rotate
    %% the conflict tree.
    D = maybe_rotate(flush(D0)),
    {next_state, ?leader(?idle), D, Actions}.

-spec handle_dirty_append(dirty_append(), state(), d()) -> leader_loop_result().
handle_dirty_append(DirtyAppend, ?leader(LeaderState), D0 = #d{pending_dirty = Buff}) ->
    %% Note: all dirty appends are cooked in a single `otx_prepare_tx` call during the flush:
    D = D0#d{pending_dirty = [DirtyAppend | Buff]},
    finalize_add_pending(LeaderState, D, []).

-spec send_dirty_append_replies(ok | false | emqx_ds:error(_), serial(), [dirty_append()]) -> ok.
send_dirty_append_replies(Result, Serial, PendingReplies) ->
    case Result of
        ok ->
            lists:foreach(
                fun(#cast_dirty_append{reply_to = From, ref = Ref, reserved = Reserved}) ->
                    reply_success(From, Ref, Reserved, Serial)
                end,
                PendingReplies
            );
        Other ->
            case Other of
                false ->
                    %% Failed to cook:
                    ErrClass = recoverable,
                    Error = aborted;
                {error, ErrClass, Error} ->
                    ok
            end,
            lists:foreach(
                fun(#cast_dirty_append{reply_to = From, ref = Ref, reserved = Reserved}) ->
                    reply_error(From, Ref, Reserved, ErrClass, Error)
                end,
                PendingReplies
            )
    end.

-spec cook_dirty_appends(d()) -> {Success, serial() | undefined, [dirty_append()], d()} when
    Success :: boolean().
cook_dirty_appends(D = #d{pending_dirty = []}) ->
    {true, undefined, [], D};
cook_dirty_appends(D0 = #d{db = DB, shard = Shard, cbm = CBM, pending_dirty = Pending}) ->
    Gen = CBM:otx_get_latest_generation(DB, Shard),
    Result = with_time_and_generation(
        Gen,
        D0,
        fun(GenData, PresumedCommitSerial) ->
            do_cook_dirty_appends(DB, Shard, CBM, Gen, GenData, PresumedCommitSerial, Pending)
        end
    ),
    %% Optimization: here we return the original `Pending' list to be
    %% passed to `send_dirty_append_replies' function. This should
    %% prevent extra list allocations when sending replies.
    case Result of
        {ok, PresumedCommitSerial, D} ->
            {true, PresumedCommitSerial, Pending, D#d{pending_dirty = []}};
        aborted ->
            {false, undefined, Pending, D0#d{pending_dirty = []}}
    end.

do_cook_dirty_appends(DB, Shard, CBM, Gen, GenData, PresumedCommitSerial, Pending) ->
    maybe
        #gen_data{
            buffer = Buff,
            pending_replies = _Pending0
        } = GenData,
        Writes = lists:foldl(
            fun(#cast_dirty_append{data = Data}, Acc) ->
                Data ++ Acc
            end,
            [],
            Pending
        ),
        {ok, CookedDirty} ?=
            CBM:otx_prepare_tx(
                {DB, Shard},
                Gen,
                serial_bin(PresumedCommitSerial),
                #{?ds_tx_write => Writes},
                #{}
            ),
        {ok, GenData#gen_data{
            buffer = [CookedDirty | Buff]
        }}
    else
        _ -> aborted
    end.

-doc """
This function must be called after adding a new item pending for commit.
It (re)schedules flushing of the data.
""".
-spec finalize_add_pending(leader_state(), d(), [gen_statem:action()]) ->
    leader_loop_result().
finalize_add_pending(_, D = #d{n_items = N, max_items = MaxItems}, Actions) when
    N >= MaxItems, MaxItems > 0
->
    handle_flush(D, Actions);
finalize_add_pending(?idle, D = #d{idle_flush_interval = IdleInterval}, Actions) ->
    %% Schedule early flush if the shard is idle:
    Timeout = {timeout, IdleInterval, ?timeout_flush},
    {next_state, ?leader(?pending), D, [Timeout | Actions]};
finalize_add_pending(_, D, Actions) ->
    {keep_state, D, Actions}.

-doc """
A wrapper that sets up the environment for calling CBM:tx_prepare_tx.

If successful, it updates generation data, current monotonic time and
commit serial in the global state.
""".
-spec with_time_and_generation(emqx_ds:generation(), d(), fun(
    (gen_data(), serial()) -> {ok, gen_data()} | aborted
)) ->
    {ok, serial(), d()} | aborted.
with_time_and_generation(
    Gen,
    D = #d{
        n_items = NItems,
        serial = Serial,
        timestamp = Timestamp,
        gens = Gens
    },
    Fun
) ->
    case Gens of
        #{Gen := GenData0} ->
            ok;
        #{} ->
            GenData0 = #gen_data{
                dirty_w = emqx_ds_tx_conflict_trie:new(Serial, infinity),
                dirty_d = emqx_ds_tx_conflict_trie:new(Serial, infinity)
            }
    end,
    PresumedCommitSerial = Serial + 1,
    put(?ds_tx_monotonic_ts, Timestamp),
    case Fun(GenData0, PresumedCommitSerial) of
        {ok, GenData} ->
            {ok, PresumedCommitSerial, D#d{
                serial = PresumedCommitSerial,
                gens = Gens#{Gen => GenData},
                timestamp = erase(?ds_tx_monotonic_ts),
                n_items = NItems + 1
            }};
        aborted ->
            _ = erase(?ds_tx_monotonic_ts),
            aborted
    end.

-spec handle_tx(d(), tx()) -> {ok, serial(), d()} | aborted.
handle_tx(
    D = #d{
        db = DB,
        shard = Shard,
        cbm = CBM,
        serial = Serial,
        committed_serial = SafeToReadSerial
    },
    Tx
) ->
    #ds_tx{
        ref = TxRef,
        ctx = #kv_tx_ctx{generation = Gen, latest_generation = IsLatest}
    } = Tx,
    with_time_and_generation(
        Gen,
        D,
        fun(GenData, PresumedCommitSerial) ->
            DBShard = {DB, Shard},
            ?tp(
                emqx_ds_optimistic_tx_commit_received,
                #{
                    shard => DBShard,
                    serial => Serial,
                    committed_serial => SafeToReadSerial,
                    ref => TxRef,
                    tx => Tx,
                    generation => {IsLatest, Gen}
                }
            ),
            try_schedule_transaction(
                DBShard, Gen, CBM, SafeToReadSerial, PresumedCommitSerial, Tx, GenData
            )
        end
    ).

-doc """
This function verifies that a transaction doesn't conflict with data
that was either committed or is scheduled for commit, and that
transaction preconditions are satisfied.

If all checks pass then the transaction is cooked and scheduled for
commit, and conflict tracking is updated.
""".
-spec try_schedule_transaction(
    {emqx_ds:db(), emqx_ds:shard()},
    emqx_ds:generation(),
    module(),
    serial(),
    serial(),
    tx(),
    gen_data()
) ->
    {ok, gen_data()} | aborted.
try_schedule_transaction(
    DBShard,
    Gen,
    CBM,
    SafeToReadSerial,
    PresumedCommitSerial,
    Tx,
    GS = #gen_data{dirty_w = DirtyW0, dirty_d = DirtyD0, buffer = Buff, pending_replies = Pending}
) ->
    #ds_tx{
        ctx = Ctx,
        ops = Ops0,
        from = From,
        ref = Ref,
        meta = Meta
    } = Tx,
    Ops = preprocess_ops(PresumedCommitSerial, Ops0),
    #kv_tx_ctx{serial = TxStartSerial, leader = TxLeader} = Ctx,
    maybe
        ok ?= check_tx_leader(TxLeader),
        ok ?= check_latest_generation(CBM, DBShard, Ctx),
        ok ?= check_conflicts(DirtyW0, DirtyD0, TxStartSerial, SafeToReadSerial, Ops),
        ok ?= verify_preconditions(DBShard, CBM, Gen, Ops),
        {ok, CookedTx} ?=
            CBM:otx_prepare_tx(
                DBShard, Gen, serial_bin(PresumedCommitSerial), Ops, #{}
            ),
        ?tp(emqx_ds_optimistic_tx_commit_pending, #{ref => Ref}),
        {ok, GS#gen_data{
            dirty_w = update_dirty_w(PresumedCommitSerial, Ops, DirtyW0),
            dirty_d = update_dirty_d(PresumedCommitSerial, Ops, DirtyD0),
            buffer = [CookedTx | Buff],
            pending_replies = [{From, Ref, Meta, PresumedCommitSerial} | Pending]
        }}
    else
        {error, Class, Error} ->
            ?tp(
                debug,
                emqx_ds_optimistic_tx_commit_abort,
                #{
                    ref => Ref, ec => Class, reason => Error, stage => prepare
                }
            ),
            reply_error(From, Ref, Meta, Class, Error),
            aborted
    end.

-doc """
Check that the transaction was created while this process was the leader.
""".
-spec check_tx_leader(pid()) -> ok | emqx_ds:error(_).
check_tx_leader(TxLeader) ->
    case self() of
        TxLeader ->
            ok;
        _ ->
            ?err_rec({leader_conflict, TxLeader, self()})
    end.

-spec check_latest_generation(module(), {emqx_ds:db(), emqx_ds:shard()}, ctx()) ->
    ok | emqx_ds:error(_).
check_latest_generation(_, _, #kv_tx_ctx{latest_generation = false}) ->
    %% This transaction was created for a constant generation that
    %% doesn't have to be the latest. This always succeeds.
    ok;
check_latest_generation(CBM, {DB, Shard}, #kv_tx_ctx{latest_generation = true, generation = Gen}) ->
    case CBM:otx_get_latest_generation(DB, Shard) of
        Gen ->
            ok;
        Latest ->
            ?err_rec({not_the_latest_generation, Gen, Latest})
    end.

-spec update_dirty_d(serial(), emqx_ds:tx_ops(), emqx_ds_tx_conflict_trie:t()) ->
    emqx_ds_tx_conflict_trie:t().
update_dirty_d(Serial, Ops, Dirty) ->
    %% Mark all deleted topics as dirty:
    lists:foldl(
        fun({TF, FromTime, ToTime}, Acc) ->
            emqx_ds_tx_conflict_trie:push_topic(
                emqx_ds_tx_conflict_trie:topic_filter_to_conflict_domain(TF),
                FromTime,
                ToTime,
                Serial,
                Acc
            )
        end,
        Dirty,
        maps:get(?ds_tx_delete_topic, Ops, [])
    ).

-spec update_dirty_w(serial(), emqx_ds:tx_ops(), emqx_ds_tx_conflict_trie:t()) ->
    emqx_ds_tx_conflict_trie:t().
update_dirty_w(Serial, Ops, Dirty) ->
    %% Mark written topics as dirty:
    lists:foldl(
        fun({TF, TS, _Val}, Acc) ->
            emqx_ds_tx_conflict_trie:push_topic(TF, TS, TS, Serial, Acc)
        end,
        Dirty,
        maps:get(?ds_tx_write, Ops, [])
    ).

-spec flush(d()) -> d().
flush(D0) ->
    {CookDirtySuccess, PresumedDirtyCommitSerial, OldDirtyAppends, D} = cook_dirty_appends(D0),
    #d{
        db = DB,
        shard = Shard,
        cbm = CBM,
        committed_serial = SerCtl,
        serial = Serial,
        gens = Gens0,
        timestamp = Timestamp
    } = D,
    DBShard = {DB, Shard},
    ?tp(debug, emqx_ds_optimistic_tx_flush, #{}),
    Batch = make_batch(Gens0),
    Result = CBM:otx_commit_tx_batch(DBShard, SerCtl, Serial, Timestamp, Batch),
    Gens = clean_buffers_and_reply(Result, Gens0),
    send_dirty_append_replies(
        CookDirtySuccess andalso Result, PresumedDirtyCommitSerial, OldDirtyAppends
    ),
    #{
        flush_interval := FI,
        idle_flush_interval := IFI,
        max_items := MaxItems,
        conflict_window := CW
    } = CBM:otx_get_runtime_config(DB),
    case Result of
        ok ->
            erlang:garbage_collect(),
            D#d{
                committed_serial = Serial,
                gens = Gens,
                flush_interval = FI,
                idle_flush_interval = IFI,
                max_items = MaxItems,
                n_items = 0,
                rotate_interval = CW
            };
        _ ->
            exit({flush_failed, #{db => DB, shard => Shard, result => Result}})
    end.

-spec make_batch(generations()) -> batch().
make_batch(Generations) ->
    maps:fold(
        fun(Generation, #gen_data{buffer = Buf}, Acc) ->
            case Buf of
                [] ->
                    %% Avoid touching generations that don't have
                    %% data. They could be deleted. TODO: drop deleted
                    %% generations from the state record.
                    Acc;
                _ ->
                    [{Generation, Buf} | Acc]
            end
        end,
        [],
        Generations
    ).

-spec clean_buffers_and_reply(ok | emqx_ds:error(_), generations()) -> generations().
clean_buffers_and_reply(Reply, Generations) ->
    maps:map(
        fun(_, GenData = #gen_data{pending_replies = Waiting}) ->
            _ =
                case Reply of
                    ok ->
                        [
                            reply_success(From, Ref, Meta, CommitSerial)
                         || {From, Ref, Meta, CommitSerial} <- Waiting
                        ];
                    {error, Class, Err} ->
                        [
                            reply_error(From, Ref, Meta, Class, Err)
                         || {From, Ref, Meta, _CommitSerial} <- Waiting
                        ]
                end,
            GenData#gen_data{
                buffer = [],
                pending_replies = []
            }
        end,
        Generations
    ).

-spec serial_bin(serial()) -> binary().
serial_bin(A) ->
    <<A:128>>.

-spec maybe_rotate(d()) -> d().
maybe_rotate(D = #d{rotate_interval = RI, last_rotate_ts = LastRotTS}) ->
    case erlang:monotonic_time(millisecond) - LastRotTS > RI of
        true ->
            rotate(D);
        false ->
            D
    end.

-spec rotate(d()) -> d().
rotate(D = #d{gens = Gens0}) ->
    Gens = maps:map(
        fun(_, GS = #gen_data{dirty_w = Dirty}) ->
            GS#gen_data{
                dirty_w = emqx_ds_tx_conflict_trie:rotate(Dirty)
            }
        end,
        Gens0
    ),
    D#d{
        gens = Gens,
        last_rotate_ts = erlang:monotonic_time(millisecond)
    }.

-spec preprocess_ops(serial(), emqx_ds:tx_ops()) -> emqx_ds:tx_ops().
preprocess_ops(_PresumedCommitSerial, Tx) ->
    MonotonicTS = get_monotonic_timestamp(),
    maps:map(
        fun
            (?ds_tx_delete_topic, Ops) ->
                preprocess_deletes(MonotonicTS, Ops);
            (_Key, Ops) ->
                Ops
        end,
        Tx
    ).

-spec preprocess_deletes(emqx_ds:time(), [emqx_ds:topic_range()]) ->
    [{emqx_ds:topic(), emqx_ds:time(), emqx_ds:time() | infinity}].
preprocess_deletes(MonotonicTS, Ops) ->
    lists:map(
        fun
            ({TopicFilter, From, ?ds_tx_ts_monotonic}) ->
                {TopicFilter, From, MonotonicTS};
            (A) ->
                A
        end,
        Ops
    ).

-spec check_conflicts(
    emqx_ds_tx_conflict_trie:t(), emqx_ds_tx_conflict_trie:t(), serial(), serial(), emqx_ds:tx_ops()
) ->
    ok | emqx_ds:error(_).
check_conflicts(DirtyW, DirtyD, TxStartSerial, SafeToReadSerial, Ops) ->
    maybe
        %% Check reads against writes and deletions:
        Reads = maps:get(?ds_tx_read, Ops, []),
        ok ?= do_check_conflicts(DirtyW, TxStartSerial, Reads),
        ok ?= do_check_conflicts(DirtyD, TxStartSerial, Reads),
        %% Deletion of topics involves scanning of the storage. We
        %% can't do it when there is potentially buffered data:
        ok ?= do_check_conflicts(DirtyW, SafeToReadSerial, maps:get(?ds_tx_delete_topic, Ops, [])),
        %% Verifying precondition requires reading from the storage
        %% too. Make sure we don't ignore the cache:
        ExpectedTopics = [{Topic, TS, TS} || {Topic, TS, _} <- maps:get(?ds_tx_expected, Ops, [])],
        ok ?= do_check_conflicts(DirtyW, SafeToReadSerial, ExpectedTopics),
        ok ?= do_check_conflicts(DirtyD, SafeToReadSerial, ExpectedTopics),
        UnexpectedTopics = [{Topic, TS, TS} || {Topic, TS} <- maps:get(?ds_tx_unexpected, Ops, [])],
        ok ?= do_check_conflicts(DirtyW, SafeToReadSerial, UnexpectedTopics)
    end.

-spec do_check_conflicts(emqx_ds_tx_conflict_trie:t(), serial(), [
    {emqx_ds:topic(), emqx_ds:time(), emqx_ds:time() | infinity}
]) -> ok | ?err_rec(_).
do_check_conflicts(Dirty, Serial, Topics) ->
    Errors = lists:filtermap(
        fun({ReadTF, FromTime, ToTime}) ->
            emqx_ds_tx_conflict_trie:is_dirty_topic(
                emqx_ds_tx_conflict_trie:topic_filter_to_conflict_domain(ReadTF),
                FromTime,
                ToTime,
                Serial,
                Dirty
            ) andalso
                {true, {ReadTF, {FromTime, ToTime}, Serial}}
        end,
        Topics
    ),
    case Errors of
        [] ->
            ok;
        _ ->
            ?err_rec({read_conflict, Errors})
    end.

-spec verify_preconditions(
    {emqx_ds:db(), emqx_ds:shard()}, module(), emqx_ds:generation(), emqx_ds:tx_ops()
) ->
    ok | ?err_unrec(_).
verify_preconditions(DBShard, CBM, GenId, Ops) ->
    %% Verify expected values:
    Unrecoverable0 = lists:foldl(
        fun({Topic, Time, ExpectedValue}, Acc) ->
            case CBM:otx_lookup_ttv(DBShard, GenId, Topic, Time) of
                {ok, Value} when
                    ExpectedValue =:= '_';
                    ExpectedValue =:= Value
                ->
                    Acc;
                {ok, Value} ->
                    [#{topic => Topic, expected => ExpectedValue, ts => Time, got => Value} | Acc];
                undefined ->
                    [
                        #{topic => Topic, expected => ExpectedValue, ts => Time, got => undefined}
                        | Acc
                    ]
            end
        end,
        [],
        maps:get(?ds_tx_expected, Ops, [])
    ),
    %% Verify unexpected values:
    Unrecoverable = lists:foldl(
        fun({Topic, Time}, Acc) ->
            case CBM:otx_lookup_ttv(DBShard, GenId, Topic, Time) of
                undefined ->
                    Acc;
                {ok, Value} ->
                    [#{topic => Topic, ts => Time, unexpected => Value} | Acc]
            end
        end,
        Unrecoverable0,
        maps:get(?ds_tx_unexpected, Ops, [])
    ),
    case Unrecoverable of
        [] ->
            ok;
        _ ->
            ?err_unrec({precondition_failed, Unrecoverable})
    end.

tx_timeout_msg(Ref) ->
    ?ds_tx_commit_error(Ref, undefined, unrecoverable, commit_timeout).

reply_success(From, Ref, Meta, Serial) ->
    ?tp(
        emqx_ds_optimistic_tx_commit_success,
        #{client => From, ref => Ref, meta => Meta, serial => Serial}
    ),
    case From of
        _ when is_pid(From) orelse is_reference(From) ->
            From ! ?ds_tx_commit_ok(Ref, Meta, serial_bin(Serial)),
            ok;
        undefined ->
            ok
    end.

reply_error(From, Ref, Meta, Class, Error) ->
    ?tp(
        debug,
        emqx_ds_optimistic_tx_commit_abort,
        #{client => From, ref => Ref, meta => Meta, ec => Class, reason => Error, stage => final}
    ),
    case From of
        _ when is_pid(From) orelse is_reference(From) ->
            From ! ?ds_tx_commit_error(Ref, Meta, Class, Error),
            ok;
        undefined ->
            ok
    end.

-spec client_post_commit_cleanup(reference()) -> ok.
client_post_commit_cleanup(Ref) ->
    demonitor(Ref),
    case erase({?pending_commit, Ref}) of
        undefined ->
            ok;
        #client_pending_commit{timeout_tref = TRef, alias = Alias} ->
            _ = erlang:unalias(Alias),
            _ = is_reference(TRef) andalso erlang:cancel_timer(TRef),
            ok
    end,
    flush_messages(Ref).

-spec flush_messages(reference()) -> ok.
flush_messages(Ref) ->
    receive
        {'DOWN', Ref, _Type, _Obj, _Info} ->
            flush_messages(Ref)
    after 0 ->
        ok
    end.

%%================================================================================
%% Tests
%%================================================================================
