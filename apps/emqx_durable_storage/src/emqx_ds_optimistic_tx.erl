%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc This module serves the same role as `emqx_ds_buffer', but it's
%% suited for transactions. Also, it runs on the leader node as
%% opposed to the buffer.
-module(emqx_ds_optimistic_tx).

-behaviour(gen_statem).

%% API:
-export([
    start_link/3,

    where/2,

    new_kv_tx_ctx/6,
    commit_kv_tx/3
]).

%% Behaviour callbacks
-export([
    init/1,
    handle_event/4,
    callback_mode/0
]).

-export_type([
    ctx/0,
    serial/0
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

-type serial() :: integer().

-callback get_tx_serial({emqx_ds:db(), emqx_ds:shard()}) -> serial().

-callback prepare_kv_tx(
    {emqx_ds:db(), emqx_ds:shard()}, emqx_ds:generation(), binary(), emqx_ds:kv_tx_ops(), _
) ->
    {ok, _CookedTx}
    | emqx_ds:error(_).

-callback commit_kv_tx_batch({emqx_ds:db(), emqx_ds:shard()}, emqx_ds:generation(), serial(), [
    _CookedTx
]) ->
    ok
    | emqx_ds:error(_).

-callback update_tx_serial({emqx_ds:db(), emqx_ds:shard()}, serial(), serial()) ->
    ok
    | emqx_ds:error(_).

-type ctx() :: #kv_tx_ctx{}.

-define(name(DB, SHARD), {n, l, {?MODULE, DB, SHARD}}).
-define(via(DB, SHARD), {via, gproc, ?name(DB, SHARD)}).

%% States
-define(leader(SUBSTATE), {leader, SUBSTATE}).
-define(idle, idle).
-define(pending, pending).

%% Timeouts
%%   Flush pending transactions to the storage:
-define(timeout_flush, timout_flush).

-record(gen_data, {
    dirty :: emqx_ds_tx_conflict_trie:t(),
    buffer = [],
    pending_replies = []
}).

-type gen_data() :: #gen_data{}.

-record(d, {
    dbshard,
    cbm,
    flush_interval,
    rotate_interval,
    last_rotate_ts,
    serial,
    committed_serial,
    gens = #{} :: #{emqx_ds:generation() => gen_data()}
}).

%%================================================================================
%% API functions
%%================================================================================

-spec where(emqx_ds:db(), emqx_ds:shard()) -> pid() | undefined.
where(DB, Shard) ->
    gproc:whereis_name(?name(DB, Shard)).

-spec start_link(emqx_ds:db(), emqx_ds:shard(), module()) -> {ok, pid()}.
start_link(DB, Shard, CBM) ->
    gen_statem:start_link(?via(DB, Shard), ?MODULE, [DB, Shard, CBM], []).

-spec new_kv_tx_ctx(
    emqx_ds:db(), emqx_ds:shard(), emqx_ds:generation(), pid(), emqx_ds:transaction_opts(), serial()
) ->
    ctx().
new_kv_tx_ctx(_DB, Shard, Generation, Leader, Options, Serial) ->
    #kv_tx_ctx{
        shard = Shard,
        leader = Leader,
        serial = Serial,
        generation = Generation,
        opts = Options
    }.

commit_kv_tx(DB, Ctx = #kv_tx_ctx{opts = #{timeout := Timeout}}, Ops) ->
    ?tp(emqx_ds_optimistic_tx_commit_begin, #{db => DB, ctx => Ctx, ops => Ops}),
    #kv_tx_ctx{leader = Leader} = Ctx,
    Ref = monitor(process, Leader),
    %% Note: currently timer is not canceled when commit abort is
    %% signalled via the monitor message (i.e. when the leader process
    %% is down). In this case the caller _will_ receive double `DOWN'.
    %% There's no trivial way to fix it. It's up to the client to
    %% track the pending transactions and ignore unexpected commit
    %% notifications.
    TRef = emqx_ds_lib:send_after(Timeout, self(), tx_timeout_msg(Ref)),
    gen_statem:cast(Leader, #ds_tx{
        ctx = Ctx, ops = Ops, from = self(), ref = Ref, meta = TRef
    }),
    Ref.

%%================================================================================
%% Behavior callbacks
%%================================================================================

callback_mode() ->
    [handle_event_function, state_enter].

init([DB, Shard, CBM]) ->
    RI = 5_000,
    Serial = CBM:get_tx_serial({DB, Shard}),
    {ok, ?leader(?idle), #d{
        dbshard = {DB, Shard},
        cbm = CBM,
        rotate_interval = RI,
        last_rotate_ts = erlang:monotonic_time(millisecond),
        %% FIXME: hardcode
        flush_interval = 10,
        serial = Serial,
        committed_serial = Serial
    }}.

handle_event(enter, _OldState, ?leader(?pending), #d{flush_interval = T}) ->
    %% Schedule unconditional flush after the given interval:
    {keep_state_and_data, {state_timeout, T, ?timeout_flush}};
handle_event(enter, _, _, _) ->
    keep_state_and_data;
handle_event(cast, Tx = #ds_tx{}, ?leader(State), D0) ->
    %% Enqueue transaction commit:
    case handle_tx(D0, Tx) of
        {ok, D} ->
            %% Schedule early flush if the shard is idle:
            IdleInterval = 1,
            Timeout = {timeout, IdleInterval, ?timeout_flush},
            case State of
                ?idle ->
                    {next_state, ?leader(?pending), D, Timeout};
                _ ->
                    {keep_state, D, Timeout}
            end;
        aborted ->
            keep_state_and_data
    end;
handle_event(cast, #ds_tx{from = From, ref = Ref, meta = Meta}, _Other, _) ->
    reply_error(From, Ref, Meta, recoverable, not_the_leader),
    keep_state_and_data;
handle_event(ET, ?timeout_flush, ?leader(_State), D0) when ET =:= state_timeout; ET =:= timeout ->
    %% Execute flush. After flushing the buffer it's safe to rotate
    %% the conflict tree.
    D = maybe_rotate(flush(D0)),
    {next_state, ?leader(?idle), D};
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

handle_tx(
    D = #d{
        dbshard = DBShard,
        cbm = CBM,
        serial = Serial,
        committed_serial = SafeToReadSerial,
        gens = Gens
    },
    Tx
) ->
    #ds_tx{ref = TxRef, ctx = #kv_tx_ctx{generation = Gen}} = Tx,
    ?tp(
        emqx_ds_optimistic_tx_commit_received,
        #{
            shard => DBShard,
            serial => Serial,
            committed_serial => SafeToReadSerial,
            ref => TxRef,
            tx => Tx
        }
    ),
    case Gens of
        #{Gen := GS0} ->
            ok;
        #{} ->
            GS0 = #gen_data{dirty = emqx_ds_tx_conflict_trie:new(Serial, infinity)}
    end,
    PresumedCommitSerial = Serial + 1,
    case try_commit(DBShard, Gen, CBM, SafeToReadSerial, PresumedCommitSerial, Tx, GS0) of
        {ok, GS} ->
            ?tp(emqx_ds_optimistic_tx_commit_pending, #{ref => TxRef}),
            {ok, D#d{
                serial = PresumedCommitSerial,
                gens = Gens#{Gen => GS}
            }};
        aborted ->
            aborted
    end.

try_commit(
    DBShard,
    Gen,
    CBM,
    SafeToReadSerial,
    PresumedCommitSerial,
    Tx,
    GS = #gen_data{dirty = Dirty0, buffer = Buff, pending_replies = Pending}
) ->
    #ds_tx{
        ctx = Ctx,
        ops = Ops,
        from = From,
        ref = Ref,
        meta = Meta
    } = Tx,
    #kv_tx_ctx{serial = TxStartSerial} = Ctx,
    maybe
        ok ?= check_conflicts(Dirty0, TxStartSerial, SafeToReadSerial, Ops),
        ok ?= verify_preconditions(DBShard, Gen, Ops),
        {ok, CookedTx} ?=
            CBM:prepare_kv_tx(
                DBShard, Gen, serial_bin(PresumedCommitSerial), Ops, #{}
            ),
        Dirty = update_dirty(PresumedCommitSerial, Ops, Dirty0),
        {ok, GS#gen_data{
            dirty = Dirty,
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

update_dirty(Serial, Ops, Dirty0) ->
    %% Mark all deleted topics as dirty:
    Dirty1 = lists:foldl(
        fun(TF, Acc) ->
            emqx_ds_tx_conflict_trie:push(
                emqx_ds_tx_conflict_trie:topic_filter_to_conflict_domain(TF),
                Serial,
                Acc
            )
        end,
        Dirty0,
        maps:get(?ds_tx_delete_topic, Ops, [])
    ),
    %% Mark written topics as dirty:
    lists:foldl(
        fun({TF, _Val}, Acc) ->
            emqx_ds_tx_conflict_trie:push(TF, Serial, Acc)
        end,
        Dirty1,
        maps:get(?ds_tx_write, Ops, [])
    ).

flush(
    D = #d{dbshard = DBShard, cbm = CBM, committed_serial = SerCtl, serial = Serial, gens = Gens0}
) ->
    Gens = maps:map(
        fun(Gen, GS = #gen_data{buffer = Buf, pending_replies = Replies, dirty = Dirty}) ->
            ?tp(emqx_ds_optimistic_tx_flush, #{
                shard => DBShard,
                generation => Gen,
                buffer => Buf,
                conflict_tree => emqx_ds_tx_conflict_trie:print(Dirty)
            }),
            _ =
                case CBM:commit_kv_tx_batch(DBShard, Gen, SerCtl, Buf) of
                    ok ->
                        [
                            reply_success(From, Ref, Meta, CommitSerial)
                         || {From, Ref, Meta, CommitSerial} <- Replies
                        ];
                    {error, Class, Err} ->
                        [
                            reply_error(From, Ref, Meta, Class, Err)
                         || {From, Ref, Meta, _CommitSerial} <- Replies
                        ]
                end,
            GS#gen_data{
                dirty = Dirty,
                buffer = [],
                pending_replies = []
            }
        end,
        Gens0
    ),
    ok = CBM:update_tx_serial(DBShard, SerCtl, Serial),
    D#d{
        committed_serial = Serial,
        gens = Gens
    }.

serial_bin(A) ->
    <<A:128>>.

maybe_rotate(D = #d{rotate_interval = RI, last_rotate_ts = LastRotTS}) ->
    case erlang:monotonic_time(millisecond) - LastRotTS > RI of
        true ->
            rotate(D);
        false ->
            D
    end.

rotate(D = #d{gens = Gens0}) ->
    Gens = maps:map(
        fun(_, GS = #gen_data{dirty = Dirty}) ->
            GS#gen_data{
                dirty = emqx_ds_tx_conflict_trie:rotate(Dirty)
            }
        end,
        Gens0
    ),
    D#d{
        gens = Gens,
        last_rotate_ts = erlang:monotonic_time(millisecond)
    }.

check_conflicts(Dirty, TxStartSerial, SafeToReadSerial, Ops) ->
    maybe
        ok ?= do_check_conflicts(Dirty, TxStartSerial, maps:get(?ds_tx_read, Ops, [])),
        %% Deletion of topics involves scanning of the storage. We
        %% can't do it when there is potentially buffered data:
        ok ?= do_check_conflicts(Dirty, SafeToReadSerial, maps:get(?ds_tx_delete_topic, Ops, [])),
        %% Verifying precondition requires reading from the storage
        %% too. Make sure we don't ignore the cache:
        ok ?= do_check_conflicts(Dirty, SafeToReadSerial, maps:get(?ds_tx_unexpected, Ops, [])),
        ExpectedTopics = [Topic || {Topic, _} <- maps:get(?ds_tx_expected, Ops, [])],
        ok ?= do_check_conflicts(Dirty, SafeToReadSerial, ExpectedTopics)
    end.

do_check_conflicts(Dirty, Serial, Topics) ->
    Errors = lists:foldl(
        fun(ReadTF, Acc) ->
            case
                emqx_ds_tx_conflict_trie:is_dirty(
                    emqx_ds_tx_conflict_trie:topic_filter_to_conflict_domain(ReadTF),
                    Serial,
                    Dirty
                )
            of
                false ->
                    Acc;
                true ->
                    [{ReadTF, Serial} | Acc]
            end
        end,
        [],
        Topics
    ),
    case Errors of
        [] ->
            ok;
        _ ->
            ?err_rec({read_conflict, Errors})
    end.

verify_preconditions(DBShard, GenId, Ops) ->
    %% Verify expected values:
    Unrecoverable0 = lists:foldl(
        fun({Topic, ValueMatcher}, Acc) ->
            case lookup_kv(DBShard, GenId, Topic) of
                {ok, {_, Value}} when
                    ValueMatcher =:= '_';
                    ValueMatcher =:= Value
                ->
                    Acc;
                {ok, {_, Value}} ->
                    [#{topic => Topic, expected => ValueMatcher, got => Value} | Acc];
                undefined ->
                    [#{topic => Topic, expected => ValueMatcher, got => undefined} | Acc]
            end
        end,
        [],
        maps:get(?ds_tx_expected, Ops, [])
    ),
    %% Verify unexpected values:
    Unrecoverable = lists:foldl(
        fun(Topic, Acc) ->
            case lookup_kv(DBShard, GenId, Topic) of
                undefined ->
                    Acc;
                {ok, Value} ->
                    [#{topic => Topic, unexpected => Value} | Acc]
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

lookup_kv(DBShard, GenId, Topic) ->
    emqx_ds_storage_layer_kv:lookup_kv(DBShard, GenId, Topic).

tx_timeout_msg(Ref) ->
    ?ds_tx_commit_error(Ref, undefined, unrecoverable, timeout).

reply_success(From, Ref, Meta, Serial) ->
    ?tp(
        emqx_ds_optimistic_tx_commit_success,
        #{client => From, ref => Ref, meta => Meta, serial => Serial}
    ),
    From ! ?ds_tx_commit_ok(Ref, Meta, serial_bin(Serial)),
    ok.

reply_error(From, Ref, Meta, Class, Error) ->
    ?tp(
        debug,
        emqx_ds_optimistic_tx_commit_abort,
        #{client => From, ref => Ref, meta => Meta, ec => Class, reason => Error, stage => final}
    ),
    From ! ?ds_tx_commit_error(Ref, Meta, Class, Error),
    ok.

%%================================================================================
%% Tests
%%================================================================================
