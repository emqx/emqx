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

%% @doc This module serves the same role as `emqx_ds_buffer', but it's
%% suited for transactions. Also, it runs on the leader node as
%% opposed to the buffer.
-module(emqx_ds_optimistic_tx).

-behaviour(gen_statem).

%% API:
-export([
    start_link/3,

    where/2,

    new_kv_tx_ctx/5,
    commit_kv_tx/3
]).

%% Behaviour callbacks
-export([
    init/1,
    handle_event/4,
    callback_mode/0
]).

-export_type([
    ctx/0
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

-opaque ctx() :: #kv_tx_ctx{}.

-define(name(DB, SHARD), {n, l, {?MODULE, DB, SHARD}}).
-define(via(DB, SHARD), {via, gproc, ?name(DB, SHARD)}).

%% States
-define(idle, idle).
-define(pending, pending).

%% Timeouts
-define(timeout_flush, timout_flush).
-define(timeout_rotate, timeout_rotate).

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
    serial,
    serial_control,
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
    emqx_ds:db(), emqx_ds:shard(), pid(), emqx_ds:transaction_opts(), emqx_ds:tx_serial()
) ->
    ctx().
new_kv_tx_ctx(DB, Shard, Leader, Options, Serial) ->
    case Options of
        #{generation := Generation} ->
            ok;
        _ ->
            {Generation, _} = emqx_ds_storage_layer:find_generation({DB, Shard}, current)
    end,
    #kv_tx_ctx{
        shard = Shard,
        leader = Leader,
        serial = Serial,
        generation = Generation,
        opts = Options
    }.

commit_kv_tx(_DB, Ctx = #kv_tx_ctx{opts = #{timeout := Timeout}}, Ops) ->
    #kv_tx_ctx{leader = Leader} = Ctx,
    Ref = monitor(process, Leader),
    %% Note: currently timer is not canceled when commit abort is
    %% signalled via the monitor message (i.e. when the leader process
    %% is down). In this case the caller will receive double `DOWN'.
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
    schedule_rotation(RI),
    {ok, ?idle, #d{
        dbshard = {DB, Shard},
        cbm = CBM,
        rotate_interval = RI,
        %% FIXME: hardcode
        flush_interval = 10,
        serial = Serial,
        serial_control = Serial
    }}.

handle_event(enter, _OldState, ?pending, #d{flush_interval = T}) ->
    {keep_state_and_data, {state_timeout, T, ?timeout_flush}};
handle_event(enter, _, _, _) ->
    keep_state_and_data;
handle_event(cast, Tx, State, D0) ->
    case handle_tx(D0, Tx) of
        {ok, D} ->
            case State of
                ?idle ->
                    {next_state, ?pending, D};
                _ ->
                    {keep_state, D}
            end;
        aborted ->
            keep_state_and_data
    end;
handle_event(ET, ?timeout_flush, _State, D0) when ET =:= state_timeout; ET =:= timeout ->
    D = flush(D0),
    {next_state, ?idle, D};
handle_event(info, ?timeout_rotate, _State, D0 = #d{rotate_interval = RI}) ->
    D = rotate(D0),
    schedule_rotation(RI),
    {keep_state, D};
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

handle_tx(D = #d{dbshard = DBShard, cbm = CBM, serial = Serial, gens = Gens}, Tx) ->
    #ds_tx{ctx = #kv_tx_ctx{generation = Gen}} = Tx,
    case Gens of
        #{Gen := GS0} ->
            ok;
        #{} ->
            GS0 = #gen_data{dirty = emqx_ds_tx_conflict_trie:new(Serial, infinity)}
    end,
    case try_commit(DBShard, Gen, CBM, Serial + 1, Tx, GS0) of
        {ok, GS} ->
            {ok, D#d{
                serial = Serial + 1,
                gens = Gens#{Gen => GS}
            }};
        aborted ->
            aborted
    end.

try_commit(
    DBShard,
    Gen,
    CBM,
    Serial,
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
    #kv_tx_ctx{serial = TxSerial} = Ctx,
    maybe
        ok ?= check_conflicts(Dirty0, TxSerial, Ops),
        ok ?= verify_preconditions(DBShard, Gen, Ops),
        {ok, CookedTx} ?=
            CBM:prepare_kv_tx(
                DBShard, Gen, serial_bin(Serial), Ops, #{}
            ),
        Dirty = update_dirty(Serial, Ops, Dirty0),
        {ok, GS#gen_data{
            dirty = Dirty,
            buffer = [CookedTx | Buff],
            pending_replies = [{From, Ref, Meta, Serial} | Pending]
        }}
    else
        {error, Class, Error} ->
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

flush(D = #d{dbshard = DBShard, cbm = CBM, serial_control = SerCtl, serial = Serial, gens = Gens0}) ->
    Gens = maps:map(
        fun(Gen, GS = #gen_data{buffer = Buf, pending_replies = Replies, dirty = Dirty}) ->
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
        serial_control = Serial,
        gens = Gens
    }.

serial_bin(A) ->
    <<A:128>>.

rotate(D = #d{gens = Gens0}) ->
    Gens = maps:map(
        fun(_, GS = #gen_data{dirty = Dirty}) ->
            GS#gen_data{
                dirty = emqx_ds_tx_conflict_trie:rotate(Dirty)
            }
        end,
        Gens0
    ),
    D#d{gens = Gens}.

check_conflicts(Dirty, TxSerial, Ops) ->
    %% Verify normal reads and also make sure we don't read buffered
    %% data while verifying preconditions:
    Topics =
        [
            maps:get(?ds_tx_read, Ops, []),
            maps:get(?ds_tx_unexpected, Ops, []),
            [Topic || {Topic, _} <- maps:get(?ds_tx_expected, Ops, [])]
        ],
    Errors = lists:foldl(
        fun(ReadTF, Acc) ->
            case
                emqx_ds_tx_conflict_trie:is_dirty(
                    emqx_ds_tx_conflict_trie:topic_filter_to_conflict_domain(ReadTF),
                    TxSerial,
                    Dirty
                )
            of
                false ->
                    Acc;
                true ->
                    [ReadTF | Acc]
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
    emqx_ds_storage_layer:lookup_kv(DBShard, GenId, Topic).

tx_timeout_msg(Ref) ->
    ?ds_tx_commit_error(Ref, undefined, unrecoverable, timeout).

schedule_rotation(Interval) ->
    erlang:send_after(Interval, self(), ?timeout_rotate).

reply_success(From, Ref, Meta, Serial) ->
    ?tp(
        emqx_ds_optimistic_tx_reply,
        #{client => From, ref => Ref, meta => Meta, success => Serial}
    ),
    From ! ?ds_tx_commit_ok(Ref, Meta, serial_bin(Serial)),
    ok.

reply_error(From, Ref, Meta, Class, Error) ->
    ?tp(
        debug,
        emqx_ds_optimistic_tx_reply,
        #{client => From, ref => Ref, meta => Meta, Class => Error}
    ),
    From ! ?ds_tx_commit_error(Ref, Meta, Class, Error),
    ok.

%%================================================================================
%% Tests
%%================================================================================
