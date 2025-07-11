%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_durable_timer_worker).
-moduledoc """
A process that is responsible for execution of the timers.

## State machine
```

         .--(type = active)--> active
        /
 [start]
        \
         `--(leader = down)--> {candidate, Type} --> ...
```

### Active worker's timeline

```                                         pending add apply_after
     pending gc trans                         ..................
      ..............     wake up timers      :   ..........     :
     :              v   v     v       v      :  :          v    v
-----=----=---=--=--=---=--=--=---=-=-=------------------------------> t
     |              |                 |                    |
 del_up_to  fully_replayed_ts   next_wake_up    active_safe_replay_pos
```

- `del_up_to` tracks the last garbage collection.
   It's used to avoid running GC unnecessarily.

- `fully_replayed_ts` is used to

Invariants:

- `del_up_to` =< `fully_replayed_ts`
- `fully_replayed_ts` =< `next_wake_up`
- `next_wake_up` < `safe_active_replay_pos`
- All pending transactions that add new timers insert entries with timestamp >= `active_safe_replay_pos`

""".

-behavior(gen_statem).

%% API:
-export([start_link/5, apply_after/5, dead_hand/5, cancel/3]).

%% behavior callbacks:
-export([callback_mode/0, init/1, terminate/3, handle_event/4]).

%% internal exports:
-export([ls/0, dead_hand_topic/3, started_topic/3]).

-export_type([kind/0]).

-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include("internals.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-type kind() :: active | {closed, started | dead_hand}.

-define(name(WORKER_KIND, TYPE, EPOCH, SHARD), {n, l, {?MODULE, WORKER_KIND, TYPE, EPOCH, SHARD}}).
-define(via(WORKER_KIND, TYPE, EPOCH, SHARD), {via, gproc, ?name(WORKER_KIND, TYPE, EPOCH, SHARD)}).

-record(call_apply_after, {
    k :: emqx_durable_timer:key(),
    v :: emqx_durable_timer:value(),
    t :: integer()
}).
-record(cast_wake_up, {t :: integer()}).

%% States:
-define(s_active, active).
-define(s_candidate(KIND), {candidate, KIND}).

%%================================================================================
%% API functions
%%================================================================================

-spec start_link(
    emqx_durable_timer:type(), emqx_durable_timer:epoch(), emqx_ds:shard(), module(), kind()
) ->
    {ok, pid()}.
start_link(Type, Epoch, Shard, CBM, WorkerKind) ->
    gen_statem:start_link(
        ?via(WorkerKind, Type, Epoch, Shard), ?MODULE, [WorkerKind, Type, CBM, Epoch, Shard], []
    ).

-spec apply_after(
    emqx_durable_timer:type(),
    emqx_durable_timer:epoch(),
    emqx_durable_timer:key(),
    emqx_durable_timer:value(),
    emqx_durable_timer:delay()
) -> ok.
apply_after(Type, Epoch, Key, Val, NotEarlierThan) when
    ?is_valid_timer(Type, Key, Val, NotEarlierThan) andalso is_binary(Epoch)
->
    Shard = emqx_ds:shard_of(?DB_GLOB, Key),
    gen_statem:call(
        ?via(active, Type, Epoch, Shard),
        #call_apply_after{k = Key, v = Val, t = NotEarlierThan},
        infinity
    ).

-spec dead_hand(
    emqx_durable_timer:type(),
    emqx_durable_timer:epoch(),
    emqx_durable_timer:key(),
    emqx_durable_timer:value(),
    emqx_durable_timer:delay()
) -> ok.
dead_hand(Type, Epoch, Key, Val, NotEarlierThan) when
    ?is_valid_timer(Type, Key, Val, NotEarlierThan) andalso is_binary(Epoch)
->
    Result = emqx_ds:trans(
        trans_opts({auto, Key}, #{}),
        fun() ->
            tx_del_dead_hand(Type, Key),
            tx_del_started(Type, Key),
            emqx_ds:tx_write({
                dead_hand_topic(Type, Epoch, Key),
                NotEarlierThan,
                Val
            })
        end
    ),
    case Result of
        {atomic, _, _} ->
            ok;
        Err ->
            Err
    end.

-spec cancel(emqx_durable_timer:type(), emqx_durable_timer:epoch(), emqx_durable_timer:key()) -> ok.
cancel(Type, _Epoch, Key) ->
    Result = emqx_ds:trans(
        trans_opts({auto, Key}, #{}),
        fun() ->
            tx_del_dead_hand(Type, Key),
            tx_del_started(Type, Key)
        end
    ),
    case Result of
        {atomic, _, _} ->
            ok;
        Err ->
            Err
    end.

%%================================================================================
%% behavior callbacks
%%================================================================================

-record(s, {
    type :: emqx_durable_timer:type(),
    cbm :: module(),
    epoch :: emqx_durable_timer:epoch(),
    shard :: emqx_ds:shard(),
    pending_tab :: ets:tid(),
    %% Iterator pointing at the beginning of un-replayed timers:
    replay_pos :: emqx_ds:iterator() | undefined,
    %% Reference to the pending transaction that async-ly cleans up
    %% the replayed data:
    pending_del_tx :: reference() | undefined,
    %% Timeline:
    del_up_to = 0 :: integer(),
    fully_replayed_ts = 0 :: integer(),
    next_wakeup = -1 :: integer()
}).

callback_mode() -> [handle_event_function, state_enter].

init([WorkerKind, Type, CBM, Epoch, Shard]) ->
    process_flag(trap_exit, true),
    logger:update_process_metadata(
        #{kind => WorkerKind, type => Type, epoch => Epoch, shard => Shard}
    ),
    ?tp(?tp_worker_started, #{}),
    case WorkerKind of
        active ->
            insert_epoch(Shard, Epoch),
            D = #s{
                type = Type,
                cbm = CBM,
                epoch = Epoch,
                shard = Shard,
                pending_tab = addq_new(),
                replay_pos = undefined
            },
            {ok, ?s_active, D};
        {closed, Closed} ->
            D = #s{
                type = Type,
                cbm = CBM,
                epoch = Epoch,
                shard = Shard,
                replay_pos = undefined
            },
            {ok, ?s_candidate(Closed), D}
    end.

%% Active:
handle_event(enter, _, ?s_active, _) ->
    keep_state_and_data;
handle_event({call, From}, #call_apply_after{k = Key, v = Value, t = NotEarlierThan}, ?s_active, S) ->
    handle_apply_after(From, Key, Value, NotEarlierThan, S);
handle_event(_TT, ?ds_tx_commit_reply(Ref, Reply), State, D = #s{pending_del_tx = Ref}) ->
    case emqx_ds:tx_commit_outcome(?DB_GLOB, Ref, Reply) of
        {ok, _} ->
            ok;
        Other ->
            ?tp(info, "Failed to clean up expired timers", #{reason => Other})
    end,
    clean_replayed(State, D#s{pending_del_tx = undefined});
handle_event(_ET, ?ds_tx_commit_reply(Ref, Reply), ?s_active, Data) ->
    handle_ds_reply(Ref, Reply, Data);
handle_event(info, #cast_wake_up{t = Treached}, ?s_active, Data) ->
    clean_replayed(?s_active, replay_active(Data, Treached));
%% Candidate:
handle_event(enter, _, ?s_candidate(_), _) ->
    keep_state_and_data;
%% Common:
handle_event(ET, Event, State, Data) ->
    ?tp(error, ?tp_unknown_event, #{m => ?MODULE, ET => Event, state => State, data => Data}),
    keep_state_and_data.

terminate(_Reason, _State, _D) ->
    ok.

%%================================================================================
%% Internal exports
%%================================================================================

%% @doc Display all active workers:
-spec ls() -> [{emqx_durable_timer:type(), emqx_durable_timer:epoch(), emqx_ds:shard()}].
ls() ->
    MS = {{?name('$1', '$2', '$3', '$4'), '_', '_'}, [], [{{'$1', '$2', '$3', '$4'}}]},
    gproc:select({local, names}, [MS]).

dead_hand_topic(Type, NodeEpochId, Key) ->
    [?top_deadhand, <<Type:?type_bits>>, NodeEpochId, Key].

started_topic(Type, NodeEpochId, Key) ->
    [?top_started, <<Type:?type_bits>>, NodeEpochId, Key].

%%================================================================================
%% Internal functions
%%================================================================================

replay_active(S = #s{fully_replayed_ts = FullyReplayedTS0}, Treached) ->
    Bound = Treached + 1,
    ?tp_span(
        ?tp_active_replay,
        #{s => S, bound => Bound, fully_replayed_ts => FullyReplayedTS0},
        maybe
            {ok, It0} ?= get_iterator(?s_active, S),
            {ok, FullyReplayedTS, It} ?= do_replay_active(S, It0, Bound, FullyReplayedTS0),
            S#s{replay_pos = It, fully_replayed_ts = FullyReplayedTS}
        else
            {retry, FullyReplayedTS1, It1, Reason} ->
                ?tp(warning, ?tp_replay_failed, #{
                    from => S#s.replay_pos, to => It1, reason => Reason
                }),
                erlang:send_after(
                    emqx_durable_timer:cfg_replay_retry_interval(),
                    self(),
                    #cast_wake_up{t = Treached}
                ),
                S#s{replay_pos = It1, fully_replayed_ts = FullyReplayedTS1}
        end
    ).

clean_replayed(
    _State,
    D = #s{pending_del_tx = Pending, fully_replayed_ts = FullyReplayed, del_up_to = DelUpTo}
) when is_reference(Pending); FullyReplayed =:= DelUpTo ->
    {keep_state, D};
clean_replayed(
    State,
    D = #s{
        shard = Shard,
        type = Type,
        epoch = Epoch,
        pending_del_tx = undefined,
        fully_replayed_ts = DelUpTo
    }
) ->
    Topic =
        case State of
            ?s_active -> started_topic(Type, Epoch, '+')
        end,
    {async, Ref, _} =
        emqx_ds:trans(
            trans_opts(Shard, #{sync => false}),
            fun() ->
                ?tp(warning, 'deleting old stuffs', #{
                    type => Type, epoch => Epoch, up_to => DelUpTo
                }),
                emqx_ds:tx_del_topic(Topic, 0, DelUpTo + 1)
            end
        ),
    {keep_state, D#s{pending_del_tx = Ref, del_up_to = DelUpTo}}.

do_replay_active(D, It0, Bound, FullyReplayedTS0) ->
    BS = 1000,
    case emqx_ds:next(?DB_GLOB, It0, {time, Bound, BS}) of
        {ok, It, Batch} ->
            {FullyReplayedTS, Nreplayed, []} =
                apply_timers_from_batch(D, Bound, FullyReplayedTS0, Batch),
            %% Have we reached the end?
            case Nreplayed < BS of
                true ->
                    {ok, FullyReplayedTS, It};
                false ->
                    do_replay_active(D, It, Bound, FullyReplayedTS)
            end;
        ?err_rec(Reason) ->
            {retry, FullyReplayedTS0, It0, Reason}
    end.

apply_timers_from_batch(D, Bound, FullyReplayedTS, L) ->
    apply_timers_from_batch(D, Bound, FullyReplayedTS, L, 0).

apply_timers_from_batch(D = #s{type = Type, cbm = CBM}, Bound, FullyReplayedTS, L, N) ->
    case L of
        [] ->
            {FullyReplayedTS, N, []};
        [{_Topic, Time, _Key} | _] when Time >= Bound ->
            {FullyReplayedTS, N, L};
        [{[_Root, _Type, _Epoch, Key], Time, Val} | Rest] ->
            handle_timeout(Type, CBM, Time, Key, Val),
            apply_timers_from_batch(D, Bound, Time, Rest, N + 1)
    end.

-doc """
Get starting position for the replay.
Either create a new iterator at the very beginning of the timer stream,
or return the existing iterator that points at the beginning of un-replayed timers.
""".
get_iterator(State, #s{replay_pos = undefined, shard = Shard, type = Type, epoch = Epoch}) ->
    %% This is the first start of the worker. Start from the beginning:
    maybe
        TopicFilter =
            case State of
                ?s_active ->
                    started_topic(Type, Epoch, '+')
            end,
        {[{_, Stream}], []} ?= emqx_ds:get_streams(?DB_GLOB, TopicFilter, 0, #{shard => Shard}),
        {ok, _It} ?= emqx_ds:make_iterator(?DB_GLOB, Stream, TopicFilter, 0)
    else
        Reason ->
            {retry, #s.fully_replayed_ts, undefined, Reason}
    end;
get_iterator(_State, #s{replay_pos = ReplayPos}) ->
    %% Iterator already exists:
    {ok, ReplayPos}.

%% Active replayer should be careful to not jump over timers that are
%% still being added via pending transactions. Consider the following
%% race condition:
%%
%% 1. Replay iterator is at t0. Next known timer fires at t2. At t1
%% we start a transaction tx to add a new timer at t1':
%%
%%                   tx
%%                 ......
%%                :      v
%% ----------|----------------|-----------------------> t
%%          t0   t1     t1'   t2
%%           ^
%%
%% 2. While the transaction is pending, t2 timer fires and replay
%% iterator moves to t2:
%%
%%                  tx
%%                ......
%%               :      v
%% ----------|----------------|----------------------> t
%%          t0  t1     t1'    t2
%%           `- replay batch -^
%%
%%
%% 3. Transaction tx finally commits, but the iterator is already at
%% t2:
%%
%%                  tx
%%                ______
%%               |      v
%% ----------|----------|-----|----------------------> t
%%          t0  t1     t1'    t2
%%                            ^
%%
%% ...We missed the event at t1'.
%%
%% This function prevents iterator from advancing past the earliest
%% timestamp that is being added via a pending transaction.
active_safe_replay_pos(#s{pending_tab = Tab, fully_replayed_ts = FullyReplayed}, T) ->
    %% 1. We should not attempt to move `fully_replayed_ts' backwards:
    max(
        FullyReplayed,
        case addq_first(Tab) of
            undefined ->
                T;
            MinPending when is_integer(MinPending) ->
                %% 2. We cannot wake up earlier than `MinPending':
                max(MinPending, T)
        end
    ).

handle_apply_after(From, Key, Val, NotEarlierThan, #s{
    type = Type, epoch = Epoch, pending_tab = Tab
}) ->
    %% Start a transaction that inserts the timer to the started queue for the epoch.
    %%
    %% Invarinat: Time > replay_bound(..)
    Time = max(emqx_durable_timer:now_ms() + 1, NotEarlierThan),
    {async, Ref, _Ret} =
        emqx_ds:trans(
            trans_opts({auto, Key}, #{sync => false}),
            fun() ->
                %% Clear previous data:
                tx_del_dead_hand(Type, Key),
                tx_del_started(Type, Key),
                %% Insert the new data:
                emqx_ds:tx_write({started_topic(Type, Epoch, Key), Time, Val})
            end
        ),
    ?tp_ignore_side_effects_in_prod(?tp_apply_after_write_begin, #{
        key => Key, val => Val, time => Time, epoch => Epoch, ref => Ref
    }),
    addq_push(Ref, Time, From, Tab),
    keep_state_and_data.

%% This function is only used in the active worker. It's called on the
%% transaction reply from DS.
handle_ds_reply(Ref, DSReply, D = #s{pending_tab = Tab}) ->
    case addq_pop(Ref, Tab) of
        {Time, From} ->
            Result =
                case emqx_ds:tx_commit_outcome(?DB_GLOB, Ref, DSReply) of
                    {ok, _} ->
                        ?tp_ignore_side_effects_in_prod(?tp_apply_after_write_ok, #{ref => Ref}),
                        ok;
                    Err ->
                        ?tp_ignore_side_effects_in_prod(?tp_apply_after_write_fail, #{
                            ref => Ref, reason => Err
                        }),
                        Err
                end,
            active_schedule_wake_up(Time, D, {reply, From, Result});
        undefined ->
            ?tp(error, ?tp_unknown_event, #{m => ?MODULE, event => DSReply}),
            keep_state_and_data
    end.

active_schedule_wake_up(Time, D0 = #s{next_wakeup = NextWakeUp}, Effects) ->
    D =
        case active_safe_replay_pos(D0, Time) of
            TMax when TMax > NextWakeUp ->
                %% New maximum wait time reached:
                Delay = max(0, TMax - emqx_durable_timer:now_ms()),
                erlang:send_after(Delay, self(), #cast_wake_up{t = TMax}),
                D0#s{next_wakeup = TMax};
            _ ->
                %% Wake up timer already exists
                D0
        end,
    {keep_state, D, Effects}.

-spec handle_timeout(
    emqx_durable_timer:type(),
    module(),
    integer(),
    emqx_durable_timer:key(),
    emqx_durable_timer:value()
) -> ok.
handle_timeout(Type, CBM, Time, Key, Value) ->
    ?tp(warning, ?tp_fire, #{type => Type, key => Key, val => Value, t => Time}),
    try
        CBM:handle_durable_timeout(Key, Value),
        ok
    catch
        _:_ ->
            ok
    end.

tx_del_dead_hand(Type, Key) ->
    [emqx_ds:tx_del_topic(dead_hand_topic(Type, Epoch, Key)) || Epoch <- tx_ls_epochs()],
    ok.

tx_del_started(Type, Key) ->
    [emqx_ds:tx_del_topic(started_topic(Type, Epoch, Key)) || Epoch <- tx_ls_epochs()],
    ok.

%% TODO: add a cache
tx_ls_epochs() ->
    emqx_ds:tx_fold_topic(
        fun(_, _, {[?top_epoch, E], _, _}, Acc) ->
            [E | Acc]
        end,
        [],
        [?top_epoch, '+']
    ).

insert_epoch(Shard, Epoch) ->
    {atomic, _, _} = emqx_ds:trans(
        trans_opts(Shard, #{}),
        fun() ->
            emqx_ds:tx_write({[?top_epoch, Epoch], ?ds_tx_ts_monotonic, <<>>})
        end
    ),
    ok.

trans_opts(Shard, Other) ->
    Other#{
        db => ?DB_GLOB,
        generation => emqx_durable_timer:generation(?DB_GLOB),
        shard => Shard
    }.

addq_new() ->
    ets:new(pending_trans, [private, ordered_set, {keypos, 1}]).

addq_push(Ref, Time, From, Tab) ->
    ets:insert(Tab, {{Time, Ref}}),
    ets:insert(Tab, {{ref, Ref}, Time, From}).

addq_first(Tab) ->
    case ets:first(Tab) of
        {Time, _Ref} ->
            ?tp(foo, #{fst => Time, ref => _Ref}),
            Time;
        '$end_of_table' ->
            undefined
    end.

addq_pop(Ref, Tab) ->
    case ets:take(Tab, {ref, Ref}) of
        [{_, Time, From}] ->
            ets:delete(Tab, {Time, Ref}),
            {Time, From};
        [] ->
            undefined
    end.
