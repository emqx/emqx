%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_durable_timer_worker).
-moduledoc """
A process responsible for execution of the timers.

## State machine
```

         .--(type = active)--> active
        /
 [start]                      ,--select_self--->(leader, Kind)
        \                    /
         `--(candidate, Kind)
                  ^          \
                  |           `--leader is up-->(standby, Kind, MRef)
                   \                              /
                    `--------leader down---------'

```

### Active worker's timeline

```                                         pending add apply_after
     pending gc trans                         ..................
      ..............     wake up timers      :   ..........     :
     :              v   v  v  v   v   v      :  :          v    v
-----=----=---=--=--=---=--=--=---=-=-=------------------------------> t
     |              |                                      |
 del_up_to  fully_replayed_ts                    active_safe_replay_pos
```

- `del_up_to` tracks the last garbage collection.
   It's used to avoid running GC unnecessarily.

- `fully_replayed_ts` is used as the upper bound for delete operations.

Invariants:

- `del_up_to` =< `fully_replayed_ts`
- `fully_replayed_ts` < `active_safe_replay_pos`
- All pending transactions that add new timers insert entries with timestamp >= `active_safe_replay_pos`

""".

-behaviour(gen_statem).

%% API:
-export([start_link/4, apply_after/5]).

%% behavior callbacks:
-export([callback_mode/0, init/1, terminate/3, handle_event/4]).

%% internal exports:
-export([ls/0]).

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
%% Note: here t is epoch time (not adjusted for delta):
-record(cast_wake_up, {t :: integer()}).
-define(try_select_self, try_select_self).
-record(cast_became_leader, {pid :: pid()}).
-define(replay_complete, replay_complete).

%% States:
%%    Active workers replay `apply_after' timers that are added in the
%%    current (open) epoch for the node.
-define(s_active, active).
%%    Replayers for the closed epochs start in this state:
-define(s_candidate(KIND), {candidate, KIND}).
%%    Active worker for the closed epoch:
-define(s_leader(KIND), {leader, KIND}).
%%    Standby worker that becomes candidate when the leader dies:
-define(s_standby(KIND, REF), {standby, KIND, REF}).

%%================================================================================
%% API functions
%%================================================================================

-spec start_link(
    kind(), emqx_durable_timer:type(), emqx_durable_timer:epoch(), emqx_ds:shard()
) ->
    {ok, pid()}.
start_link(WorkerKind, Type, Epoch, Shard) ->
    gen_statem:start_link(
        ?via(WorkerKind, Type, Epoch, Shard), ?MODULE, [WorkerKind, Type, Epoch, Shard], []
    ).

-spec apply_after(
    emqx_durable_timer:type(),
    emqx_durable_timer:epoch(),
    emqx_durable_timer:key(),
    emqx_durable_timer:value(),
    emqx_durable_timer:delay()
) -> ok | emqx_ds:error(_).
apply_after(Type, Epoch, Key, Val, NotEarlierThan) when
    ?is_valid_timer(Type, Key, Val, NotEarlierThan) andalso is_binary(Epoch)
->
    Shard = emqx_ds:shard_of(?DB_GLOB, Key),
    gen_statem:call(
        ?via(active, Type, Epoch, Shard),
        #call_apply_after{k = Key, v = Val, t = NotEarlierThan},
        infinity
    ).

%%================================================================================
%% behavior callbacks
%%================================================================================

-record(s, {
    type :: emqx_durable_timer:type(),
    cbm :: module(),
    epoch :: emqx_durable_timer:epoch(),
    shard :: emqx_ds:shard(),
    pending_tab :: ets:tid() | undefined,
    %% Iterator pointing at the beginning of un-replayed timers:
    replay_pos :: emqx_ds:iterator() | undefined,
    %% Remaining entries from the last replay (relevent for closed)
    tail = [] :: [emqx_ds:ttv()],
    %% Reference to the pending transaction that async-ly cleans up
    %% the replayed data:
    pending_del_tx :: reference() | undefined,
    topic :: emqx_ds:topic(),
    %% This value should be added to the timestamp of the timer TTV to
    %% convert it to the local time:
    time_delta :: integer() | undefined,
    %% Timeline (Note: here times are NOT adjusted for delta):
    del_up_to = -1 :: integer(),
    fully_replayed_ts = -1 :: integer()
}).

-type data() :: #s{}.

callback_mode() -> [handle_event_function, state_enter].

init([WorkerKind, Type, Epoch, Shard]) ->
    process_flag(trap_exit, true),
    logger:update_process_metadata(
        #{kind => WorkerKind, type => Type, epoch => Epoch, shard => Shard}
    ),
    CBM = emqx_durable_timer:get_cbm(Type),
    ?tp(debug, ?tp_worker_started, #{
        type => Type, epoch => Epoch, shard => Shard, kind => WorkerKind
    }),
    case WorkerKind of
        active ->
            emqx_durable_timer_dl:insert_epoch_marker(Shard, Epoch),
            D = #s{
                type = Type,
                cbm = CBM,
                epoch = Epoch,
                shard = Shard,
                pending_tab = addq_new(),
                time_delta = 0,
                topic = emqx_durable_timer_dl:started_topic(Type, Epoch, '+')
            },
            {ok, ?s_active, D};
        {closed, Kind} ->
            pg:join(?workers_pg, {Kind, Type, Epoch, Shard}, self()),
            D = #s{
                type = Type,
                cbm = CBM,
                epoch = Epoch,
                shard = Shard,
                topic =
                    case Kind of
                        started -> emqx_durable_timer_dl:started_topic(Type, Epoch, '+');
                        dead_hand -> emqx_durable_timer_dl:dead_hand_topic(Type, Epoch, '+')
                    end
            },
            {ok, ?s_candidate(Kind), D}
    end.

%% Active:
handle_event(enter, From, ?s_active, _) ->
    ?tp(debug, ?tp_state_change, #{from => From, to => ?s_active}),
    keep_state_and_data;
handle_event({call, From}, #call_apply_after{k = Key, v = Value, t = NotEarlierThan}, ?s_active, S) ->
    handle_apply_after(From, Key, Value, NotEarlierThan, S);
%% Candidate and standby:
handle_event(enter, From, ?s_candidate(Kind), Data) ->
    ?tp(debug, ?tp_state_change, #{from => From, to => ?s_candidate(Kind)}),
    init_candidate(Kind, Data);
handle_event(state_timeout, ?try_select_self, ?s_candidate(Kind), Data) ->
    handle_leader_selection(Kind, Data);
handle_event(enter, From, State = ?s_leader(_Kind), Data) ->
    ?tp(debug, ?tp_state_change, #{from => From, to => State}),
    init_leader(State, Data);
handle_event(_, #cast_became_leader{pid = Leader}, State, Data) ->
    case State of
        ?s_candidate(Kind) ->
            init_standby(Kind, Data, Leader);
        ?s_standby(_, _) ->
            keep_state_and_data
    end;
handle_event(info, ?replay_complete, State, _Data) ->
    case State of
        ?s_candidate(_) -> ok;
        ?s_standby(_, _) -> ok
    end,
    {stop, normal};
handle_event(enter, From, State = ?s_standby(_, _), _) ->
    ?tp(debug, ?tp_state_change, #{from => From, to => State}),
    keep_state_and_data;
handle_event(info, {'DOWN', Ref, _, _, Reason}, ?s_standby(Kind, Ref), Data) ->
    handle_leader_down(Kind, Data, Reason);
%% Common:
handle_event(_ET, ?ds_tx_commit_reply(Ref, Reply), _State, Data) ->
    handle_ds_reply(Ref, Reply, Data);
handle_event(info, #cast_wake_up{t = Treached}, State, Data) ->
    handle_wake_up(State, Data, Treached);
handle_event(info, {global_name_conflict, _}, _State, _Data) ->
    %% Name has been taken over by another peer:
    {stop, taken_over};
handle_event(info, {'EXIT', _, shutdown}, _State, _Data) ->
    {stop, shutdown};
handle_event(ET, Event, State, Data) ->
    ?tp(debug, ?tp_unknown_event, #{m => ?MODULE, ET => Event, state => State, data => Data}),
    keep_state_and_data.

terminate(_Reason, _State, _D) ->
    ?tp(debug, ?tp_terminate, #{m => ?MODULE, reason => _Reason, s => _State}),
    ok.

%%================================================================================
%% Internal exports
%%================================================================================

%% @doc Display all active workers:
-spec ls() -> [{emqx_durable_timer:type(), emqx_durable_timer:epoch(), emqx_ds:shard(), pid()}].
ls() ->
    MS = {{?name('$1', '$2', '$3', '$4'), '$5', '_'}, [], [{{'$1', '$2', '$3', '$4', '$5'}}]},
    gproc:select({local, names}, [MS]).

%%================================================================================
%% Internal functions
%%================================================================================

%%--------------------------------------------------------------------------------
%% Leader selection
%%--------------------------------------------------------------------------------

handle_leader_down(Kind, Data, _Reason) ->
    {next_state, ?s_candidate(Kind), Data}.

init_candidate(Kind, #s{type = T, epoch = E, shard = S}) ->
    case global:whereis_name(?name(Kind, T, E, S)) of
        Leader when is_pid(Leader) ->
            self() ! #cast_became_leader{pid = Leader},
            keep_state_and_data;
        undefined ->
            %% Try to become leader immediately when leader for the
            %% raft shard:
            Timeout =
                case emqx_ds_builtin_raft_shard:servers(?DB_GLOB, S, leader) of
                    [{_, Node}] when Node =:= node() ->
                        0;
                    _ ->
                        rand:uniform(1000)
                end,
            {keep_state_and_data, {state_timeout, Timeout, ?try_select_self}}
    end.

init_standby(Kind, Data, Leader) ->
    MRef = monitor(process, Leader),
    {next_state, ?s_standby(Kind, MRef), Data}.

handle_leader_selection(Kind, Data = #s{type = T, epoch = E, shard = S}) ->
    case global:register_name(?name(Kind, T, E, S), self(), fun global:random_notify_name/3) of
        yes ->
            {next_state, ?s_leader(Kind), Data};
        no ->
            {next_state, ?s_candidate(Kind), Data}
    end.

init_leader(?s_leader(Kind), Data0 = #s{epoch = Epoch}) ->
    maybe
        {ok, LastHeartbeat0} ?= emqx_durable_timer_dl:last_heartbeat(Epoch),
        Delta =
            case Kind of
                started ->
                    %% TODO: try to account for clock skew?
                    0;
                dead_hand ->
                    %% Upper margin for error:
                    LastHeartbeat0 + emqx_durable_timer:cfg_heartbeat_interval()
            end,
        Data1 = Data0#s{time_delta = Delta},
        {ok, It} ?= get_iterator(Data1),
        Data = Data1#s{replay_pos = It},
        %% Broadcast the self-appointment to the peers:
        pg_bcast(Kind, Data, #cast_became_leader{pid = self()}),
        %% Start the replay loop:
        wake_up(Data, 0),
        {keep_state, Data}
    else
        undefined ->
            complete_replay(Kind, Data0, 0)
    end.

%%--------------------------------------------------------------------------------
%% Replay
%%--------------------------------------------------------------------------------

handle_wake_up(
    _State, #s{fully_replayed_ts = FullyReplayed}, Treached
) when Treached =< FullyReplayed ->
    %% Ignore duplicate timers:
    keep_state_and_data;
handle_wake_up(
    State, Data0 = #s{fully_replayed_ts = FullyReplayedTS0}, Treached
) ->
    Bound = Treached + 1,
    DSBatchSize =
        case State of
            ?s_active ->
                {time, Bound, emqx_durable_timer:cfg_batch_size()};
            _ ->
                emqx_durable_timer:cfg_batch_size()
        end,
    Result = ?tp_span(
        ?tp_replay,
        #{bound => Bound, fully_replayed_ts => FullyReplayedTS0},
        replay_timers(State, Data0, Bound, DSBatchSize)
    ),
    case Result of
        {end_of_stream, FullyReplayedTS} ->
            ?s_leader(LeaderKind) = State,
            complete_replay(LeaderKind, clean_replayed(Data0), FullyReplayedTS);
        {ok, Data} ->
            {keep_state, clean_replayed(schedule_next_wake_up(State, Data))};
        {retry, Data, Reason} ->
            ?tp(warning, ?tp_replay_failed, #{
                from => Data0#s.replay_pos, to => Bound, reason => Reason
            }),
            erlang:send_after(
                emqx_durable_timer:cfg_replay_retry_interval(),
                self(),
                #cast_wake_up{t = Treached}
            ),
            {keep_state, Data}
    end.

schedule_next_wake_up(?s_active, Data) ->
    Data;
schedule_next_wake_up(?s_leader(_), Data = #s{tail = [{_, NextTime, _} | _]}) ->
    wake_up(Data, NextTime),
    Data.

complete_replay(
    LeaderKind, Data = #s{shard = Shard, topic = Topic, epoch = Epoch}, FullyReplayedTS
) ->
    _ = emqx_durable_timer_dl:clean_replayed(Shard, Topic, FullyReplayedTS),
    pg_bcast(LeaderKind, Data, ?replay_complete),
    _ = emqx_durable_timer_dl:delete_epoch_if_empty(Epoch),
    {stop, normal}.

pg_bcast(LeaderKind, #s{type = Type, epoch = Epoch, shard = Shard}, Msg) ->
    _ = [
        Peer ! Msg
     || Peer <- pg:get_members(?workers_pg, {LeaderKind, Type, Epoch, Shard}),
        Peer =/= self()
    ],
    ok.

replay_timers(
    State,
    Data = #s{fully_replayed_ts = FullyReplayedTS0, replay_pos = It0, tail = Tail0},
    Bound,
    DSBatchSize
) ->
    case do_replay_timers(Data, It0, Bound, DSBatchSize, FullyReplayedTS0, Tail0) of
        {ok, FullyReplayedTS, _, []} when State =/= ?s_active ->
            {end_of_stream, FullyReplayedTS};
        {ok, FullyReplayedTS, It, Tail} ->
            {ok, Data#s{replay_pos = It, fully_replayed_ts = FullyReplayedTS, tail = Tail}};
        {retry, FullyReplayedTS1, It1, Reason} ->
            {retry, Data#s{replay_pos = It1, fully_replayed_ts = FullyReplayedTS1, tail = []},
                Reason}
    end.

do_replay_timers(D, It0, Bound, DSBatchSize, FullyReplayedTS, []) ->
    case emqx_ds:next(?DB_GLOB, It0, DSBatchSize) of
        {ok, It, []} ->
            {ok, FullyReplayedTS, It, []};
        {ok, It, Batch} ->
            do_replay_timers(D, It, Bound, DSBatchSize, FullyReplayedTS, Batch);
        ?err_rec(Reason) ->
            {retry, FullyReplayedTS, It0, Reason}
    end;
do_replay_timers(D, It, Bound, DSBatchSize, FullyReplayedTS0, Batch) ->
    {FullyReplayedTS, Tail} = apply_timers_from_batch(D, Bound, FullyReplayedTS0, Batch),
    case Tail of
        [] ->
            do_replay_timers(D, It, Bound, DSBatchSize, FullyReplayedTS, []);
        _ ->
            {ok, FullyReplayedTS, It, Tail}
    end.

apply_timers_from_batch(
    D = #s{type = Type, cbm = CBM}, Bound, FullyReplayedTS, L
) ->
    case L of
        [] ->
            {FullyReplayedTS, []};
        [{[_Root, _Type, _Epoch, Key], Time, Val} | Rest] when Time =< Bound ->
            handle_timeout(Type, CBM, Time, Key, Val),
            apply_timers_from_batch(D, Bound, Time, Rest);
        _ ->
            {FullyReplayedTS, L}
    end.

-spec clean_replayed(data()) -> data().
clean_replayed(
    D = #s{
        pending_del_tx = Pending,
        fully_replayed_ts = FullyReplayed,
        del_up_to = DelUpTo
    }
) when is_reference(Pending); FullyReplayed =:= DelUpTo ->
    %% Clean up is already in progress or there's nothing to clean:
    D;
clean_replayed(
    D = #s{
        shard = Shard,
        topic = Topic,
        pending_del_tx = undefined,
        fully_replayed_ts = DelUpTo
    }
) ->
    Ref = emqx_durable_timer_dl:clean_replayed_async(Shard, Topic, DelUpTo),
    D#s{pending_del_tx = Ref, del_up_to = DelUpTo}.

-spec on_clean_complete(reference(), _, data()) -> data().
on_clean_complete(Ref, Reply, Data) ->
    case emqx_ds:tx_commit_outcome(?DB_GLOB, Ref, Reply) of
        {ok, _} ->
            ok;
        Other ->
            ?tp(info, "Failed to clean up expired timers", #{reason => Other})
    end,
    clean_replayed(Data#s{pending_del_tx = undefined}).

-spec handle_timeout(
    emqx_durable_timer:type(),
    module(),
    integer(),
    emqx_durable_timer:key(),
    emqx_durable_timer:value()
) -> ok.
handle_timeout(Type, CBM, Time, Key, Value) ->
    ?tp(debug, ?tp_fire, #{type => Type, key => Key, val => Value, t => Time}),
    try
        emqx_durable_timer:handle_durable_timeout(CBM, Key, Value),
        ok
    catch
        _:_ ->
            ok
    end.

-doc """
Get starting position for the replay.
Either create a new iterator at the very beginning of the timer stream,
or return the existing iterator that points at the beginning of un-replayed timers.
""".
get_iterator(#s{shard = Shard, topic = Topic}) ->
    %% This is the first start of the worker. Start from the beginning:
    case emqx_ds:get_streams(?DB_GLOB, Topic, 0, #{shard => Shard}) of
        {[{_, Stream}], []} ->
            emqx_ds:make_iterator(?DB_GLOB, Stream, Topic, 0);
        {[], []} ->
            undefined;
        {_, Errors} ->
            error({failed_to_make_iterator, Errors})
    end.

%%--------------------------------------------------------------------------------
%% Adding a new timer to the active queue
%%--------------------------------------------------------------------------------

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

active_ensure_iterator(Data = #s{replay_pos = undefined}) ->
    {ok, It} = get_iterator(Data),
    Data#s{
        replay_pos = It
    };
active_ensure_iterator(Data) ->
    Data.

handle_apply_after(From, Key, Val, NotEarlierThan, #s{
    type = Type, epoch = Epoch, pending_tab = Tab
}) ->
    %% Start a transaction that inserts the timer to the started queue for the epoch.
    %%
    %% Invarinat: Time > replay_bound(..)
    Time = max(emqx_durable_timer:now_ms() + 1, NotEarlierThan),
    Ref = emqx_durable_timer_dl:insert_started_async(Type, Epoch, Key, Val, Time),
    ?tp_ignore_side_effects_in_prod(?tp_apply_after_write_begin, #{
        key => Key, val => Val, time => Time, epoch => Epoch, ref => Ref
    }),
    addq_push(Ref, Time, From, Tab),
    keep_state_and_data.

handle_ds_reply(Ref, DSReply, D = #s{pending_del_tx = Ref}) ->
    {keep_state, on_clean_complete(Ref, DSReply, D)};
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
            active_schedule_wake_up(Time, active_ensure_iterator(D), {reply, From, Result});
        undefined ->
            ?tp(error, ?tp_unknown_event, #{m => ?MODULE, event => DSReply}),
            keep_state_and_data
    end.

active_schedule_wake_up(Time, D, Effects) ->
    Tsafe = active_safe_replay_pos(D, Time),
    wake_up(D, Tsafe),
    {keep_state, D, Effects}.

%%--------------------------------------------------------------------------------
%% Pending transaction queue
%%--------------------------------------------------------------------------------

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

%%--------------------------------------------------------------------------------
%% Misc
%%--------------------------------------------------------------------------------

epoch_to_local(#s{time_delta = Delta}, T) ->
    T + Delta.

%% Schedule next wake up. T is in "epoch" time
wake_up(Data, T) ->
    Delay = epoch_to_local(Data, T) - emqx_durable_timer:now_ms(),
    erlang:send_after(
        max(0, Delay),
        self(),
        #cast_wake_up{t = T}
    ).
