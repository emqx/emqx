%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Global session registry table maintenance: history retention and
%% stale-pid garbage collection. Walks `?CHAN_REG_TAB' on a slow cadence,
%% applying per-row predicates to remove (a) expired history tombstones
%% and (b) rows whose `pid' is provably no longer a live owner.
-module(emqx_cm_registry_keeper).
-behaviour(gen_server).

-export([
    start_link/0,
    count/1,
    purge/0,
    ensure_started/0
]).

-ifdef(TEST).
-export([force_sweep_stale_pids/0]).
-endif.

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("emqx_cm.hrl").

-define(CACHE_COUNT_THRESHOLD, 1000).
-define(MIN_COUNT_INTERVAL_SECONDS, 5).

-ifdef(TEST).
-define(REGISTRY_GC_INTERVAL_MS, 1000).
-define(REGISTRY_GC_CHUNK_SIZE, 50).
-define(REGISTRY_GC_CHUNK_INTERVAL_MS, 50).
-else.
-define(REGISTRY_GC_INTERVAL_MS, timer:minutes(10)).
-define(REGISTRY_GC_CHUNK_SIZE, 100).
%% 100 keys / 200 ms = 500 keys / s
-define(REGISTRY_GC_CHUNK_INTERVAL_MS, 200).
-endif.

-define(IS_HIST_ENABLED(RETAIN), (RETAIN > 0)).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_) ->
    %% Both cores and replicants run the sweep:
    %% - Cores act on hist tombstones, local-dead pids, and remote orphans
    %%   (the last gated by mria:is_peer_alive/1 consensus).
    %% - Replicants act only on local-dead pids. They skip hist tombstones
    %%   (cluster-wide concern: cores are the single deleter) and skip
    %%   remote pids (the consensus check is core-only).
    TimerRef = send_delay_start(),
    {ok, new_sweep_state(TimerRef)}.

%% @doc Kick off a sweep tick on a running process that may have started
%% under an older beam that never scheduled a timer for its role (the
%% pre-fix replicant case). Idempotent: when a timer is already
%% scheduled, this just brings the next tick forward; the new chunk
%% will reschedule at the normal cadence.
-spec ensure_started() -> ok.
ensure_started() ->
    case whereis(?MODULE) of
        undefined ->
            ok;
        Pid when is_pid(Pid) ->
            erlang:send(Pid, start),
            ok
    end.

%% @doc Count the number of sessions.
%% Include sessions which are expired since the given timestamp if `since' is greater than 0.
-spec count(non_neg_integer()) -> non_neg_integer().
count(Since) ->
    Retain = retain_duration(),
    Now = now_ts(),
    %% Get table size if hist is not enabled or
    %% Since is before the earliest possible retention time.
    IsCountAll = (not ?IS_HIST_ENABLED(Retain) orelse (Now - Retain >= Since)),
    case IsCountAll of
        true ->
            mnesia:table_info(?CHAN_REG_TAB, size);
        false ->
            %% make a gen call to avoid many callers doing the same concurrently
            gen_server:call(?MODULE, {count, Since}, infinity)
    end.

%% @doc Delete all retained history. Only for tests.
-spec purge() -> ok.
purge() ->
    purge_loop(mnesia:dirty_first(?CHAN_REG_TAB)).

-ifdef(TEST).
%% @doc Run a synchronous full sweep that applies the stale-pid predicate
%% (and the hist predicate if enabled) to every row in the registry table.
%% Returns the per-sweep counter map. Tests only.
-spec force_sweep_stale_pids() -> map().
force_sweep_stale_pids() ->
    gen_server:call(?MODULE, force_sweep_stale_pids, infinity).
-endif.

purge_loop('$end_of_table') ->
    ok;
purge_loop(ClientId) ->
    Rows = mnesia:dirty_read(?CHAN_REG_TAB, ClientId),
    Next = mnesia:dirty_next(?CHAN_REG_TAB, ClientId),
    lists:foreach(
        fun(R) -> mria:dirty_delete_object(?CHAN_REG_TAB, R) end,
        Rows
    ),
    purge_loop(Next).

handle_call(force_sweep_stale_pids, _From, State0) ->
    State1 = reset_sweep_state(State0, _Forced = true),
    ?tp(info, cm_registry_gc_started, #{forced => true}),
    State2 = run_full_sweep(undefined, State1),
    Summary = sweep_summary(State2),
    ?tp(info, cm_registry_gc_finished, Summary#{forced => true}),
    {reply, Summary, reset_sweep_counters(State2)};
handle_call({count, Since}, _From, State) ->
    {LastCountTime, LastCount} =
        case State of
            #{last_count_time := T, last_count := C} ->
                {T, C};
            _ ->
                {0, 0}
        end,
    Now = now_ts(),
    Total = mnesia:table_info(?CHAN_REG_TAB, size),
    %% Always count if the table is small enough
    %% or when the last count is too old
    IsTableSmall = (Total < ?CACHE_COUNT_THRESHOLD),
    IsLastCountOld = (Now - LastCountTime > ?MIN_COUNT_INTERVAL_SECONDS),
    case IsTableSmall orelse IsLastCountOld of
        true ->
            Count = do_count(Since),
            CountFinishedAt = now_ts(),
            {reply, Count, State#{last_count_time => CountFinishedAt, last_count => Count}};
        false ->
            {reply, LastCount, State}
    end;
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(start, State0) ->
    %% ensure_sweep_keys/1 makes the per-sweep counter and node-cache keys
    %% present, so a hot-loaded beam survives the first tick when the
    %% pre-upgrade state only had next_clientid+timer_ref.
    #{next_clientid := Cursor, timer_ref := OldTimerRef} =
        State1 = ensure_sweep_keys(State0),
    is_reference(OldTimerRef) andalso erlang:cancel_timer(OldTimerRef),
    State2 =
        case Cursor of
            undefined ->
                ?tp(info, cm_registry_gc_started, #{}),
                reset_sweep_state(State1, _Forced = false);
            _ ->
                State1
        end,
    do_run_chunk(Cursor, State2);
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Sweep state and counters
%%--------------------------------------------------------------------

new_sweep_state(TimerRef) ->
    (zero_state())#{timer_ref => TimerRef}.

zero_state() ->
    #{
        next_clientid => undefined,
        timer_ref => undefined,
        sweep_started_at => undefined,
        gone_nodes => sets:new(),
        alive_nodes => sets:new(),
        scanned => 0,
        deleted_hist => 0,
        deleted_local_dead => 0,
        deleted_remote_orphan => 0
    }.

%% Defensive: after a hot code load, the running gen_server's state may
%% have the pre-upgrade shape (only next_clientid + timer_ref). Merge
%% with zero_state so every key the new code reads with `:=' is present.
%% Existing values win.
ensure_sweep_keys(State) ->
    maps:merge(zero_state(), State).

%% Reset per-sweep accumulators and stamp a fresh start time.
reset_sweep_state(State, _Forced) ->
    State#{
        sweep_started_at => erlang:monotonic_time(millisecond),
        gone_nodes => sets:new(),
        alive_nodes => sets:new(),
        scanned => 0,
        deleted_hist => 0,
        deleted_local_dead => 0,
        deleted_remote_orphan => 0
    }.

%% Clear counters after a forced sweep so the next scheduled tick starts fresh.
reset_sweep_counters(State) ->
    State#{
        sweep_started_at => undefined,
        gone_nodes => sets:new(),
        alive_nodes => sets:new(),
        scanned => 0,
        deleted_hist => 0,
        deleted_local_dead => 0,
        deleted_remote_orphan => 0
    }.

sweep_summary(State) ->
    Now = erlang:monotonic_time(millisecond),
    StartedAt =
        case maps:get(sweep_started_at, State, undefined) of
            undefined -> Now;
            T -> T
        end,
    #{
        scanned => maps:get(scanned, State, 0),
        deleted_hist => maps:get(deleted_hist, State, 0),
        deleted_local_dead => maps:get(deleted_local_dead, State, 0),
        deleted_remote_orphan => maps:get(deleted_remote_orphan, State, 0),
        duration_ms => Now - StartedAt
    }.

bump(Key, State) ->
    maps:update_with(Key, fun(V) -> V + 1 end, State).

%%--------------------------------------------------------------------
%% Chunked walk (scheduled)
%%--------------------------------------------------------------------

do_run_chunk(Cursor, State0) ->
    {NewNext, State1} = run_chunk_loop(Cursor, ?REGISTRY_GC_CHUNK_SIZE, State0),
    case NewNext of
        '$end_of_table' ->
            Summary = sweep_summary(State1),
            ?tp(info, cm_registry_gc_finished, Summary),
            TimerRef = send_delay_start(),
            {noreply, State1#{next_clientid := undefined, timer_ref := TimerRef}};
        Cid ->
            %% ensure the next clientid is not in the cache
            _ = erlang:garbage_collect(),
            TimerRef = send_delay_start(?REGISTRY_GC_CHUNK_INTERVAL_MS),
            {noreply, State1#{next_clientid := Cid, timer_ref := TimerRef}}
    end.

run_chunk_loop(undefined, Budget, State) ->
    run_chunk_loop(mnesia:dirty_first(?CHAN_REG_TAB), Budget, State);
run_chunk_loop('$end_of_table', _Budget, State) ->
    {'$end_of_table', State};
run_chunk_loop(ClientId, 0, State) ->
    {ClientId, State};
run_chunk_loop(ClientId, Budget, State0) ->
    Rows = mnesia:dirty_read(?CHAN_REG_TAB, ClientId),
    Next = mnesia:dirty_next(?CHAN_REG_TAB, ClientId),
    State1 = lists:foldl(
        fun(R, S) -> process_row(ClientId, R, S) end,
        State0,
        Rows
    ),
    State2 = bump(scanned, State1),
    run_chunk_loop(Next, Budget - 1, State2).

%%--------------------------------------------------------------------
%% Full synchronous walk (TEST-only force_sweep_stale_pids/0)
%%--------------------------------------------------------------------

run_full_sweep(undefined, State) ->
    run_full_sweep(mnesia:dirty_first(?CHAN_REG_TAB), State);
run_full_sweep('$end_of_table', State) ->
    State;
run_full_sweep(ClientId, State0) ->
    Rows = mnesia:dirty_read(?CHAN_REG_TAB, ClientId),
    Next = mnesia:dirty_next(?CHAN_REG_TAB, ClientId),
    State1 = lists:foldl(
        fun(R, S) -> process_row(ClientId, R, S) end,
        State0,
        Rows
    ),
    State2 = bump(scanned, State1),
    run_full_sweep(Next, State2).

%%--------------------------------------------------------------------
%% Per-row predicates
%%--------------------------------------------------------------------

%% History row: integer Unix-seconds timestamp in the pid field.
%% Hist tombstones are a cluster-wide concern: cores are the single
%% deleter, replicants skip them.
process_row(_ClientId, #channel{pid = Ts} = R, State) when is_integer(Ts) ->
    case
        mria_rlog:role() =:= core andalso
            is_hist_enabled() andalso
            (Ts < now_ts() - retain_duration())
    of
        true ->
            ok = mria:dirty_delete_object(?CHAN_REG_TAB, R),
            bump(deleted_hist, State);
        false ->
            State
    end;
%% Live registration row: classify the pid.
process_row(ClientId, #channel{pid = Pid}, State) when is_pid(Pid) ->
    {Verdict, State1} = classify_pid(Pid, State),
    case Verdict of
        keep ->
            State1;
        local_dead ->
            ok = emqx_cm_registry:unregister_channel({ClientId, Pid}),
            bump(deleted_local_dead, State1);
        remote_orphan ->
            ok = emqx_cm_registry:unregister_channel({ClientId, Pid}),
            bump(deleted_remote_orphan, State1)
    end.

classify_pid(Pid, State) when node(Pid) =:= node() ->
    case erlang:is_process_alive(Pid) of
        true -> {keep, State};
        false -> {local_dead, State}
    end;
classify_pid(Pid, State) ->
    %% Remote pid: only cores act on it. Replicants defer to the cores'
    %% authoritative consensus check.
    case mria_rlog:role() of
        replicant -> classify_remote_pid_on_replicant(Pid, State);
        _ -> classify_remote_pid_on_core(Pid, State)
    end.

classify_remote_pid_on_replicant(_Pid, State) ->
    {keep, State}.

%% Reuse the same cluster-wide consensus predicate that
%% emqx_cm_registry:can_run_cleanup/1 uses, cached per sweep so each
%% unique remote node-id triggers at most one mria:is_peer_alive/1 RPC
%% per sweep. Any non-{ok, false} answer (including {aborted, _} on RPC
%% error) is treated as "keep this sweep, retry next" to preserve the
%% safety bias.
classify_remote_pid_on_core(Pid, State) ->
    N = node(Pid),
    #{gone_nodes := Gone, alive_nodes := Alive} = State,
    %% Check alive first: a node that rejoined the cluster mid-sweep
    %% may carry a stale gone-mark from an earlier chunk; the alive
    %% cache is the authoritative vote.
    {Verdict, State1} =
        case {sets:is_element(N, Alive), sets:is_element(N, Gone)} of
            {true, _} ->
                {keep, State};
            {_, true} ->
                {remote_orphan, State};
            _ ->
                case mria:is_peer_alive(N) of
                    {ok, false} ->
                        {remote_orphan, State#{
                            gone_nodes := sets:add_element(N, Gone)
                        }};
                    _ ->
                        {keep, State#{
                            alive_nodes := sets:add_element(N, Alive)
                        }}
                end
        end,
    {Verdict, normalize_node_caches(State1)}.

%% Invariant: gone_nodes excludes alive_nodes. Re-established after each
%% remote-pid classification so a stale gone entry (e.g. for a node that
%% rejoined the cluster mid-sweep and got re-cached as alive) cannot
%% survive to cause a wrongful purge in a later chunk.
normalize_node_caches(#{gone_nodes := Gone, alive_nodes := Alive} = State) ->
    State#{gone_nodes := sets:subtract(Gone, Alive)}.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

is_hist_enabled() ->
    retain_duration() > 0.

%% Return the session registration history retain duration in seconds.
-spec retain_duration() -> non_neg_integer().
retain_duration() ->
    emqx:get_config([broker, session_history_retain]).

%% Single cadence between sweeps for both hist retention and stale-pid GC.
%% Hist tombstones may therefore live up to ?REGISTRY_GC_INTERVAL_MS past
%% their `session_history_retain' expiry; that grace is acceptable since
%% the field is informational (count/1 already filters by timestamp).
cleanup_delay() ->
    ?REGISTRY_GC_INTERVAL_MS.

send_delay_start() ->
    Delay = cleanup_delay(),
    send_delay_start(Delay).

send_delay_start(Delay) ->
    erlang:send_after(Delay, self(), start).

now_ts() ->
    erlang:system_time(seconds).

do_count(Since) ->
    Ms = ets:fun2ms(fun(#channel{pid = V}) ->
        is_pid(V) orelse (is_integer(V) andalso (V >= Since))
    end),
    ets:select_count(?CHAN_REG_TAB, Ms).
