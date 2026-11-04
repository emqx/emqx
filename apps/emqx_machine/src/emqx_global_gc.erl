%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_global_gc).

-moduledoc """
Periodic global garbage collection.

A sweep walks every process on the node and forces a garbage collection on
each one whose status is `waiting`. A process that is waiting is not running,
so it never reaches its own collection thresholds, however `fullsweep_after`
is tuned. The sweep reclaims what those processes hold.

Hibernating processes are collected along with the rest. They hold nothing to
reclaim, but telling them apart costs more than collecting them: the cheapest
reliable test is `current_function`, and that costs about twice what the
collection does.

`node.global_gc_interval` sets the period. A sweep runs in a one-off process
and walks in chunks, pausing between them. The pause comes from the interval
and the process count, so a sweep aims to finish within half the period. That
spreads the work over time rather than reducing it: the same collections, plus
the scheduling of taking them in pieces, instead of one burst. Only one sweep
runs at a time: starting one kills the sweep still in progress.

`run/1` starts a sweep now and replaces the period for the rest of this
node's life, which is the way to use this during support work.
`node.global_gc_interval` is `readOnly`, so changing the configured value
needs a restart.
""".

-behaviour(gen_server).

-export([start_link/0, stop/0]).

-export([run/0, run/1]).

-ifdef(TEST).
-export([chunk_pause/2]).
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

-include_lib("emqx/include/logger.hrl").

%% The shortest period `run/1' accepts, 10 minutes. A literal, because a guard
%% rejects a function call.
-define(MIN_INTERVAL, 600000).
-define(CHUNK_SIZE, 100).
%% A sweep aims to finish within this share of the interval, leaving the rest
%% of the period free of it.
-define(BUDGET_SHARE, 2).
%% A pause longer than this buys no more smoothing. It only leaves the sweep
%% half done for longer, over a process list that keeps changing.
-define(MAX_PAUSE, 1000).
-define(MIN_PAUSE, 1).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-doc """
Start a sweep now and leave the schedule alone.

Returns as soon as the sweep is started, not when it finishes. The sweep logs
its duration and how many processes it collected.
""".
-spec run() -> ok.
run() -> gen_server:call(?MODULE, run, infinity).

-doc """
Start a sweep now, then repeat every `Interval` milliseconds.

Cancels the pending timer, so `Interval` replaces whatever
`node.global_gc_interval` set until the node restarts. `Interval` must be at
least 10 minutes.

Returns as soon as the first sweep is started. Any sweep still in progress is
killed first, so two sweeps never run at once.
""".
-spec run(pos_integer()) -> ok | {error, {interval_too_short, pos_integer()}}.
run(Interval) when is_integer(Interval), Interval >= ?MIN_INTERVAL ->
    gen_server:call(?MODULE, {run, Interval}, infinity);
run(Interval) when is_integer(Interval) ->
    {error, {interval_too_short, ?MIN_INTERVAL}}.

-spec stop() -> ok.
stop() -> gen_server:stop(?MODULE).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    State = #{timer => undefined, interval => undefined, worker => undefined},
    {ok, ensure_timer(State)}.

handle_call(run, _From, State0) ->
    State = upgraded(State0),
    {reply, ok, start_sweep(State), hibernate};
handle_call({run, Interval}, _From, State0) ->
    State0Up = upgraded(State0),
    %% Set the interval first, so the sweep paces itself against the new one.
    State = start_sweep(cancel_timer(State0Up#{interval := Interval})),
    {reply, ok, ensure_timer(State), hibernate};
handle_call(_Req, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({timeout, TRef, run}, State0 = #{timer := TRef}) ->
    State = upgraded(State0),
    {noreply, ensure_timer(start_sweep(State#{timer := undefined})), hibernate};
handle_info({'DOWN', MRef, process, Pid, Reason}, State = #{worker := {Pid, MRef}}) ->
    ok = log_worker_exit(Reason),
    {noreply, State#{worker := undefined}, hibernate};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    _ = kill_worker(upgraded(State)),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%% Loading this module into a running node leaves the server holding the state
%% the previous version built, which has only `timer'. A code load does not
%% call `code_change/3', so each callback that reads the newer keys fills them
%% in first and the rest of the module can assume they are there.
upgraded(State) ->
    maps:merge(#{interval => undefined, worker => undefined}, State).

ensure_timer(State) ->
    start_timer(configured_interval(State), State).

start_timer(Interval, State) when is_integer(Interval) ->
    State#{timer := emqx_utils:start_timer(Interval, run)};
start_timer(_Disabled, State) ->
    State#{timer := undefined}.

%% `interval' holds what run/1 was given, and overrides the configuration until
%% the node restarts. node.global_gc_interval is readOnly, so the configured
%% value cannot change under a running node.
configured_interval(#{interval := undefined}) ->
    application:get_env(emqx_machine, global_gc_interval, disabled);
configured_interval(#{interval := Interval}) ->
    Interval.

%% What a sweep paces itself against. A manual run/0 on a node with the
%% periodic sweep disabled has no interval to work from, so it paces as if the
%% interval were the smallest one run/1 accepts.
pacing_interval(State) ->
    case configured_interval(State) of
        Interval when is_integer(Interval) -> Interval;
        _Disabled -> ?MIN_INTERVAL
    end.

cancel_timer(State = #{timer := TRef}) ->
    ok = emqx_utils:cancel_timer(TRef),
    State#{timer := undefined}.

start_sweep(State0) ->
    State = kill_worker(State0),
    Interval = pacing_interval(State),
    State#{worker := erlang:spawn_monitor(fun() -> sweep(Interval) end)}.

kill_worker(State = #{worker := undefined}) ->
    State;
kill_worker(State = #{worker := {Pid, MRef}}) ->
    %% Demonitor with flush first, so the kill leaves no 'DOWN' behind and the
    %% only 'DOWN' this server ever sees is a worker ending on its own.
    true = erlang:demonitor(MRef, [flush]),
    true = erlang:exit(Pid, kill),
    State#{worker := undefined}.

log_worker_exit(normal) ->
    ok;
log_worker_exit(Reason) ->
    ?SLOG(warning, #{msg => "global_gc_sweep_crashed", reason => Reason}).

sweep(Interval) ->
    Start = erlang:monotonic_time(millisecond),
    Pids = processes(),
    Scanned = length(Pids),
    Pause = chunk_pause(Interval, Scanned),
    Collected = sweep(Pids, Pause, 0),
    ?SLOG(info, #{
        msg => "global_gc_finished",
        elapsed_ms => erlang:monotonic_time(millisecond) - Start,
        scanned => Scanned,
        collected => Collected,
        chunk_pause_ms => Pause
    }).

%% Pause long enough that a sweep still finishes within its share of the
%% interval. Scaling with the process count smooths the spike across time: the
%% work per chunk is roughly constant, so a longer pause means a smaller share
%% of wall clock spent sweeping, paid for with the extra timer and scheduling
%% work of taking it in pieces.
chunk_pause(Interval, Scanned) ->
    %% One pause between each pair of chunks, so one fewer than the chunks.
    Pauses = max(1, ceil_div(Scanned, ?CHUNK_SIZE) - 1),
    Budget = Interval div ?BUDGET_SHARE,
    min(?MAX_PAUSE, max(?MIN_PAUSE, Budget div Pauses)).

ceil_div(Dividend, Divisor) ->
    (Dividend + Divisor - 1) div Divisor.

sweep([], _Pause, Collected) ->
    Collected;
sweep(Pids, Pause, Collected0) ->
    {Chunk, Rest} = take_chunk(?CHUNK_SIZE, Pids, []),
    Collected = Collected0 + gc_chunk(Chunk),
    case Rest of
        [] ->
            Collected;
        _ ->
            ok = timer:sleep(Pause),
            sweep(Rest, Pause, Collected)
    end.

%% The chunk comes back reversed. A sweep collects every process it is given,
%% so the order does not matter and reversing it back would be wasted work.
take_chunk(0, Rest, Acc) ->
    {Acc, Rest};
take_chunk(_N, [], Acc) ->
    {Acc, []};
take_chunk(N, [Pid | Rest], Acc) ->
    take_chunk(N - 1, Rest, [Pid | Acc]).

%% Request a collection for the whole chunk before waiting for any of it, so
%% the collections can overlap instead of running strictly one after another.
gc_chunk(Chunk) ->
    Ref = make_ref(),
    Requested = lists:foldl(fun(Pid, N) -> N + request_gc(Pid, Ref) end, 0, Chunk),
    await_gc(Ref, Requested, 0).

request_gc(Pid, Ref) ->
    case is_waiting(Pid) of
        true ->
            _ = erlang:garbage_collect(Pid, [{async, Ref}]),
            1;
        false ->
            0
    end.

%% Every request is answered, including one for a process that has exited in
%% the meantime, so this always ends.
await_gc(_Ref, 0, Collected) ->
    Collected;
await_gc(Ref, Pending, Collected) ->
    receive
        {garbage_collect, Ref, true} ->
            await_gc(Ref, Pending - 1, Collected + 1);
        {garbage_collect, Ref, false} ->
            await_gc(Ref, Pending - 1, Collected)
    end.

%% A process that runs reaches its own collection thresholds, so only the ones
%% that are not running are worth a request.
-compile({inline, [is_waiting/1]}).
is_waiting(Pid) ->
    {status, waiting} == process_info(Pid, status).
