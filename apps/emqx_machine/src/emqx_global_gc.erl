%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_global_gc).

-moduledoc """
Periodic global garbage collection.

A sweep walks every process on the node and forces a garbage collection on
each one whose status is `waiting`. That costs CPU in proportion to the
process count, so `node.global_gc_interval` is `disabled` by default and the
sweep is meant for a node whose memory keeps growing.

A sweep runs in a one-off process, in chunks, with a pause between chunks, so
it spreads its cost over time instead of taking the scheduler for the whole
walk. Only one sweep runs at a time: starting one kills the sweep still in
progress.

`run/1` starts a sweep and takes over the schedule without a node restart,
which is the way to use this during support work. `node.global_gc_interval`
is `readOnly`, so a configuration change needs a restart.
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

%% 10 minutes. A shorter interval leaves the node sweeping more than it serves
%% traffic. A literal, because a guard rejects a function call.
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

handle_call(run, _From, State) ->
    {reply, ok, start_sweep(State), hibernate};
handle_call({run, Interval}, _From, State0) ->
    %% Set the interval first, so the sweep paces itself against the new one.
    State = start_sweep(cancel_timer(State0#{interval := Interval})),
    {reply, ok, ensure_timer(State), hibernate};
handle_call(_Req, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({timeout, TRef, run}, State = #{timer := TRef}) ->
    {noreply, ensure_timer(start_sweep(State#{timer := undefined})), hibernate};
handle_info({'DOWN', MRef, process, Pid, Reason}, State = #{worker := {Pid, MRef}}) ->
    ok = log_worker_exit(Reason),
    {noreply, State#{worker := undefined}, hibernate};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    _ = kill_worker(State),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

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
%% interval. Scaling with the process count is what keeps the cost off a CPU
%% graph: the work per chunk is fixed, so a longer pause means a smaller share
%% of wall clock spent sweeping.
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
    Collected = Collected0 + lists:foldl(fun(Pid, Acc) -> Acc + do_gc(Pid) end, 0, Chunk),
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

do_gc(Pid) ->
    case is_waiting(Pid) andalso erlang:garbage_collect(Pid) of
        true -> 1;
        false -> 0
    end.

-compile({inline, [is_waiting/1]}).
is_waiting(Pid) ->
    {status, waiting} == process_info(Pid, status).
